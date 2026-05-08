//! Atomic KV counter operations.
//!
//! This module is intentionally independent of the central SQL/RQL parser so
//! issue #241 can wire transports onto the `KvAtomicOps` seam without forcing a
//! broad parser merge first.

use std::sync::Arc;

use crate::application::entity::CreateKvInput;
use crate::application::ports::RuntimeEntityPort;
use crate::application::ttl_payload::INTERNAL_TTL_MILLIS_KEY;
use crate::storage::schema::Value;
use crate::storage::unified::{EntityData, Metadata, MetadataValue};
use crate::{RedDBError, RedDBResult, RedDBRuntime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvAtomicOp {
    Incr,
    Decr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvAtomicCommand {
    pub op: KvAtomicOp,
    pub collection: String,
    pub key: String,
    pub by: i64,
    pub ttl_ms: Option<u64>,
}

impl KvAtomicCommand {
    pub const DEFAULT_COLLECTION: &'static str = "kv_default";

    pub fn parse(input: &str) -> RedDBResult<Self> {
        Self::parse_with_default_collection(input, Self::DEFAULT_COLLECTION)
    }

    pub fn parse_with_default_collection(input: &str, collection: &str) -> RedDBResult<Self> {
        let mut tokens = input.split_whitespace();
        let op = match tokens.next() {
            Some(word) if word.eq_ignore_ascii_case("INCR") => KvAtomicOp::Incr,
            Some(word) if word.eq_ignore_ascii_case("DECR") => KvAtomicOp::Decr,
            Some(word) => {
                return Err(RedDBError::Query(format!(
                    "expected INCR or DECR, got {word}"
                )))
            }
            None => return Err(RedDBError::Query("empty KV atomic command".to_string())),
        };

        let key = tokens
            .next()
            .ok_or_else(|| RedDBError::Query("INCR/DECR requires a key".to_string()))?
            .to_string();

        let mut magnitude = 1_i64;
        let mut ttl_ms = None;

        while let Some(clause) = tokens.next() {
            if clause.eq_ignore_ascii_case("BY") {
                let raw = tokens.next().ok_or_else(|| {
                    RedDBError::Query("BY requires an integer argument".to_string())
                })?;
                magnitude = raw
                    .parse::<i64>()
                    .map_err(|_| RedDBError::Query("BY requires an integer".to_string()))?;
            } else if clause.eq_ignore_ascii_case("EXPIRE") {
                let raw = tokens.next().ok_or_else(|| {
                    RedDBError::Query("EXPIRE requires a duration argument".to_string())
                })?;
                ttl_ms = Some(parse_duration_ms(raw)?);
            } else {
                return Err(RedDBError::Query(format!(
                    "unsupported INCR/DECR clause {clause}"
                )));
            }
        }

        let by = match op {
            KvAtomicOp::Incr => magnitude,
            KvAtomicOp::Decr => magnitude
                .checked_neg()
                .ok_or_else(|| RedDBError::Query("DECR BY value overflows i64".to_string()))?,
        };

        Ok(Self {
            op,
            collection: collection.to_string(),
            key,
            by,
            ttl_ms,
        })
    }
}

pub trait KvAtomicOps {
    fn incr(&self, collection: &str, key: &str, by: i64, ttl_ms: Option<u64>) -> RedDBResult<i64>;

    fn decr(&self, collection: &str, key: &str, by: i64, ttl_ms: Option<u64>) -> RedDBResult<i64>;

    fn apply_kv_atomic(&self, command: &KvAtomicCommand) -> RedDBResult<i64> {
        match command.op {
            KvAtomicOp::Incr => self.incr(
                &command.collection,
                &command.key,
                command.by,
                command.ttl_ms,
            ),
            KvAtomicOp::Decr => self.incr(
                &command.collection,
                &command.key,
                command.by,
                command.ttl_ms,
            ),
        }
    }
}

impl KvAtomicOps for RedDBRuntime {
    fn incr(&self, collection: &str, key: &str, by: i64, ttl_ms: Option<u64>) -> RedDBResult<i64> {
        self.check_write(crate::runtime::write_gate::WriteKind::Dml)?;
        let lock = self.kv_atomic_lock(collection, key);
        let _guard = lock.lock();

        match self.get_kv(collection, key)? {
            Some((value, id)) => {
                let current = kv_counter_i64(&value)?;
                let next = current
                    .checked_add(by)
                    .ok_or_else(|| RedDBError::Query("INCR/DECR counter overflow".to_string()))?;
                self.replace_kv_counter_value(collection, key, id, next, ttl_ms)?;
                Ok(next)
            }
            None => {
                self.create_kv(CreateKvInput {
                    collection: collection.to_string(),
                    key: key.to_string(),
                    value: Value::Integer(by),
                    metadata: ttl_metadata_fields(ttl_ms),
                })?;
                Ok(by)
            }
        }
    }

    fn decr(&self, collection: &str, key: &str, by: i64, ttl_ms: Option<u64>) -> RedDBResult<i64> {
        let delta = by
            .checked_neg()
            .ok_or_else(|| RedDBError::Query("DECR BY value overflows i64".to_string()))?;
        self.incr(collection, key, delta, ttl_ms)
    }
}

impl RedDBRuntime {
    fn kv_atomic_lock(&self, collection: &str, key: &str) -> Arc<parking_lot::Mutex<()>> {
        let map_key = (collection.to_string(), key.to_string());
        if let Some(lock) = self.inner.kv_atomic_locks.read().get(&map_key).cloned() {
            return lock;
        }

        let mut locks = self.inner.kv_atomic_locks.write();
        locks
            .entry(map_key)
            .or_insert_with(|| Arc::new(parking_lot::Mutex::new(())))
            .clone()
    }

    fn replace_kv_counter_value(
        &self,
        collection: &str,
        key: &str,
        id: crate::storage::EntityId,
        next: i64,
        ttl_ms: Option<u64>,
    ) -> RedDBResult<()> {
        let db = self.db();
        let store = db.store();
        let manager = store
            .get_collection(collection)
            .ok_or_else(|| RedDBError::NotFound(format!("collection not found: {collection}")))?;
        let mut entity = manager.get(id).ok_or_else(|| {
            RedDBError::NotFound(format!("KV key disappeared during atomic update: {key}"))
        })?;

        let EntityData::Row(row) = &mut entity.data else {
            return Err(RedDBError::Query(format!(
                "KV key {key} is not backed by a table row"
            )));
        };

        let named = row
            .named
            .as_mut()
            .ok_or_else(|| RedDBError::Query(format!("KV key {key} has no named fields")))?;
        named.insert("value".to_string(), Value::Integer(next));

        entity.updated_at = current_unix_secs();
        manager
            .update(entity.clone())
            .map_err(|err| RedDBError::Internal(err.to_string()))?;

        if let Some(ttl_ms) = ttl_ms {
            let mut metadata = store.get_metadata(collection, id).unwrap_or_default();
            metadata.set(INTERNAL_TTL_MILLIS_KEY, ttl_metadata_value(ttl_ms));
            manager
                .set_metadata(id, metadata)
                .map_err(|err| RedDBError::Internal(err.to_string()))?;
        }

        store
            .persist_entities_to_pager(collection, std::slice::from_ref(&entity))
            .map_err(|err| RedDBError::Internal(err.to_string()))?;
        self.invalidate_result_cache_for_table(collection);
        self.cdc_emit(
            crate::replication::cdc::ChangeOperation::Update,
            collection,
            id.raw(),
            "kv",
        );
        Ok(())
    }
}

fn kv_counter_i64(value: &Value) -> RedDBResult<i64> {
    match value {
        Value::Integer(v) => Ok(*v),
        Value::UnsignedInteger(v) if *v <= i64::MAX as u64 => Ok(*v as i64),
        _ => Err(RedDBError::Query(
            "INCR/DECR requires the existing KV value to be an integer".to_string(),
        )),
    }
}

fn ttl_metadata_fields(ttl_ms: Option<u64>) -> Vec<(String, MetadataValue)> {
    ttl_ms
        .map(|ttl| vec![(INTERNAL_TTL_MILLIS_KEY.to_string(), ttl_metadata_value(ttl))])
        .unwrap_or_default()
}

fn ttl_metadata_value(ttl_ms: u64) -> MetadataValue {
    if ttl_ms <= i64::MAX as u64 {
        MetadataValue::Int(ttl_ms as i64)
    } else {
        MetadataValue::Timestamp(ttl_ms)
    }
}

fn parse_duration_ms(raw: &str) -> RedDBResult<u64> {
    let split_at = raw
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(raw.len());
    let (number, unit) = raw.split_at(split_at);
    if number.is_empty() {
        return Err(RedDBError::Query("duration requires a number".to_string()));
    }
    let value = number
        .parse::<u64>()
        .map_err(|_| RedDBError::Query("duration number is out of range".to_string()))?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "" | "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(RedDBError::Query(format!(
                "unsupported duration unit {unit}"
            )))
        }
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| RedDBError::Query("duration overflows milliseconds".to_string()))
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn parse_incr_decr_commands() {
        let incr = KvAtomicCommand::parse("INCR visits BY 5 EXPIRE 10s").unwrap();
        assert_eq!(incr.op, KvAtomicOp::Incr);
        assert_eq!(incr.collection, "kv_default");
        assert_eq!(incr.key, "visits");
        assert_eq!(incr.by, 5);
        assert_eq!(incr.ttl_ms, Some(10_000));

        let decr = KvAtomicCommand::parse("DECR visits BY 2 EXPIRE 250ms").unwrap();
        assert_eq!(decr.op, KvAtomicOp::Decr);
        assert_eq!(decr.by, -2);
        assert_eq!(decr.ttl_ms, Some(250));
    }

    #[test]
    fn missing_key_initializes_to_by() {
        let rt = RedDBRuntime::in_memory().unwrap();
        assert_eq!(rt.incr("kv_default", "counter", 7, None).unwrap(), 7);
        assert_eq!(
            rt.get_kv("kv_default", "counter").unwrap().unwrap().0,
            Value::Integer(7)
        );
    }

    #[test]
    fn non_integer_value_errors_without_mutating() {
        let rt = RedDBRuntime::in_memory().unwrap();
        rt.create_kv(CreateKvInput {
            collection: "kv_default".to_string(),
            key: "counter".to_string(),
            value: Value::text("not-an-int"),
            metadata: Vec::new(),
        })
        .unwrap();

        let err = rt.incr("kv_default", "counter", 1, None).unwrap_err();
        assert!(err.to_string().contains("existing KV value"));
        assert_eq!(
            rt.get_kv("kv_default", "counter").unwrap().unwrap().0,
            Value::text("not-an-int")
        );
    }

    #[test]
    fn ttl_refreshes_on_existing_counter() {
        let rt = RedDBRuntime::in_memory().unwrap();
        assert_eq!(rt.incr("kv_default", "counter", 1, Some(100)).unwrap(), 1);
        assert_eq!(rt.incr("kv_default", "counter", 1, Some(250)).unwrap(), 2);

        let (_, id) = rt.get_kv("kv_default", "counter").unwrap().unwrap();
        let metadata = rt.db().store().get_metadata("kv_default", id).unwrap();
        assert_eq!(
            metadata.get(INTERNAL_TTL_MILLIS_KEY),
            Some(&MetadataValue::Int(250))
        );
    }

    #[test]
    fn concurrent_incr_converges() {
        let rt = Arc::new(RedDBRuntime::in_memory().unwrap());
        let mut handles = Vec::new();

        for _ in 0..100 {
            let rt = Arc::clone(&rt);
            handles.push(std::thread::spawn(move || {
                rt.incr("kv_default", "counter", 1, None).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            rt.get_kv("kv_default", "counter").unwrap().unwrap().0,
            Value::Integer(100)
        );
    }
}
