# Additional Value variants (bool, float, bytes, json, timestamp, uuid) end-to-end [AFK]

GitHub: https://github.com/reddb-io/reddb/issues/356

Labels: needs-triage

GitHub issue number: #356

## AFK instruction

Implement this issue as a focused vertical slice. Preserve behavior with tests/checks, commit all changes, and move this file to `issues/done/` when complete. If blocked, add a progress note and move it to `issues/blocked/`.

## Parent

#351

## What to build

Round out the `Value` enum with the remaining variants and prove them end-to-end via embedded stdio + JS SDK:

- `Value::Bool`
- `Value::Float` (f64) — including NaN, ±inf, subnormals
- `Value::Bytes` (binary blob)
- `Value::Json` (canonical JSON object/array)
- `Value::Timestamp` (epoch nanoseconds)
- `Value::Uuid`

Each variant gets parser context support in the binder (which clauses accept which types), JSON-RPC encoding (with `{"$bytes": ...}`, `{"$ts": ...}`, `{"$uuid": ...}` envelope per ADR), and JS SDK type mapping (`Uint8Array`, `Date`, native `null`, `boolean`, `number`).

## Acceptance criteria

- [x] All variants round-trip through embedded stdio JSON-RPC.
- [x] JS SDK maps native types correctly: `null`, `boolean`, `number` (int vs float distinction documented), `Uint8Array`, `Date`, plain object → json, UUID strings.
- [x] Boundary values tested: i64::MIN/MAX, f64 NaN/±inf, empty/very long bytes, deeply nested json.
- [x] Property-based round-trip tests for the wire Value codec (deep module).
- [x] Binder rejects type mismatches with typed errors per variant.

## Done

- Added JSON-RPC typed parameter/result envelopes for bytes, timestamps, UUIDs,
  JSON, and non-finite floats.
- Routed bound `INSERT` shapes through the existing insert executor so
  parameterized typed inserts can complete end-to-end.
- Extended JS SDK embedded parameter serialization for `Uint8Array`, `Date`,
  UUID strings, plain JSON objects, and non-finite numbers.
- Added stdio JSON-RPC, JS SDK, RedWire codec, and value-codec property tests.

## Blocked by

- #353
