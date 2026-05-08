/**
 * KV client — exposes `kv.{put,get,delete,incr,decr,invalidateTags}` through the underlying transport.
 *
 * HTTP uses the REST KV endpoint. RedWire transports may bridge the same
 * method names directly or fall back to SQL while dedicated wire frames are
 * still landing server-side.
 */

export class KvClient {
  /** @param {{ call: Function }} client */
  constructor(client) {
    this._client = client
  }

  /**
   * Store or replace a key in a collection.
   * @param {string} collection
   * @param {string | number} key
   * @param {unknown} value
   * @param {{ tags?: string[], ttlMs?: number, ifNotExists?: boolean }} [opts]
   * @returns {Promise<object>}
   */
  async put(collection, key, value, opts = {}) {
    return await this._client.call('kv.put', { collection, key: String(key), value, ...opts })
  }

  /**
   * Fetch a key from a collection.
   * @param {string} collection
   * @param {string | number} key
   * @returns {Promise<object>}
   */
  async get(collection, key) {
    return await this._client.call('kv.get', { collection, key: String(key) })
  }

  /**
   * Delete a key from a collection.
   * @param {string} collection
   * @param {string | number} key
   * @returns {Promise<object>}
   */
  async delete(collection, key) {
    return await this._client.call('kv.delete', { collection, key: String(key) })
  }

  /**
   * Atomically increment an integer key and return the new value.
   * @param {string} collection
   * @param {string | number} key
   * @param {number} [by]
   * @param {number | undefined} [ttlMs]
   * @returns {Promise<object>}
   */
  async incr(collection, key, by = 1, ttlMs = undefined) {
    return await this._client.call('kv.incr', { collection, key: String(key), by, ttlMs })
  }

  /**
   * Atomically decrement an integer key and return the new value.
   * @param {string} collection
   * @param {string | number} key
   * @param {number} [by]
   * @param {number | undefined} [ttlMs]
   * @returns {Promise<object>}
   */
  async decr(collection, key, by = 1, ttlMs = undefined) {
    return await this._client.call('kv.decr', { collection, key: String(key), by, ttlMs })
  }

  /**
   * Delete every KV entry in a collection tagged with any of the supplied tags.
   * @param {string} collection
   * @param {string[]} tags
   * @returns {Promise<object>}
   */
  async invalidateTags(collection, tags) {
    return await this._client.call('kv.invalidate_tags', { collection, tags })
  }
}
