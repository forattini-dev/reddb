/**
 * Mock-server tests for the KV API in @reddb-io/client.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { once } from 'node:events'

import { HttpRpcClient } from '../src/http.js'
import { KvClient } from '../src/kv.js'

async function startMockServer(handlers) {
  const server = createServer(async (req, res) => {
    let body = ''
    req.on('data', (chunk) => { body += chunk })
    req.on('end', async () => {
      const key = `${req.method} ${req.url}`
      const handler = handlers[key] ?? handlers['*']
      if (!handler) {
        res.statusCode = 404
        res.setHeader('content-type', 'application/json')
        res.end(JSON.stringify({ ok: false, error: `no handler for ${key}` }))
        return
      }
      try {
        const parsed = body ? JSON.parse(body) : {}
        const out = await handler(parsed, req)
        res.statusCode = out?.status ?? 200
        res.setHeader('content-type', 'application/json')
        res.end(JSON.stringify(out?.body ?? out))
      } catch (err) {
        res.statusCode = 500
        res.end(JSON.stringify({ ok: false, error: String(err.message || err) }))
      }
    })
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const { port } = server.address()
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  }
}

function createKv(baseUrl) {
  return new KvClient(new HttpRpcClient({ baseUrl }))
}

test('kv.put/get/delete use canonical /kv endpoint', async () => {
  const seen = []
  const stub = await startMockServer({
    'GET /health': () => ({ ok: true, version: 'mock' }),
    'PUT /collections/app/kv/theme': (body) => {
      seen.push(body)
      return { ok: true, id: 7, key: 'theme' }
    },
    'GET /collections/app/kv/theme': () => ({
      ok: true,
      collection: 'app',
      key: 'theme',
      value: 'dark',
      id: 7,
    }),
    'DELETE /collections/app/kv/theme': () => ({ ok: true, deleted: true, key: 'theme' }),
  })
  try {
    const kv = createKv(stub.baseUrl)
    assert.deepEqual(await kv.put('app', 'theme', 'dark'), { ok: true, id: 7, key: 'theme' })
    assert.deepEqual(seen[0], { value: 'dark' })
    assert.equal((await kv.get('app', 'theme')).value, 'dark')
    assert.deepEqual(await kv.delete('app', 'theme'), { ok: true, deleted: true, key: 'theme' })
  } finally {
    await stub.close()
  }
})

test('kv HTTP route falls back to legacy /kvs endpoint on 404', async () => {
  const requests = []
  const stub = await startMockServer({
    'GET /health': () => ({ ok: true, version: 'mock' }),
    'PUT /collections/app/kvs/theme': () => ({ ok: true, id: 9, key: 'theme' }),
    '*': (_body, req) => {
      requests.push(`${req.method} ${req.url}`)
      return { status: 404, body: { ok: false, error: 'not found' } }
    },
  })
  try {
    const kv = createKv(stub.baseUrl)
    const out = await kv.put('app', 'theme', 'dark')
    assert.equal(out.id, 9)
    assert.deepEqual(requests, ['PUT /collections/app/kv/theme'])
  } finally {
    await stub.close()
  }
})

test('kv.put with tags routes through query SQL', async () => {
  const queries = []
  const stub = await startMockServer({
    'GET /health': () => ({ ok: true, version: 'mock' }),
    'POST /query': (body) => {
      queries.push(body.query)
      return { ok: true, result: { affected: 1 } }
    },
  })
  try {
    const kv = createKv(stub.baseUrl)
    const out = await kv.put('sessions', 'user:1', { role: 'admin' }, {
      tags: ['tenant:1', "vip'user"],
      ttlMs: 5000,
      ifNotExists: true,
    })
    assert.deepEqual(out, { affected: 1 })
    assert.deepEqual(queries, [
      "PUT 'sessions'.'user:1' = '{\"role\":\"admin\"}' EXPIRE 5000 ms TAGS ['tenant:1', 'vip''user'] IF NOT EXISTS",
    ])
  } finally {
    await stub.close()
  }
})

test('kv.invalidateTags routes through query SQL', async () => {
  const queries = []
  const stub = await startMockServer({
    'GET /health': () => ({ ok: true, version: 'mock' }),
    'POST /query': (body) => {
      queries.push(body.query)
      return { ok: true, result: { affected: 2 } }
    },
  })
  try {
    const kv = createKv(stub.baseUrl)
    const out = await kv.invalidateTags('sessions', ['active', 'tenant:1'])
    assert.deepEqual(out, { affected: 2 })
    assert.deepEqual(queries, [
      "INVALIDATE TAGS ['active', 'tenant:1'] FROM sessions",
    ])
  } finally {
    await stub.close()
  }
})
