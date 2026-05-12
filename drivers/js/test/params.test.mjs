import { test } from 'node:test'
import assert from 'node:assert/strict'

import { RedDB } from '../src/index.js'

class CaptureClient {
  constructor() {
    this.calls = []
  }

  call(method, params) {
    this.calls.push({ method, params })
    return Promise.resolve({ rows: [] })
  }

  close() {
    return Promise.resolve()
  }
}

test('query serializes native JS params for embedded JSON-RPC', async () => {
  const client = new CaptureClient()
  const db = new RedDB(client)
  const when = new Date('2023-11-14T22:13:20.000Z')

  await db.query(
    'INSERT INTO value_params VALUES ($1, $2, $3, $4, $5, $6, $7)',
    [
      null,
      true,
      1.5,
      new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
      when,
      '00112233-4455-6677-8899-aabbccddeeff',
      { b: 2, a: 1 },
    ],
  )

  assert.deepEqual(client.calls, [{
    method: 'query',
    params: {
      sql: 'INSERT INTO value_params VALUES ($1, $2, $3, $4, $5, $6, $7)',
      params: [
        null,
        true,
        1.5,
        { $bytes: '3q2+7w==' },
        { $ts: 1700000000 },
        { $uuid: '00112233-4455-6677-8899-aabbccddeeff' },
        { b: 2, a: 1 },
      ],
    },
  }])
})
