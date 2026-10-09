import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test } from 'node:test'
import { Conflict, openStore } from './store'

test('a write succeeds only from the version last read or written', async () => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  try {
    const store = openStore(lakeDir, 'polygon-mainnet')
    assert.equal(await store.read('registry.json'), undefined)

    const v1 = await store.write('registry.json', { n: 1 }, null)
    await assert.rejects(store.write('registry.json', { n: 0 }, null), Conflict, 'it exists already')

    const v2 = await store.write('registry.json', { n: 2 }, v1)
    await assert.rejects(store.write('registry.json', { n: 3 }, v1), Conflict, 'another process wrote it since v1')
    assert.deepEqual(await store.read('registry.json'), { value: { n: 2 }, version: v2 })
  } finally {
    rmSync(lakeDir, { recursive: true, force: true })
  }
})

test('on S3, a write whose answer was lost is recognised by its content, not taken for a conflict', async () => {
  const store = openStore('s3://bucket/lake', 'polygon-mainnet')
  const written = JSON.stringify({ n: 1 }, null, 2) + '\n'
  ;(store as unknown as { client: { send(command: { constructor: { name: string } }): Promise<unknown> } }).client = {
    async send(command) {
      if (command.constructor.name === 'PutObjectCommand') throw Object.assign(new Error('precondition failed'), { $metadata: { httpStatusCode: 412 } })
      return { ETag: '"v2"', Body: { transformToString: async () => written } }
    },
  }
  assert.equal(await store.write('registry.json', { n: 1 }, '"v1"'), '"v2"', 'the file holds what this process wrote')
  await assert.rejects(store.write('registry.json', { n: 2 }, '"v1"'), Conflict, 'it holds something else')
})
