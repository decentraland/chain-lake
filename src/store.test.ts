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
