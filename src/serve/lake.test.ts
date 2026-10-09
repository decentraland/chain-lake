import { test, TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { DuckDBInstance } from '@duckdb/node-api'
import { Chunk, Manifest, MANIFEST_FILE } from '../manifest'
import { LakeDataset, MAX_LOGS } from './lake'
import { parseQuery } from './query'

const ADDRESS = '0x' + 'aa'.repeat(20)
const TOPIC = '0x' + '01'.repeat(32)
const DATASET = 'polygon-mainnet'

/** A dataset in a fresh directory, removed once the test is done. */
function dataset(t: TestContext): string {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  t.after(() => rmSync(lakeDir, { recursive: true, force: true }))
  return lakeDir
}

/** Writes a chunk holding `count` logs in `logBlock`, and the header of its last block. */
async function writeChunk(lakeDir: string, from: number, to: number, logBlock: number, count = 1, writer = '0a0b0c0d'): Promise<Chunk> {
  const name = `${String(from).padStart(10, '0')}-${String(to).padStart(10, '0')}-${writer}`
  const dir = join(lakeDir, DATASET, 'chunks', name)
  mkdirSync(dir, { recursive: true })
  const db = await (await DuckDBInstance.create(':memory:')).connect()
  await db.run(
    `COPY (SELECT ${logBlock}::BIGINT AS block_number, i::INTEGER AS log_index, 0::INTEGER AS transaction_index, '0xtx' AS transaction_hash,
       '${ADDRESS}' AS address, '${TOPIC}' AS topic0, NULL::VARCHAR AS topic1, NULL::VARCHAR AS topic2, NULL::VARCHAR AS topic3, '0x' AS data
       FROM range(${count}) t(i))
     TO '${dir}/logs.parquet' (FORMAT parquet)`
  )
  await db.run(
    `COPY (SELECT * FROM (VALUES (${logBlock}::BIGINT, '0x${logBlock}', '0x${logBlock - 1}', ${logBlock}::BIGINT), (${to}::BIGINT, '0x${to}', '0x${to - 1}', ${to}::BIGINT))
       AS t(number, hash, parent_hash, timestamp)) TO '${dir}/blocks.parquet' (FORMAT parquet)`
  )
  await db.run(
    `COPY (SELECT ${logBlock}::BIGINT AS block_number, 0::INTEGER AS transaction_index, '0xtx' AS hash, '0xf' AS "from", NULL::VARCHAR AS "to", '0x' AS input)
     TO '${dir}/transactions.parquet' (FORMAT parquet)`
  )
  return { dir: name, from, to }
}

function writeManifest(lakeDir: string, manifest: Manifest) {
  writeFileSync(join(lakeDir, DATASET, MANIFEST_FILE), JSON.stringify(manifest))
}

const query = parseQuery({ type: 'evm', fromBlock: 1, fields: { log: { address: true } }, logs: [{ address: [ADDRESS] }] })

test('a chunk the writer reports is served at once, not after the manifest is read again', async (t) => {
  const lakeDir = dataset(t)
  const first = await writeChunk(lakeDir, 1, 100, 50)
  writeManifest(lakeDir, { height: 100, hash: '0x100', chunks: [first] })
  const lake = await LakeDataset.open(lakeDir, DATASET)
  assert.deepEqual((await lake.page(query, 1, 100)).logs.map((l) => l.block_number), [50])

  // The follower commits the next chunk and lets go of its blocks; the manifest is cached meanwhile.
  const second = await writeChunk(lakeDir, 101, 200, 150)
  const committed = { height: 200, hash: '0x200', chunks: [first, second] }
  writeManifest(lakeDir, committed)
  lake.setWritten(committed)

  const page = await lake.page(query, 101, 200)
  assert.deepEqual(page.logs.map((l) => l.block_number), [150])
  assert.equal(page.upper.number, 200)
})

test('a manifest read that started before the writer reported a chunk does not take it back', async (t) => {
  const lakeDir = dataset(t)
  const first = await writeChunk(lakeDir, 1, 100, 50)
  const before = { height: 100, hash: '0x100', chunks: [first] }
  writeManifest(lakeDir, before)
  const lake = await LakeDataset.open(lakeDir, DATASET)
  await lake.written()

  // The cached manifest expires, and a new read of it is still on its way when the writer reports.
  const internals = lake as unknown as { store: { read(name: string): Promise<unknown> }; manifest: { readAt: number } }
  const read = internals.store.read
  let release!: (value: unknown) => void
  internals.store.read = () => new Promise((resolve) => (release = resolve))
  internals.manifest.readAt = 0
  const inFlight = lake.written()

  const second = await writeChunk(lakeDir, 101, 200, 150)
  lake.setWritten({ height: 200, hash: '0x200', chunks: [first, second] })
  release({ value: before, version: 'old' })
  assert.equal((await inFlight).height, 200)
  internals.store.read = read

  assert.deepEqual((await lake.page(query, 101, 200)).logs.map((l) => l.block_number), [150])
})

test('a chunk the manifest does not list is never read', async (t) => {
  const lakeDir = dataset(t)
  const listed = await writeChunk(lakeDir, 1, 100, 50)
  // Left behind by a writer that failed, or that lost the manifest to another.
  await writeChunk(lakeDir, 1, 100, 60, 1, 'ffffffff')
  writeManifest(lakeDir, { height: 100, hash: '0x100', chunks: [listed] })
  const lake = await LakeDataset.open(lakeDir, DATASET)
  assert.deepEqual((await lake.page(query, 1, 100)).logs.map((l) => l.block_number), [50])
})

test('a block holding more logs than a page is served whole', async (t) => {
  const lakeDir = dataset(t)
  const chunk = await writeChunk(lakeDir, 1, 200, 150, MAX_LOGS + 1)
  writeManifest(lakeDir, { height: 200, hash: '0x200', chunks: [chunk] })
  const lake = await LakeDataset.open(lakeDir, DATASET)
  const page = await lake.page(query, 1, 200)
  assert.equal(page.logs.length, MAX_LOGS + 1)
  assert.equal(page.upper.number, 150)
})
