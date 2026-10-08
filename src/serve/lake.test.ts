import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { DuckDBInstance } from '@duckdb/node-api'
import { LakeDataset, MAX_LOGS } from './lake'
import { parseQuery } from './query'

const ADDRESS = '0x' + 'aa'.repeat(20)
const TOPIC = '0x' + '01'.repeat(32)

/** Writes a chunk holding `count` logs in `logBlock`, and the header of its last block. */
async function writeChunk(chunks: string, from: number, to: number, logBlock: number, count = 1) {
  const dir = join(chunks, `${String(from).padStart(10, '0')}-${String(to).padStart(10, '0')}`)
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
}

test('a chunk the writer reports is served at once, not after the status is read again', async () => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  try {
    const chunks = join(lakeDir, 'polygon-mainnet', 'chunks')
    await writeChunk(chunks, 1, 100, 50)
    writeFileSync(join(chunks, 'status.txt'), '100\n0x100')
    const lake = await LakeDataset.open(lakeDir, 'polygon-mainnet')
    const query = parseQuery({ type: 'evm', fromBlock: 1, fields: { log: { address: true } }, logs: [{ address: [ADDRESS] }] })
    assert.deepEqual((await lake.page(query, 1, 100)).logs.map((l) => l.block_number), [50])

    // The follower writes the next chunk and lets go of its blocks; the status is cached meanwhile.
    await writeChunk(chunks, 101, 200, 150)
    writeFileSync(join(chunks, 'status.txt'), '200\n0x200')
    lake.setWritten({ height: 200, hash: '0x200' })

    const page = await lake.page(query, 101, 200)
    assert.deepEqual(page.logs.map((l) => l.block_number), [150])
    assert.equal(page.upper.number, 200)
  } finally {
    rmSync(lakeDir, { recursive: true, force: true })
  }
})

test('a status read that started before the writer reported a chunk does not take it back', async () => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  try {
    const chunks = join(lakeDir, 'polygon-mainnet', 'chunks')
    await writeChunk(chunks, 1, 100, 50)
    writeFileSync(join(chunks, 'status.txt'), '100\n0x100')
    const lake = await LakeDataset.open(lakeDir, 'polygon-mainnet')
    await lake.written()

    // The cached status expires, and a new read of it is still on its way when the writer reports.
    const internals = lake as unknown as { dest: { readFile(name: string): Promise<string> }; status: { readAt: number } }
    const readFile = internals.dest.readFile
    let release!: (status: string) => void
    internals.dest.readFile = () => new Promise<string>((resolve) => (release = resolve))
    internals.status.readAt = 0
    const inFlight = lake.written()

    await writeChunk(chunks, 101, 200, 150)
    writeFileSync(join(chunks, 'status.txt'), '200\n0x200')
    lake.setWritten({ height: 200, hash: '0x200' })
    release('100\n0x100')
    assert.equal((await inFlight).height, 200)
    internals.dest.readFile = readFile

    const query = parseQuery({ type: 'evm', fromBlock: 101, fields: { log: { address: true } }, logs: [{ address: [ADDRESS] }] })
    assert.deepEqual((await lake.page(query, 101, 200)).logs.map((l) => l.block_number), [150])
  } finally {
    rmSync(lakeDir, { recursive: true, force: true })
  }
})

test('a block holding more logs than a page is served whole', async () => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  try {
    const chunks = join(lakeDir, 'polygon-mainnet', 'chunks')
    await writeChunk(chunks, 1, 200, 150, MAX_LOGS + 1)
    writeFileSync(join(chunks, 'status.txt'), '200\n0x200')
    const lake = await LakeDataset.open(lakeDir, 'polygon-mainnet')
    const query = parseQuery({ type: 'evm', fromBlock: 1, fields: { log: { address: true } }, logs: [{ address: [ADDRESS] }] })
    const page = await lake.page(query, 1, 200)
    assert.equal(page.logs.length, MAX_LOGS + 1)
    assert.equal(page.upper.number, 150)
  } finally {
    rmSync(lakeDir, { recursive: true, force: true })
  }
})
