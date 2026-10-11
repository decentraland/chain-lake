import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs'
import { AddressInfo } from 'net'
import { tmpdir } from 'os'
import { join } from 'path'
import { DuckDBInstance } from '@duckdb/node-api'
import { DataSourceBuilder } from '@subsquid/evm-stream'
import { MANIFEST_FILE } from '../manifest'
import { LakeDataset } from '../serve/lake'
import { createPortal } from '../serve/server'
import { blocksOf, compareStreams, StreamedBlock } from './compare'

const DATASET = 'ethereum-sepolia'
const ADDRESS = '0x' + 'aa'.repeat(20)
const SENDER = '0x' + 'bb'.repeat(20)
const TOPIC = '0x' + '01'.repeat(32)
const hash = (n: number) => '0x' + n.toString(16).padStart(64, '0')

/** A lake of one chunk, blocks 1 to 100: two logs in block 50, one in block 80, each with its transaction. */
async function writeLake(lakeDir: string): Promise<void> {
  const chunk = '0000000001-0000000100-0a0b0c0d'
  const dir = join(lakeDir, DATASET, 'chunks', chunk)
  mkdirSync(dir, { recursive: true })
  const db = await (await DuckDBInstance.create(':memory:')).connect()
  await db.run(
    `COPY (SELECT * FROM (VALUES (50::BIGINT, 0::INTEGER, 0::INTEGER, '${hash(5000)}'), (50, 1, 0, '${hash(5000)}'), (80, 0, 1, '${hash(8001)}'))
       AS t(block_number, log_index, transaction_index, transaction_hash))
     TO '${dir}/logs.tmp.parquet' (FORMAT parquet)`
  )
  await db.run(
    `COPY (SELECT *, '${ADDRESS}' AS address, '${TOPIC}' AS topic0, NULL::VARCHAR AS topic1, NULL::VARCHAR AS topic2, NULL::VARCHAR AS topic3, '0x' AS data
       FROM '${dir}/logs.tmp.parquet') TO '${dir}/logs.parquet' (FORMAT parquet)`
  )
  rmSync(join(dir, 'logs.tmp.parquet'))
  await db.run(
    `COPY (SELECT n::BIGINT AS number, h AS hash, p AS parent_hash, (1700000000 + n)::BIGINT AS timestamp
       FROM (VALUES (50, '${hash(50)}', '${hash(49)}'), (80, '${hash(80)}', '${hash(79)}'), (100, '${hash(100)}', '${hash(99)}')) AS t(n, h, p))
     TO '${dir}/blocks.parquet' (FORMAT parquet)`
  )
  await db.run(
    `COPY (SELECT * FROM (VALUES (50::BIGINT, 0::INTEGER, '${hash(5000)}', '${SENDER}', '${ADDRESS}', '0x'), (80, 1, '${hash(8001)}', '${SENDER}', NULL, '0x01'))
       AS t(block_number, transaction_index, hash, "from", "to", input))
     TO '${dir}/transactions.parquet' (FORMAT parquet)`
  )
  writeFileSync(join(lakeDir, DATASET, MANIFEST_FILE), JSON.stringify({ height: 100, hash: hash(100), chunks: [{ dir: chunk, from: 1, to: 100 }] }))
}

test("the squids' portal client reads what the lake portal serves, and two reads of it compare equal", async (t) => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  t.after(() => rmSync(lakeDir, { recursive: true, force: true }))
  await writeLake(lakeDir)
  const view = await LakeDataset.open(lakeDir, DATASET)
  const server = createPortal((name) => (name === DATASET ? Promise.resolve(view) : undefined)).listen(0, '127.0.0.1')
  await new Promise((resolve) => server.once('listening', resolve))
  t.after(() => server.close())
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/datasets/${DATASET}`

  const read = () =>
    blocksOf(
      new DataSourceBuilder()
        .setPortal({ url, http: { retryAttempts: 1 } })
        .setBlockRange({ from: 1, to: 100 })
        .setFields({
          block: { timestamp: true },
          log: { address: true, topics: true, data: true, transactionHash: true },
          transaction: { hash: true, from: true, to: true, input: true },
        })
        .addLog({ where: { address: [ADDRESS] }, include: { transaction: true } })
        .build()
        .getFinalizedStream({ from: 1, to: 100 })
    ) as AsyncIterable<StreamedBlock>

  const tally = await compareStreams(read(), read())
  assert.equal(tally.differences, 0)
  assert.deepEqual(tally.left, { blocks: 2, logs: 3, transactions: 2 })
  assert.equal(tally.lastBlock, 80)
})
