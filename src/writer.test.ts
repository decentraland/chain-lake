import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test } from 'node:test'
import { Manifest, MANIFEST_FILE } from './manifest'
import { LakeDataset } from './serve/lake'
import { parseQuery } from './serve/query'
import { Conflict, openStore } from './store'
import { LakeBlock, writeBlocks } from './write'
import { openWriter } from './writer'

const ADDRESS = '0x' + 'aa'.repeat(20)
const DATASET = 'polygon-mainnet'

function block(number: number): LakeBlock {
  return {
    header: { number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number * 1000 },
    logs: [{ logIndex: 0, transactionIndex: 0, transactionHash: `0xtx${number}`, address: ADDRESS, topics: ['0x' + '01'.repeat(32)], data: '0x' }],
    transactions: [{ transactionIndex: 0, hash: `0xtx${number}`, from: '0xf', to: ADDRESS, input: '0x' }],
  }
}

const head = (n: number) => ({ height: n, hash: n < 0 ? '0x' : `0x${n}` })
const tx = (from: number, to: number) => ({ prevHead: head(from), nextHead: head(to), isOnTop: true }) as never

test('of two processes writing one dataset, one extends the lake and the other publishes nothing', async () => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  try {
    const a = openWriter(lakeDir, DATASET, openStore(lakeDir, DATASET), { chunkSizeMb: 64 })
    const b = openWriter(lakeDir, DATASET, openStore(lakeDir, DATASET), { chunkSizeMb: 64 })
    await a.connect()
    await b.connect()

    // A gets past its checks and is still inside its batch when B commits: A's commit must fail.
    await assert.rejects(
      a.transact(tx(-1, 2), async (store) => {
        writeBlocks(store as never, [block(1), block(2)])
        store.setForceFlush(true)
        await b.transact(tx(-1, 1), async (other) => {
          writeBlocks(other as never, [block(1)])
          other.setForceFlush(true)
        })
      }),
      Conflict
    )

    const manifest = (await openStore(lakeDir, DATASET).read<Manifest>(MANIFEST_FILE))!.value
    assert.equal(manifest.height, 1, 'the lake is B’s, and was not taken back')
    assert.equal(manifest.chunks.length, 1)

    // A's chunk is on disk but not in the manifest, so the portal never reads it.
    const lake = await LakeDataset.open(lakeDir, DATASET)
    const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true } }, logs: [{ address: [ADDRESS] }] })
    assert.deepEqual((await lake.page(query, 0, 1)).logs.map((l) => l.block_number), [1])

    // And A cannot go on: its next batch finds the manifest changed.
    await assert.rejects(a.transact(tx(2, 3), async () => {}), Conflict)
  } finally {
    rmSync(lakeDir, { recursive: true, force: true })
  }
})

test('two processes starting a new dataset at once: only one creates its lake', async () => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  try {
    const store = openStore(lakeDir, DATASET)
    await store.write<Manifest>(MANIFEST_FILE, { height: -1, hash: '0x', chunks: [] }, null)
    await assert.rejects(store.write<Manifest>(MANIFEST_FILE, { height: -1, hash: '0x', chunks: [] }, null), Conflict)
  } finally {
    rmSync(lakeDir, { recursive: true, force: true })
  }
})

test('a writer started from one manifest refuses to continue from a later one', async () => {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  try {
    const store = openStore(lakeDir, DATASET)
    const pinned = await store.write<Manifest>(MANIFEST_FILE, { height: 9, hash: '0x9', chunks: [] }, null)
    // It read the registry alongside that manifest. Another process then saves a new contract and
    // commits block 10 before this one connects: its registry may be missing that contract.
    await store.write<Manifest>(MANIFEST_FILE, { height: 10, hash: '0x10', chunks: [] }, pinned)
    const late = openWriter(lakeDir, DATASET, store, { chunkSizeMb: 64, startAt: pinned })
    await assert.rejects(late.connect(), Conflict)

    // Started from the current manifest, it connects.
    const current = (await store.read<Manifest>(MANIFEST_FILE))!.version
    await openWriter(lakeDir, DATASET, store, { chunkSizeMb: 64, startAt: current }).connect()
  } finally {
    rmSync(lakeDir, { recursive: true, force: true })
  }
})
