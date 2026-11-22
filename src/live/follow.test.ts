import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test } from 'node:test'
import { DatasetConfig } from '../config'
import { Coverage, COVERAGE_FILE } from '../coverage'
import { Registry, REGISTRY_FILE } from '../discover'
import { Manifest, MANIFEST_FILE } from '../manifest'
import { Upstream } from '../serve/headers'
import { LakeDataset } from '../serve/lake'
import { parseQuery } from '../serve/query'
import { openStore, Store } from '../store'
import { LakeBlock, writeBlocks } from '../write'
import { openWriter } from '../writer'
import { follow } from './follow'
import { LiveView } from './view'

const DATASET = 'ethereum-mainnet'
const A = '0x' + 'aa'.repeat(20)
const config: DatasetConfig = { dataset: DATASET, chainId: 1, fromBlock: 0, contracts: [{ name: 'A', address: A }], factories: [] }

const block = (number: number): LakeBlock => ({
  header: { number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number * 1000 },
  logs: [{ logIndex: 0, transactionIndex: 0, transactionHash: `0xtx${number}`, address: A, topics: ['0x' + '01'.repeat(32)], data: '0x' }],
  transactions: [{ transactionIndex: 0, hash: `0xtx${number}`, from: '0xf', to: A, input: '0x' }],
})
const head = (n: number) => ({ height: n, hash: n < 0 ? '0x' : `0x${n}` })

/** Counts every outbound request, answering each with a block of another chain. */
let fetches = 0
const realFetch = globalThis.fetch
function countFetches(t: { after(f: () => void): void }) {
  fetches = 0
  globalThis.fetch = (async () => {
    fetches++
    return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { number: '0x7a120', hash: '0xforeign', parentHash: '0x0', timestamp: '0x1' } }))
  }) as typeof fetch
  t.after(() => (globalThis.fetch = realFetch))
}

test('the history a lake holds is served while its RPC is being checked, and no block is written until it is', async (t) => {
  countFetches(t)
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-follow-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A], complete: { height: 20 } }, null)
  await store.write<Registry>(REGISTRY_FILE, { dataset: DATASET, height: 20, contracts: [{ name: 'A', address: A }] }, null)
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64 })
  await db.connect()
  await db.transact({ prevHead: head(-1), nextHead: head(20), isOnTop: true } as never, async (s) => {
    writeBlocks(s as never, [block(10)])
    s.setForceFlush(true)
  })
  const before = await store.read<Manifest>(MANIFEST_FILE)

  // An RPC that never answers the chain id.
  let served: (view: LiveView) => void = () => {}
  const view = new Promise<LiveView>((resolve) => (served = resolve))
  let followed = false
  void follow(dir, store, DATASET, config, served, { checkSource: () => new Promise(() => {}) }).then(() => (followed = true))

  const durable = await view
  assert.deepEqual(await durable.head(), { number: 20, hash: '0x20' })
  const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true } }, logs: [{ address: [A] }] })
  const page = await durable.page(query, 0, 10)
  assert.deepEqual([page.logs.map((l) => l.block_number), page.upper.hash], [[10], '0x10'])
  // The header of block 20 is not in the files: a retryable error, never a header from the unchecked RPC.
  await assert.rejects(durable.page(query, 0, 20), Upstream)
  assert.equal(fetches, 0, 'no request left for the RPC')
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(followed, false, 'the follower waits for the check')
  assert.equal((await store.read<Manifest>(MANIFEST_FILE))!.version, before!.version, 'and nothing advanced the lake')
})

test('a lake holding RPC headers answers a header it lacks with a retryable error, and reads it from the RPC once allowed', async (t) => {
  countFetches(t)
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-follow-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  const store = openStore(dir, DATASET)
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64 })
  await db.connect()
  await db.transact({ prevHead: head(-1), nextHead: head(20), isOnTop: true } as never, async (s) => {
    writeBlocks(s as never, [block(10)])
    s.setForceFlush(true)
  })
  const lake = await LakeDataset.open(dir, DATASET)
  lake.holdRpcHeaders()
  assert.equal((await lake.header(10)).hash, '0x10')
  await assert.rejects(lake.header(500_000), Upstream)
  assert.equal(fetches, 0)
  lake.allowRpcHeaders()
  assert.equal((await lake.header(500_000)).hash, '0xforeign')
  assert.equal(fetches, 1)
})

test('a check that refuses the RPC stops the follow, with the lake as it stands served and nothing written', async (t) => {
  countFetches(t)
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-follow-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A], complete: { height: 20 } }, null)
  await store.write<Registry>(REGISTRY_FILE, { dataset: DATASET, height: 20, contracts: [{ name: 'A', address: A }] }, null)
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64 })
  await db.connect()
  await db.transact({ prevHead: head(-1), nextHead: head(20), isOnTop: true } as never, async (s) => {
    writeBlocks(s as never, [block(10)])
    s.setForceFlush(true)
  })
  const before = await store.read<Manifest>(MANIFEST_FILE)
  const served: LiveView[] = []
  await assert.rejects(
    follow(dir, store, DATASET, config, (view) => served.push(view), { checkSource: () => Promise.reject(new Error('the RPC serves chain 137')) }),
    /chain 137/
  )
  assert.equal(served.length, 1)
  assert.equal((await store.read<Manifest>(MANIFEST_FILE))!.version, before!.version)
  assert.equal(fetches, 0)
})

test('a lake whose naming is slow is served meanwhile, and its RPC headers come back once the RPC is checked', async (t) => {
  countFetches(t)
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-follow-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  const store = openStore(dir, DATASET)
  // A complete lake from before receipts: following it names it, and that write is held.
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A], complete: { height: 20 } }, null)
  await store.write<Registry>(REGISTRY_FILE, { dataset: DATASET, height: 20, contracts: [{ name: 'A', address: A }] }, null)
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64 })
  await db.connect()
  await db.transact({ prevHead: head(-1), nextHead: head(20), isOnTop: true } as never, async (s) => {
    writeBlocks(s as never, [block(10)])
    s.setForceFlush(true)
  })
  const held: Store = {
    read: (name) => store.read(name),
    version: (name) => store.version(name),
    write: (name, value, version) => (name === COVERAGE_FILE ? new Promise(() => {}) : store.write(name, value, version)),
  }
  let served: (view: LiveView) => void = () => {}
  const view = new Promise<LiveView>((resolve) => (served = resolve))
  let followed = false
  void follow(dir, held, DATASET, config, served, { checkSource: async () => {} }).then(() => (followed = true))
  const durable = await view
  const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true } }, logs: [{ address: [A] }] })
  assert.deepEqual((await durable.page(query, 0, 10)).logs.map((l) => l.block_number), [10])
  await new Promise((resolve) => setTimeout(resolve, 20))
  // The check passed: the header the files lack comes from the RPC, though the lake is not named yet.
  assert.equal((await durable.page(query, 0, 20)).upper.hash, '0xforeign')
  assert.equal(fetches, 1)
  assert.equal(followed, false, 'no writer before the lake is named')
})

test('a refused RPC ends the follow at once, though the lake is still being named', async (t) => {
  countFetches(t)
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-follow-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A], complete: { height: 20 } }, null)
  await store.write<Registry>(REGISTRY_FILE, { dataset: DATASET, height: 20, contracts: [{ name: 'A', address: A }] }, null)
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64 })
  await db.connect()
  await db.transact({ prevHead: head(-1), nextHead: head(20), isOnTop: true } as never, async (s) => {
    writeBlocks(s as never, [block(10)])
    s.setForceFlush(true)
  })
  const held: Store = {
    read: (name) => store.read(name),
    version: (name) => store.version(name),
    write: (name, value, version) => (name === COVERAGE_FILE ? new Promise(() => {}) : store.write(name, value, version)),
  }
  await assert.rejects(follow(dir, held, DATASET, config, () => {}, { checkSource: () => Promise.reject(new Error('the RPC serves chain 137')) }), /chain 137/)
})
