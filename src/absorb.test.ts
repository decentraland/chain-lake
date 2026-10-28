import assert from 'node:assert/strict'
import { mkdtempSync, readdirSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test, TestContext } from 'node:test'
import { absorb, FetchHistory, missingContracts, planChunks } from './absorb'
import { bootstrap } from './bootstrap'
import { DatasetConfig, Factory } from './config'
import { Coverage, COVERAGE_FILE } from './coverage'
import { Registry, REGISTRY_FILE } from './discover'
import { createdIn } from './follow/fetch'
import { Manifest, MANIFEST_FILE } from './manifest'
import { LakeDataset } from './serve/lake'
import { parseQuery } from './serve/query'
import { openStore, Store } from './store'
import { LakeBlock, writeBlocks } from './write'
import { openWriter } from './writer'

const DATASET = 'ethereum-mainnet'
const OLD = '0x' + 'aa'.repeat(20)
const FACTORY = '0x' + 'ff'.repeat(20)
const CHILD = '0x' + 'cc'.repeat(20)
const CREATED = '0x' + '11'.repeat(32)
const EVENT = '0x' + '22'.repeat(32)
const word = (address: string) => '0x' + '0'.repeat(24) + address.slice(2)

const factory: Factory = { name: 'VestingFactory', address: FACTORY, fromBlock: 5, topic0: CREATED, addressTopic: 1, childName: 'vesting' }
const before: DatasetConfig = { dataset: DATASET, fromBlock: 0, contracts: [{ name: 'Old', address: OLD }], factories: [] }
const after: DatasetConfig = { ...before, factories: [factory] }

type Log = { address: string; topics: string[]; tx: number }
function block(number: number, logs: Log[]): LakeBlock {
  return {
    header: { number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number * 1000 },
    logs: logs.map((l, i) => ({ logIndex: i, transactionIndex: l.tx, transactionHash: `0xtx${number}-${l.tx}`, address: l.address, topics: l.topics, data: '0x' })),
    transactions: [...new Set(logs.map((l) => l.tx))].map((tx) => ({ transactionIndex: tx, hash: `0xtx${number}-${tx}`, from: '0xf', to: null, input: '0x' })),
  }
}

/** The chain as RPC sees it: the old contract's logs, and the factory's and its child's. */
const chain = new Map<number, LakeBlock>([
  [10, block(10, [{ address: OLD, topics: [EVENT], tx: 0 }])],
  [15, block(15, [{ address: FACTORY, topics: [CREATED, word(CHILD)], tx: 0 }])],
  // A block the old contract has logs in too: its header and its transaction are already in the lake.
  [20, block(20, [{ address: OLD, topics: [EVENT], tx: 0 }, { address: CHILD, topics: [EVENT], tx: 0 }])],
  [30, block(30, [{ address: OLD, topics: [EVENT], tx: 0 }])],
  [35, block(35, [{ address: CHILD, topics: [EVENT], tx: 1 }])],
  // Past the last chunk, below the lake's height: a gap the old contract left empty.
  [45, block(45, [{ address: CHILD, topics: [EVENT], tx: 0 }])],
])

/** What the follower wrote before the config gained the factory: the old contract only. */
const onlyOld = (n: number) => {
  const b = chain.get(n)!
  const logs = b.logs.filter((l) => l.address === OLD)
  return { ...b, logs, transactions: b.transactions.filter((t) => logs.some((l) => l.transactionIndex === t.transactionIndex)) }
}

/** An RPC that answers with the chain above, as `fetchFollowing` does. */
const rpc: FetchHistory = async (followed, factories, from, to) => {
  const inRange = [...chain.values()].filter((b) => b.header.number >= from && b.header.number <= to)
  const created = createdIn(inRange, factories)
  const addresses = new Set([...followed, ...created.map((c) => c.address)])
  const blocks = inRange.map((b) => ({ ...b, logs: b.logs.filter((l) => addresses.has(l.address)) }))
  return { blocks, created }
}

async function lake(t: TestContext) {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-absorb-'))
  t.after(() => rmSync(lakeDir, { recursive: true, force: true }))
  const store = openStore(lakeDir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [OLD], complete: { height: 50 } }, null)
  await store.write<Registry>(REGISTRY_FILE, { dataset: DATASET, height: 50, contracts: [{ name: 'Old', address: OLD }] }, null)
  const head = (n: number) => ({ height: n, hash: n < 0 ? '0x' : `0x${n}` })
  const tx = (from: number, to: number) => ({ prevHead: head(from), nextHead: head(to), isOnTop: true }) as never
  const db = openWriter(lakeDir, DATASET, store, { chunkSizeMb: 64 })
  await db.connect()
  const chunk = async (from: number, to: number, numbers: number[]) =>
    db.transact(tx(from, to), async (s) => {
      writeBlocks(s as never, numbers.map(onlyOld))
      s.setForceFlush(true)
    })
  await chunk(-1, 20, [10, 20])
  await chunk(20, 40, [30])
  // The follower then reached block 50 with nothing to write: no chunk, only the height.
  const current = (await store.read<Manifest>(MANIFEST_FILE))!
  await store.write<Manifest>(MANIFEST_FILE, { ...current.value, height: 50, hash: '0x50' }, current.version)
  return { lakeDir, store }
}

async function logsOf(lakeDir: string, address: string) {
  const lake = await LakeDataset.open(lakeDir, DATASET)
  const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true, transactionHash: true } }, logs: [{ address: [address], transaction: true }] })
  const page = await lake.page(query, 0, 50)
  return { blocks: page.logs.map((l) => l.block_number), transactions: page.transactions.map((t) => t.hash) }
}

test('the contracts a config gained are those its coverage lacks', () => {
  assert.deepEqual(missingContracts(after, { contracts: [OLD] }).map((c) => c.address), [FACTORY])
  assert.deepEqual(missingContracts(before, { contracts: [OLD] }), [])
})

test('a factory the config follows from an earlier block than the lake holds it from is missing too', () => {
  const covered = { contracts: [OLD, FACTORY] }
  assert.deepEqual(missingContracts(after, { ...covered, factories: { [FACTORY]: 9 } }).map((c) => c.address), [FACTORY])
  assert.deepEqual(missingContracts(after, { ...covered, factories: { [FACTORY]: 5 } }), [])
  assert.deepEqual(missingContracts(after, { ...covered, factories: { [FACTORY]: 2 } }), [])
  // A lake recorded before factories' blocks were: held from the configured block.
  assert.deepEqual(missingContracts(after, covered), [])
})

test('new history goes into the chunk covering each block, or into a chunk of its own in a gap', () => {
  const plan = planChunks(
    [
      { dir: 'b', from: 21, to: 40 },
      { dir: 'a', from: 0, to: 20 },
    ],
    [45, 3, 25, 41, 20]
  )
  assert.deepEqual(
    plan.map((p) => [p.chunk.from, p.chunk.to, p.original?.dir ?? 'new', p.blocks]),
    [
      [0, 20, 'a', [3, 20]],
      [21, 40, 'b', [25]],
      [41, 45, 'new', [41, 45]],
    ]
  )
})

test("a factory added to a complete lake comes with its whole history, and its children's, in chain order", async (t) => {
  const { lakeDir, store } = await lake(t)
  await absorb({ lakeDest: lakeDir, config: after, store, fetch: rpc, maxRange: 7 })

  // The factory's and its child's logs are served, with their transactions, where the chain has them.
  assert.deepEqual((await logsOf(lakeDir, CHILD)).blocks, [20, 35, 45])
  assert.deepEqual((await logsOf(lakeDir, FACTORY)).blocks, [15])
  assert.deepEqual((await logsOf(lakeDir, CHILD)).transactions, ['0xtx20-0', '0xtx35-1', '0xtx45-0'])
  // The old contract's are untouched, the block they share included.
  assert.deepEqual((await logsOf(lakeDir, OLD)).blocks, [10, 20, 30])

  const manifest = (await store.read<Manifest>(MANIFEST_FILE))!.value
  assert.equal(manifest.height, 50)
  assert.deepEqual(manifest.chunks.map((c) => [c.from, c.to]), [[0, 20], [21, 40], [45, 45]])
  const registry = (await store.read<Registry>(REGISTRY_FILE))!.value
  assert.deepEqual(
    registry.contracts.map((c) => [c.name, c.address]),
    [
      ['Old', OLD],
      ['VestingFactory', FACTORY],
      ['vesting', CHILD],
    ]
  )
  const coverage = (await store.read<Coverage>(COVERAGE_FILE))!.value
  assert.deepEqual(coverage.contracts, [OLD, FACTORY].sort())
  assert.deepEqual(coverage.factories, { [FACTORY]: 5 })
})

test('reading several ranges at once absorbs the same history', async (t) => {
  const { lakeDir, store } = await lake(t)
  await absorb({ lakeDest: lakeDir, config: after, store, fetch: rpc, maxRange: 3, concurrency: 4 })
  assert.deepEqual((await logsOf(lakeDir, CHILD)).blocks, [20, 35, 45])
  assert.deepEqual((await logsOf(lakeDir, FACTORY)).blocks, [15])
  assert.deepEqual((await store.read<Registry>(REGISTRY_FILE))!.value.contracts.map((c) => c.address), [OLD, FACTORY, CHILD])
})

/** A contract added on its own, deployed at block 18: before it, the chain holds nothing of it. */
const deployed: DatasetConfig = { ...before, contracts: [...before.contracts, { name: 'Store', address: CHILD, fromBlock: 18 }] }

test('a contract with a fromBlock is read from that block, not from the start of the dataset', async (t) => {
  const { lakeDir, store } = await lake(t)
  const from: number[] = []
  await absorb({ lakeDest: lakeDir, config: deployed, store, fetch: async (...args) => (from.push(args[2]), rpc(...args)), maxRange: 7 })
  assert.equal(Math.min(...from), 18)
  assert.deepEqual((await logsOf(lakeDir, CHILD)).blocks, [20, 35, 45])
})

test('each chunk is written as soon as all its blocks are read, before the blocks past it', async (t) => {
  const { lakeDir, store } = await lake(t)
  const chunksDir = join(lakeDir, DATASET, 'chunks')
  const before = new Set(readdirSync(chunksDir))
  // Whether the chunk of blocks 0-20 has been written again, with block 20's new log.
  const firstRewritten = () => readdirSync(chunksDir).some((d) => !before.has(d) && d.startsWith('0000000000-0000000020-'))
  const seen: [number, boolean][] = []
  const watching: FetchHistory = async (...args) => (seen.push([args[2], firstRewritten()]), rpc(...args))
  await absorb({ lakeDest: lakeDir, config: deployed, store, fetch: watching, maxRange: 7 })
  // Ranges 18-24 and 25-31: the first ends past the chunk, which is then written before the second is read.
  assert.deepEqual(seen.slice(0, 2), [
    [18, false],
    [25, true],
  ])
  assert.deepEqual((await logsOf(lakeDir, CHILD)).blocks, [20, 35, 45])
  assert.deepEqual((await logsOf(lakeDir, OLD)).blocks, [10, 20, 30])
})

test("a factory moved to an earlier block is absorbed again from there, with the children it created before", async (t) => {
  const { lakeDir, store } = await lake(t)
  await absorb({ lakeDest: lakeDir, config: after, store, fetch: rpc, maxRange: 7 })
  const EARLY = '0x' + 'ee'.repeat(20)
  chain.set(3, block(3, [{ address: FACTORY, topics: [CREATED, word(EARLY)], tx: 0 }]))
  chain.set(25, block(25, [{ address: EARLY, topics: [EVENT], tx: 0 }]))
  t.after(() => (chain.delete(3), chain.delete(25)))

  const earlier: DatasetConfig = { ...after, factories: [{ ...factory, fromBlock: 2 }] }
  await absorb({ lakeDest: lakeDir, config: earlier, store, fetch: rpc, maxRange: 7, concurrency: 2 })

  assert.deepEqual((await logsOf(lakeDir, FACTORY)).blocks, [3, 15])
  assert.deepEqual((await logsOf(lakeDir, EARLY)).blocks, [25])
  // What the first run absorbed is still there, once.
  assert.deepEqual((await logsOf(lakeDir, CHILD)).blocks, [20, 35, 45])
  assert.deepEqual((await store.read<Registry>(REGISTRY_FILE))!.value.contracts.map((c) => c.address), [OLD, FACTORY, CHILD, EARLY])
  assert.deepEqual((await store.read<Coverage>(COVERAGE_FILE))!.value.factories, { [FACTORY]: 2 })
})

test('coverage another process records meanwhile is kept', async (t) => {
  const { lakeDir, store } = await lake(t)
  const OTHER = '0x' + 'bb'.repeat(20)
  let raced = false
  const racing: FetchHistory = async (...args) => {
    if (!raced) {
      raced = true
      const current = (await store.read<Coverage>(COVERAGE_FILE))!
      await store.write<Coverage>(COVERAGE_FILE, { ...current.value, contracts: [...current.value.contracts, OTHER] }, current.version)
    }
    return rpc(...args)
  }
  await absorb({ lakeDest: lakeDir, config: after, store, fetch: racing, maxRange: 100 })
  assert.deepEqual((await store.read<Coverage>(COVERAGE_FILE))!.value.contracts, [OLD, OTHER, FACTORY].sort())
})

test('a lake that already holds every configured contract is left as it is', async (t) => {
  const { lakeDir, store } = await lake(t)
  await absorb({ lakeDest: lakeDir, config: after, store, fetch: rpc, maxRange: 7 })
  const manifest = await store.read<Manifest>(MANIFEST_FILE)
  let fetched = false
  await absorb({ lakeDest: lakeDir, config: after, store, fetch: async (...args) => ((fetched = true), rpc(...args)), maxRange: 7 })
  assert.equal(fetched, false)
  assert.equal((await store.read<Manifest>(MANIFEST_FILE))!.version, manifest!.version)
})

test('a follower that extends the lake meanwhile: the new blocks are absorbed too, and the run tries again', async (t) => {
  const { lakeDir, store } = await lake(t)
  chain.set(55, block(55, [{ address: CHILD, topics: [EVENT], tx: 0 }]))
  t.after(() => chain.delete(55))
  let moved = false
  const racing: FetchHistory = async (...args) => {
    if (!moved) {
      moved = true
      // The task a deployment replaces commits block 60 before this run publishes.
      const current = (await store.read<Manifest>(MANIFEST_FILE))!
      await store.write<Manifest>(MANIFEST_FILE, { ...current.value, height: 60, hash: '0x60' }, current.version)
    }
    return rpc(...args)
  }
  await absorb({ lakeDest: lakeDir, config: after, store, fetch: racing, maxRange: 100 })
  assert.equal((await store.read<Manifest>(MANIFEST_FILE))!.value.height, 60)
  assert.deepEqual((await logsOf(lakeDir, CHILD)).blocks, [20, 35, 45])
  const lakeAt60 = await LakeDataset.open(lakeDir, DATASET)
  const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true } }, logs: [{ address: [CHILD] }] })
  assert.deepEqual((await lakeAt60.page(query, 0, 60)).logs.map((l) => l.block_number), [20, 35, 45, 55])
})

test('bootstrap absorbs the contracts a complete lake gained before it is followed', async (t) => {
  const { store } = await lake(t)
  const ran: string[] = []
  await bootstrap(store, after, async (script) => {
    ran.push(script)
    const coverage = (await store.read<Coverage>(COVERAGE_FILE))!
    await store.write<Coverage>(COVERAGE_FILE, { ...coverage.value, contracts: [OLD, FACTORY] }, coverage.version)
  })
  assert.deepEqual(ran, ['absorb.js'])
})

test('a lake still backfilling refuses the contracts its config gained', async (t) => {
  const { store }: { store: Store } = await lake(t)
  const coverage = (await store.read<Coverage>(COVERAGE_FILE))!
  await store.write<Coverage>(COVERAGE_FILE, { contracts: coverage.value.contracts }, coverage.version)
  await assert.rejects(bootstrap(store, after, async () => {}), /joined the config after the backfill started/)
})
