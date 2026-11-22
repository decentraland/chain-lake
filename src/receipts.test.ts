import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test, TestContext } from 'node:test'
import { Logger } from '@subsquid/logger'
import { absorb, FetchHistory } from './absorb'
import { checkPortalChain, checkRpcChain } from './chain'
import { DatasetConfig, Factory } from './config'
import { Coverage, COVERAGE_FILE } from './coverage'
import { Registry, REGISTRY_FILE } from './discover'
import { createdIn } from './follow/fetch'
import { RpcClient } from './follow/rpc'
import { Manifest, MANIFEST_FILE } from './manifest'
import {
  appendReceipt,
  claims,
  digestOf,
  FilterObject,
  filterFile,
  Filters,
  Identity,
  identityOf,
  Ledger,
  Receipt,
  lazyIdentity,
  receiptId,
  refuseReplacedLake,
  ScanLog,
  scansFile,
} from './receipts'
import { LakeDataset } from './serve/lake'
import { parseQuery } from './serve/query'
import { openStore, Store } from './store'
import { LakeBlock, writeBlocks } from './write'
import { openWriter } from './writer'

const DATASET = 'ethereum-mainnet'
/** Followed from the start. */
const A = '0x' + 'aa'.repeat(20)
/** Absorbed later, with a log inside a chunk the lake already has. */
const X = '0x' + 'bb'.repeat(20)
/** Absorbed later, with no log at all. */
const Y = '0x' + 'dd'.repeat(20)
/** Absorbed after X, with a log in the chunk X's absorb rewrote. */
const Z = '0x' + 'ee'.repeat(20)
const FACTORY = '0x' + 'ff'.repeat(20)
const CHILD = '0x' + 'c1'.repeat(20)
const LATE_CHILD = '0x' + 'c2'.repeat(20)
const CREATED = '0x' + '11'.repeat(32)
const EVENT = '0x' + '22'.repeat(32)
const word = (address: string) => '0x' + '0'.repeat(24) + address.slice(2)

const quiet = { warn() {}, info() {}, error() {}, debug() {} } as unknown as Logger
const factory: Factory = { name: 'Factory', address: FACTORY, fromBlock: 5, topic0: CREATED, addressTopic: 1, childName: 'child' }
const config: DatasetConfig = { dataset: DATASET, chainId: 1, fromBlock: 0, contracts: [{ name: 'A', address: A }], factories: [] }
const gaining = (...contracts: DatasetConfig['contracts']): DatasetConfig => ({ ...config, contracts: [...config.contracts, ...contracts] })

type Log = { address: string; topics: string[] }
function block(number: number, logs: Log[], hash = `0x${number}`): LakeBlock {
  return {
    header: { number, hash, parentHash: `0x${number - 1}`, timestamp: number * 1000 },
    logs: logs.map((l, i) => ({ logIndex: i, transactionIndex: 0, transactionHash: `0xtx${number}`, address: l.address, topics: l.topics, data: '0x' })),
    transactions: logs.length ? [{ transactionIndex: 0, hash: `0xtx${number}`, from: '0xf', to: null, input: '0x' }] : [],
  }
}

const chain = new Map<number, LakeBlock>([
  [10, block(10, [{ address: A, topics: [EVENT] }])],
  [12, block(12, [{ address: Z, topics: [EVENT] }])],
  [15, block(15, [{ address: FACTORY, topics: [CREATED, word(CHILD)] }])],
  [20, block(20, [{ address: X, topics: [EVENT] }])],
  [25, block(25, [{ address: CHILD, topics: [EVENT] }])],
  [30, block(30, [{ address: A, topics: [EVENT] }])],
  [55, block(55, [{ address: FACTORY, topics: [CREATED, word(LATE_CHILD)] }])],
  [57, block(57, [{ address: LATE_CHILD, topics: [EVENT] }])],
])

/** What the lake was written with before it gained any contract: A's logs only. */
const onlyA = (n: number) => {
  const b = chain.get(n)!
  const logs = b.logs.filter((l) => l.address === A)
  return { ...b, logs, transactions: logs.length ? b.transactions : [] }
}

/** An RPC answering with the chain above, as `fetchFollowing` does: with the header of the range's end. */
const rpcSeeing = (hashAt: (n: number) => string = (n) => `0x${n}`): FetchHistory =>
  async (followed, factories, from, to) => {
    const inRange = [...chain.values()].filter((b) => b.header.number >= from && b.header.number <= to)
    const created = createdIn(inRange, factories)
    const addresses = new Set([...followed, ...created.map((c) => c.address)])
    const blocks = inRange.map((b) => ({ ...b, logs: b.logs.filter((l) => addresses.has(l.address)) })).filter((b) => b.logs.length > 0)
    if (!blocks.some((b) => b.header.number === to)) blocks.push(block(to, [], hashAt(to)))
    return { blocks, created }
  }

const head = (n: number) => ({ height: n, hash: n < 0 ? '0x' : `0x${n}` })
const tx = (from: number, to: number) => ({ prevHead: head(from), nextHead: head(to), isOnTop: true }) as never

function tempDir(t: TestContext): string {
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-receipts-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  return dir
}

/** A complete lake of A to block 50, every commit receipted: chunks [0, 20] and [21, 40], then an empty commit to 50. */
async function lake(t: TestContext) {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A], complete: { height: 50 } }, null)
  await store.write<Registry>(REGISTRY_FILE, { dataset: DATASET, height: 50, contracts: [{ name: 'A', address: A }] }, null)
  const identity = await identityOf(store, config)
  const scans = ScanLog.reading()
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64, receipts: { identity, source: 'rpc', lowerBound: 0, scans, filters: new Filters(store), log: quiet } })
  await db.connect()
  const commit = async (from: number, to: number, numbers: number[]) => {
    scans.add(from + 1, to, [A])
    await db.transact(tx(from, to), async (s) => {
      writeBlocks(s as never, numbers.map(onlyA))
      s.setForceFlush(true)
    })
  }
  await commit(-1, 20, [10])
  await commit(20, 40, [30])
  await commit(40, 50, [])
  return { dir, store, identity }
}

const manifestOf = async (store: Store) => (await store.read<Manifest>(MANIFEST_FILE))!.value
const ledgerOf = async (store: Store) => {
  const lakeId = (await store.read<Coverage>(COVERAGE_FILE))?.value.lakeId
  return lakeId ? (await store.read<Ledger>(scansFile(lakeId)))?.value : undefined
}
const loadFilter = (store: Store) => async (digest: string) => (await store.read<FilterObject>(filterFile(digest)))?.value

async function claimsAt(store: Store, identity: Identity, snapshot?: Manifest) {
  return claims(snapshot ?? (await manifestOf(store)), await ledgerOf(store), identity, loadFilter(store))
}

const absorbing = (dir: string, store: Store, identity: Identity, cfg: DatasetConfig, fetch: FetchHistory = rpcSeeing(), maxAttempts?: number) =>
  absorb({ lakeDest: dir, config: cfg, store, fetch, maxRange: 100, log: quiet, maxAttempts, receipts: { identity, filters: new Filters(store) } })

test('each commit is receipted with the blocks it moved the lake over, and a lake of receipted commits proves its addresses', async (t) => {
  const { store, identity } = await lake(t)
  const ledger = (await ledgerOf(store))!
  assert.deepEqual(
    ledger.receipts.map((r) => [r.kind, r.segments.map((s) => [s.from, s.to]), r.to, r.wrote.length]),
    [
      ['commit', [[0, 20]], 20, 1],
      ['commit', [[21, 40]], 40, 1],
      // An advance with no chunk is receipted too: the blocks were read and held nothing.
      ['commit', [[41, 50]], 50, 0],
    ]
  )
  const proven = await claimsAt(store, identity)
  assert.deepEqual(proven.finalized, { number: 50, hash: '0x50' })
  assert.deepEqual([...proven.from], [[A, 0]])
})

test('blocks an older writer committed without receipts are a gap: an address is proven only from past it', async (t) => {
  const { dir, store, identity } = await lake(t)
  // A build without receipts advances the lake, here with no chunk at all.
  const current = (await store.read<Manifest>(MANIFEST_FILE))!
  await store.write<Manifest>(MANIFEST_FILE, { ...current.value, height: 60, hash: '0x60' }, current.version)
  const meanwhile = await claimsAt(store, identity)
  assert.deepEqual(meanwhile.finalized, { number: 50, hash: '0x50' }, 'the claims stop at the last receipted block')
  assert.equal(meanwhile.from.get(A), 0)

  // A writer with receipts takes over from there.
  const pinned = (await store.read<Manifest>(MANIFEST_FILE))!
  const scans = ScanLog.reading()
  const db = openWriter(dir, DATASET, store, {
    chunkSizeMb: 64,
    startAt: pinned.version,
    receipts: { identity, source: 'rpc', lowerBound: 61, scans, filters: new Filters(store), log: quiet },
  })
  await db.connect()
  scans.add(61, 80, [A])
  await db.transact(tx(60, 80), async (s) => s.setForceFlush(true))
  const proven = await claimsAt(store, identity)
  assert.deepEqual(proven.finalized, { number: 80, hash: '0x80' })
  assert.equal(proven.from.get(A), 61)
})

test("a backfill's first commit claims nothing below the block it starts from, even with no log in it", async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A] }, null)
  const identity = await identityOf(store, config)
  const db = openWriter(dir, DATASET, store, {
    chunkSizeMb: 64,
    receipts: { identity, source: 'sqd-portal', lowerBound: 4_944_642, scans: ScanLog.constant([A]), filters: new Filters(store), log: quiet },
  })
  await db.connect()
  await db.transact(tx(-1, 4_944_700), async (s) => s.setForceFlush(true))
  const ledger = (await ledgerOf(store))!
  assert.deepEqual(ledger.receipts[0].segments.map((s) => [s.from, s.to]), [[4_944_642, 4_944_700]])
  assert.equal((await claimsAt(store, identity)).from.get(A), 4_944_642)
})

test('a commit whose blocks were not all read gets no receipt', async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A] }, null)
  const identity = await identityOf(store, config)
  const scans = ScanLog.reading()
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64, receipts: { identity, source: 'rpc', lowerBound: 0, scans, filters: new Filters(store), log: quiet } })
  await db.connect()
  scans.add(0, 9, [A])
  scans.add(12, 20, [A])
  await db.transact(tx(-1, 20), async (s) => s.setForceFlush(true))
  assert.equal(await ledgerOf(store), undefined)
})

test('a snapshot from before an absorb at the same height does not take its receipt; one after it does, and serves the log', async (t) => {
  const { dir, store, identity } = await lake(t)
  const m0 = await manifestOf(store)
  await absorbing(dir, store, identity, gaining({ name: 'X', address: X }))
  const m1 = await manifestOf(store)
  assert.equal(m1.height, m0.height)
  assert.equal(m1.hash, m0.hash)
  assert.notDeepEqual(m1.chunks, m0.chunks, 'the absorb rewrote the chunk holding block 20')

  // A replica still serving M0 reads the new ledger: its page lacks X's log, so X is not proven there.
  const before = await claimsAt(store, identity, m0)
  assert.equal(before.from.has(X), false)
  assert.equal(before.from.get(A), 0)

  const after = await claimsAt(store, identity, m1)
  assert.equal(after.from.get(X), 0)
  assert.equal(after.from.get(A), 0)
  assert.deepEqual(after.finalized, { number: 50, hash: '0x50' })
  const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true } }, logs: [{ address: [X] }] })
  assert.deepEqual((await (await LakeDataset.open(dir, DATASET)).page(query, 0, 50)).logs.map((l) => l.block_number), [20])
})

test('an absorb that found no log proves its address for any snapshot at its height, once its read ended on the lake’s block', async (t) => {
  const { dir, store, identity } = await lake(t)
  const m0 = await manifestOf(store)
  await absorbing(dir, store, identity, gaining({ name: 'Y', address: Y }))
  const receipt = (await ledgerOf(store))!.receipts.at(-1)!
  assert.deepEqual([receipt.kind, receipt.wrote, receipt.toHash], ['absorb', [], '0x50'])
  assert.equal((await claimsAt(store, identity, m0)).from.get(Y), 0)
})

/** The manifest, coverage and ledger, to tell that a refused absorb published nothing. */
const published = async (store: Store) => ({
  manifest: await manifestOf(store),
  coverage: (await store.read<Coverage>(COVERAGE_FILE))!.value,
  ledger: await ledgerOf(store),
})

test('an absorb whose read ended on another hash than the lake holds publishes nothing', async (t) => {
  const { dir, store, identity } = await lake(t)
  const before = await published(store)
  await assert.rejects(absorbing(dir, store, identity, gaining({ name: 'Y', address: Y }), rpcSeeing((n) => (n === 50 ? '0xother' : `0x${n}`))), /nothing was absorbed/)
  assert.deepEqual(await published(store), before)
})

test('an absorb whose first read ended on another hash is not saved by an extension that matches', async (t) => {
  const { dir, store, identity } = await lake(t)
  let moved = false
  const base = rpcSeeing((n) => (n === 50 ? '0xother' : `0x${n}`))
  const racing: FetchHistory = async (...args) => {
    if (!moved) {
      moved = true
      const current = (await store.read<Manifest>(MANIFEST_FILE))!
      await store.write<Manifest>(MANIFEST_FILE, { ...current.value, height: 60, hash: '0x60' }, current.version)
    }
    return base(...args)
  }
  const before = await published(store)
  await assert.rejects(absorbing(dir, store, identity, gaining({ name: 'Y', address: Y }), racing), /nothing was absorbed/)
  const after = await published(store)
  assert.deepEqual([after.manifest.chunks, after.coverage, after.ledger], [before.manifest.chunks, before.coverage, before.ledger])
  assert.equal(after.manifest.height, 60, 'only the follower moved the lake')
})

test('an absorb retried after the lake moved credits a child it found in the extension only for the extension', async (t) => {
  const { dir, store, identity } = await lake(t)
  let moved = false
  const base = rpcSeeing()
  const racing: FetchHistory = async (...args) => {
    if (!moved) {
      moved = true
      // A follower without receipts commits blocks 51-60 before the absorb publishes.
      const current = (await store.read<Manifest>(MANIFEST_FILE))!
      await store.write<Manifest>(MANIFEST_FILE, { ...current.value, height: 60, hash: '0x60' }, current.version)
    }
    return base(...args)
  }
  await absorbing(dir, store, identity, { ...config, factories: [factory] }, racing)
  const receipt = (await ledgerOf(store))!.receipts.at(-1)!
  assert.deepEqual(receipt.segments.map((s) => [s.from, s.to]), [[5, 50], [51, 60]])

  const proven = await claimsAt(store, identity)
  assert.deepEqual(proven.finalized, { number: 60, hash: '0x60' })
  assert.equal(proven.from.get(FACTORY), 5)
  assert.equal(proven.from.get(CHILD), 5)
  assert.equal(proven.from.get(LATE_CHILD), 51, 'blocks 5-50 were never asked for it')
  assert.equal(proven.from.has(A), false, 'nothing receipted blocks 51-60 for A')
})

test('a registry entry whose absorb lost the manifest is credited by no receipt', async (t) => {
  const { dir, store, identity } = await lake(t)
  const base = rpcSeeing()
  // Every read finds the lake moved on, so the one attempt allowed writes the registry and then loses.
  const racing: FetchHistory = async (...args) => {
    const current = (await store.read<Manifest>(MANIFEST_FILE))!
    await store.write<Manifest>(MANIFEST_FILE, { ...current.value, height: current.value.height + 1, hash: `0x${current.value.height + 1}` }, current.version)
    return base(...args)
  }
  await assert.rejects(absorbing(dir, store, identity, gaining({ name: 'X', address: X }), racing, 1), /kept moving/)
  assert.ok((await store.read<Registry>(REGISTRY_FILE))!.value.contracts.some((c) => c.address === X), 'the registry took X')
  assert.equal((await ledgerOf(store))!.receipts.some((r) => r.kind === 'absorb'), false)
  const proven = await claims({ height: 50, hash: '0x50', chunks: (await manifestOf(store)).chunks }, await ledgerOf(store), identity, loadFilter(store))
  assert.equal(proven.from.has(X), false)
  assert.equal(proven.from.get(A), 0)
})

test('moving a plain contract’s fromBlock earlier writes no receipt, so it stays proven from where it was read', async (t) => {
  const { dir, store, identity } = await lake(t)
  await absorbing(dir, store, identity, gaining({ name: 'X', address: X, fromBlock: 18 }))
  assert.equal((await claimsAt(store, identity)).from.get(X), 18)
  const receipts = (await ledgerOf(store))!.receipts.length
  await absorbing(dir, store, identity, gaining({ name: 'X', address: X, fromBlock: 10 }))
  assert.equal((await ledgerOf(store))!.receipts.length, receipts)
  assert.equal((await claimsAt(store, identity)).from.get(X), 18)
})

test('two empty absorbs at one height keep a receipt each, and a delayed retry of the first adds none', async (t) => {
  const { store, identity } = await lake(t)
  const filters = new Filters(store)
  const empty = async (address: string): Promise<Receipt> => ({
    kind: 'absorb',
    source: 'rpc',
    segments: [{ from: 0, to: 50, filter: await filters.digest([address]) }],
    to: 50,
    toHash: '0x50',
    wrote: [],
    replaced: {},
  })
  const [rx, ry] = [await empty(X), await empty(Y)]
  await appendReceipt(store, identity, rx)
  await appendReceipt(store, identity, ry)
  await appendReceipt(store, identity, rx)
  assert.equal((await ledgerOf(store))!.receipts.filter((r) => r.kind === 'absorb').length, 2)
  const proven = await claimsAt(store, identity)
  assert.deepEqual([proven.from.get(X), proven.from.get(Y)], [0, 0])
})

test('a receipt written while another process writes the ledger is added to what that process wrote', async (t) => {
  const { store, identity } = await lake(t)
  const filters = new Filters(store)
  const receipt = async (address: string): Promise<Receipt> => ({
    kind: 'absorb',
    source: 'rpc',
    segments: [{ from: 0, to: 50, filter: await filters.digest([address]) }],
    to: 50,
    toHash: '0x50',
    wrote: [],
    replaced: {},
  })
  const [rx, ry] = [await receipt(X), await receipt(Y)]
  let raced = false
  const racing: Store = {
    read: (name) => store.read(name),
    version: (name) => store.version(name),
    async write(name, value, version) {
      if (name === scansFile(identity.lakeId) && !raced) {
        raced = true
        await appendReceipt(store, identity, ry)
      }
      return store.write(name, value, version)
    },
  }
  await appendReceipt(racing, identity, rx)
  const ids = (await ledgerOf(store))!.receipts.filter((r) => r.kind === 'absorb')
  assert.equal(ids.length, 2)
})

test('a filter that only grew is written as what it added, and proves its new addresses from where it was first used', async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  const filters = new Filters(store)
  const first = await filters.digest([A])
  const grown = await filters.digest([X, A])
  const shrunk = await filters.digest([X])
  assert.deepEqual((await store.read<FilterObject>(filterFile(first)))!.value, { add: [A] })
  assert.deepEqual((await store.read<FilterObject>(filterFile(grown)))!.value, { parent: first, add: [X] })
  assert.deepEqual((await store.read<FilterObject>(filterFile(shrunk)))!.value, { add: [X] }, 'a set that lost an address is written whole')
  assert.equal(await filters.digest([A, X]), grown)

  const identity: Identity = { dataset: DATASET, chainId: 1, lakeId: 'lake' }
  const commit = (from: number, to: number, filter: string) => {
    const receipt: Receipt = { kind: 'commit', source: 'rpc', segments: [{ from, to, filter }], to, toHash: `0x${to}`, wrote: [], replaced: {} }
    return { id: receiptId(identity, receipt), ...receipt }
  }
  const ledger: Ledger = { schemaVersion: 1, ...identity, receipts: [commit(0, 20, first), commit(21, 50, grown)] }
  const proven = await claims({ height: 50, hash: '0x50', chunks: [] }, ledger, identity, loadFilter(store))
  assert.equal(proven.from.get(A), 0)
  assert.equal(proven.from.get(X), 21)
})

test('a ledger of another lake or schema, or a filter that is missing or does not match its name, proves nothing', async (t) => {
  const { store, identity } = await lake(t)
  const snapshot = await manifestOf(store)
  const ledger = (await ledgerOf(store))!
  const none = async (l: Ledger, load = loadFilter(store)) => (await claims(snapshot, l, identity, load)).from.size
  assert.equal(await none({ ...ledger, lakeId: 'another' }), 0)
  assert.equal(await none({ ...ledger, chainId: 137 }), 0)
  assert.equal(await none({ ...ledger, schemaVersion: 2 }), 0)
  assert.equal(await none(ledger, async () => undefined), 0)
  assert.equal(await none(ledger, async () => ({ add: [A, X] })), 0)
  assert.equal(await none(ledger), 1)
})

test('a late receipt of the lake a rebuild replaced lands in that lake’s ledger, never in the new one', async (t) => {
  const { store, identity } = await lake(t)
  const before = (await ledgerOf(store))!.receipts
  const filters = new Filters(store)
  const late: Receipt = { kind: 'commit', source: 'rpc', segments: [{ from: 51, to: 60, filter: await filters.digest([X]) }], to: 60, toHash: '0x60', wrote: [], replaced: {} }
  await appendReceipt(store, { ...identity, lakeId: 'the lake before' }, late)
  assert.deepEqual((await ledgerOf(store))!.receipts, before)
  assert.equal((await claimsAt(store, identity)).from.get(A), 0)
  // A ledger naming another chain is refused, and left as it was.
  await assert.rejects(appendReceipt(store, { ...identity, chainId: 137 }, late), /not to chain 137/)
  assert.deepEqual((await ledgerOf(store))!.receipts, before)
})

test('a receipt altered after it was written, or one that is not a receipt, proves nothing, and leaves the others usable', async (t) => {
  const { store, identity } = await lake(t)
  const ledger = (await ledgerOf(store))!
  const elsewhere = await new Filters(store).digest([X])
  const [first, second, third] = ledger.receipts
  // The last commit now says it read X, under the id it was written with.
  const altered = { ...third, segments: [{ ...third.segments[0], filter: elsewhere }] }
  const stored = { ...ledger, receipts: [first, null, { ...second, segments: [null] }, second, altered] } as unknown as Ledger
  const proven = await claims(await manifestOf(store), stored, identity, loadFilter(store))
  assert.equal(proven.from.has(X), false)
  assert.deepEqual(proven.finalized, { number: 40, hash: '0x40' }, 'the altered receipt counts for nothing')
  assert.equal(proven.from.get(A), 0, 'and the receipts that verify still prove A')
})

test('a lake is named once, and keeps its name', async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A] }, null)
  const first = await identityOf(store, config)
  assert.match(first.lakeId, /^[0-9a-f]{32}$/)
  assert.deepEqual(await identityOf(store, config), first)
  assert.deepEqual([first.dataset, first.chainId], [DATASET, 1])
})

test('a receipt id covers the whole receipt, so two receipts differing in one address differ', () => {
  const identity: Identity = { dataset: DATASET, chainId: 1, lakeId: 'lake' }
  const r = (filter: string): Receipt => ({ kind: 'absorb', source: 'rpc', segments: [{ from: 0, to: 9, filter }], to: 9, toHash: '0x9', wrote: [], replaced: {} })
  assert.notEqual(digestOf({ ...identity, ...r('a') }), digestOf({ ...identity, ...r('b') }))
})

test('the reads that cover a commit are taken clipped to it, and a commit with an unread block gets none', () => {
  const scans = ScanLog.reading()
  scans.add(0, 9, [A])
  scans.add(10, 30, [X, A])
  assert.deepEqual(scans.take(0, 20), [
    { from: 0, to: 9, addresses: [A] },
    { from: 10, to: 20, addresses: [A, X] },
  ])
  assert.deepEqual(scans.take(21, 30), [{ from: 21, to: 30, addresses: [A, X] }], 'a read spanning two commits serves both')
  scans.add(31, 35, [A])
  scans.add(37, 40, [A])
  assert.equal(scans.take(31, 40), undefined)
  assert.deepEqual(ScanLog.constant([X, A]).take(5, 9), [{ from: 5, to: 9, addresses: [A, X] }])
})

test('a source serving another chain is refused before it writes a receipt', async () => {
  const client = (chainId: string, hash: string) =>
    ({
      async call(method: string) {
        if (method === 'eth_chainId') return chainId
        return { number: '0x5', hash, parentHash: '0x4', timestamp: '0x1' }
      },
    }) as unknown as RpcClient
  await assert.rejects(checkRpcChain(client('0x89', '0xa'), config, quiet), /serves chain 137, and the dataset is chain 1/)
  await checkRpcChain(client('0x1', '0xa'), config, quiet)
  await assert.rejects(checkPortalChain({ number: 5, hash: '0xb' }, client('0x1', '0xa'), config, quiet), /different chains/)
  await checkPortalChain({ number: 5, hash: '0xa' }, client('0x1', '0xa'), config, quiet)
})

test('a receipt ending at the snapshot’s height on another hash proves nothing', async (t) => {
  const { store, identity } = await lake(t)
  const snapshot = await manifestOf(store)
  const proven = await claims({ ...snapshot, hash: '0xanother' }, await ledgerOf(store), identity, loadFilter(store))
  assert.deepEqual(proven.finalized, { number: 40, hash: '0x40' }, 'only the receipts below the snapshot’s height count')
  assert.equal(proven.from.get(A), 0)
})

test('a chunk a later absorb rewrote again still witnesses the first absorb', async (t) => {
  const { dir, store, identity } = await lake(t)
  await absorbing(dir, store, identity, gaining({ name: 'X', address: X }))
  const afterX = await manifestOf(store)
  await absorbing(dir, store, identity, gaining({ name: 'X', address: X }, { name: 'Z', address: Z }))
  const afterZ = await manifestOf(store)
  const [first, second] = (await ledgerOf(store))!.receipts.filter((r) => r.kind === 'absorb')
  assert.ok(first.wrote.every((d) => !afterZ.chunks.some((c) => c.dir === d)), 'the chunk X was written to is gone')
  assert.ok(first.wrote.every((d) => d in second.replaced))
  const proven = await claimsAt(store, identity, afterZ)
  assert.deepEqual([proven.from.get(X), proven.from.get(Z)], [0, 0])
  assert.equal((await claimsAt(store, identity, afterX)).from.has(Z), false)
  // A replacement link only counts from a receipt that verifies.
  const ledger = (await ledgerOf(store))!
  const forged = ledger.receipts.map((r) => (r.id === second.id ? { ...r, replaced: { ...r.replaced, [first.wrote[0]]: 'a folder no snapshot lists' } } : r))
  assert.equal((await claims(afterZ, { ...ledger, receipts: forged }, identity, loadFilter(store))).from.has(X), false)
})

test('a ledger this build cannot write leaves the commit published, without a receipt', async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A] }, null)
  const identity = await identityOf(store, config)
  await store.write(scansFile(identity.lakeId), { schemaVersion: 2, ...identity, receipts: [] }, null)
  const errors: string[] = []
  const log = { ...quiet, error: (_: unknown, message: string) => void errors.push(message) } as unknown as Logger
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64, receipts: { identity, source: 'rpc', lowerBound: 0, scans: ScanLog.constant([A]), filters: new Filters(store), log } })
  await db.connect()
  await db.transact(tx(-1, 20), async (s) => s.setForceFlush(true))
  assert.equal((await manifestOf(store)).height, 20)
  assert.equal((await store.read<{ schemaVersion: number }>(scansFile(identity.lakeId)))!.value.schemaVersion, 2, 'left as it was')
  assert.deepEqual(errors, ['blocks to 20 got no receipt'])
})

test('a receipt appended after an entry that is not a receipt, or one altered, is kept and proves what it says', async (t) => {
  const { store, identity } = await lake(t)
  const file = scansFile(identity.lakeId)
  const current = (await store.read<Ledger>(file))!
  const [first, second, third] = current.value.receipts
  // Junk, and the last commit altered under its own id: neither is the receipt appended below.
  const junk = [first, second, null, { ...third, segments: [{ ...third.segments[0], to: 49 }] }]
  await store.write(file, { ...current.value, receipts: junk }, current.version)
  const { id: _, ...last } = third
  await appendReceipt(store, identity, last as Receipt)
  const ledger = (await ledgerOf(store))!
  assert.equal(ledger.receipts.length, 5, 'the altered entry did not pass for it')
  assert.equal((await claimsAt(store, identity)).from.get(A), 0)
})

test('a lake is never started from nothing where another one stood', async (t) => {
  const { dir, store } = await lake(t)
  // The manifest is gone, but the coverage still names the lake that was here.
  rmSync(join(dir, DATASET, MANIFEST_FILE))
  await assert.rejects(refuseReplacedLake(store, config), /start the new lake in an empty location/)
  // A location discover just started names no lake yet.
  const fresh = openStore(tempDir(t), DATASET)
  await fresh.write<Coverage>(COVERAGE_FILE, { contracts: [A] }, null)
  await refuseReplacedLake(fresh, config)
})

test('a lake is named by its first receipt; one stopped before that leaves only a gap', async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A] }, null)
  // The first attempt to name the lake fails, as a process stopped there would.
  let refuse = true
  const flaky: Store = {
    read: (name) => store.read(name),
    version: (name) => store.version(name),
    async write(name, value, version) {
      if (name === COVERAGE_FILE && refuse) {
        refuse = false
        throw new Error('stopped')
      }
      return store.write(name, value, version)
    },
  }
  const scans = ScanLog.reading()
  const db = openWriter(dir, DATASET, store, {
    chunkSizeMb: 64,
    receipts: { identity: lazyIdentity(flaky, config), source: 'rpc', lowerBound: 0, scans, filters: new Filters(store), log: quiet },
  })
  await db.connect()
  scans.add(0, 20, [A])
  await db.transact(tx(-1, 20), async (s) => s.setForceFlush(true))
  assert.equal((await store.read<Coverage>(COVERAGE_FILE))!.value.lakeId, undefined, 'not named, and no receipt')
  scans.add(21, 40, [A])
  await db.transact(tx(20, 40), async (s) => s.setForceFlush(true))
  const identity = await identityOf(store, config)
  const proven = await claimsAt(store, identity)
  assert.deepEqual(proven.finalized, { number: 40, hash: '0x40' })
  assert.equal(proven.from.get(A), 21, 'the unreceipted first commit is a gap, never a claim')
})

test('of two processes starting one lake, only the one that publishes it names it', async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A] }, null)
  const starter = () => {
    const scans = ScanLog.reading()
    const db = openWriter(dir, DATASET, openStore(dir, DATASET), {
      chunkSizeMb: 64,
      receipts: { identity: lazyIdentity(openStore(dir, DATASET), config), source: 'rpc', lowerBound: 0, scans, filters: new Filters(store), log: quiet },
    })
    return { db, scans }
  }
  const [a, b] = [starter(), starter()]
  await a.db.connect()
  await b.db.connect()
  a.scans.add(0, 20, [A])
  await a.db.transact(tx(-1, 20), async (s) => s.setForceFlush(true))
  b.scans.add(0, 30, [A])
  await assert.rejects(b.db.transact(tx(-1, 30), async (s) => s.setForceFlush(true)))
  const coverage = (await store.read<Coverage>(COVERAGE_FILE))!.value
  const identity = await identityOf(store, config)
  assert.equal(identity.lakeId, coverage.lakeId)
  assert.deepEqual((await ledgerOf(store))!.receipts.map((r) => r.to), [20], 'the one that lost wrote nothing')
  assert.equal((await claimsAt(store, identity)).from.get(A), 0)
})

test('an absorb that read nothing writes no receipt', async (t) => {
  const { dir, store, identity } = await lake(t)
  const before = (await ledgerOf(store))!.receipts.length
  // X is deployed past the lake's height: there is nothing to read yet.
  await absorbing(dir, store, identity, gaining({ name: 'X', address: X, fromBlock: 60 }))
  assert.equal((await ledgerOf(store))!.receipts.length, before)
})

test('an answer to eth_chainId that is not a chain id is waited out, not taken for a chain', async () => {
  const answers: unknown[] = [null, '0x1']
  let calls = 0
  const client = {
    async call() {
      calls++
      return answers.shift()
    },
  } as unknown as RpcClient
  await checkRpcChain(client, config, quiet)
  assert.equal(calls, 2)
})
