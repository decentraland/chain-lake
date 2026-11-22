import assert from 'node:assert/strict'
import { cpSync, mkdtempSync, readdirSync, renameSync, rmSync } from 'fs'
import { AddressInfo } from 'net'
import { tmpdir } from 'os'
import { join } from 'path'
import { test, TestContext } from 'node:test'
import { Logger } from '@subsquid/logger'
import { DatasetConfig } from '../config'
import { Coverage, COVERAGE_FILE } from '../coverage'
import { Tail } from '../live/tail'
import { LiveView } from '../live/view'
import { Manifest, MANIFEST_FILE } from '../manifest'
import { appendReceipt, FilterObject, Filters, Identity, identityOf, ScanLog } from '../receipts'
import { openStore, Store } from '../store'
import { LakeBlock, writeBlocks } from '../write'
import { openWriter } from '../writer'
import { ContractsAnswer, CoverageProvider, LakeReplaced } from './contracts'
import { LakeDataset, revisionOf } from './lake'
import { ChainView, createPortal, NotReady } from './server'
import { openStaticView, staticViews } from './static'

/** The dataset's configured chain (config/ethereum-mainnet.json), which a static view states coverage for. */
const DATASET = 'ethereum-mainnet'
const A = '0x' + 'aa'.repeat(20)
const X = '0x' + 'bb'.repeat(20)
const EVENT = '0x' + '22'.repeat(32)
const quiet = { warn() {}, info() {}, error() {}, debug() {} } as unknown as Logger
const config: DatasetConfig = { dataset: DATASET, chainId: 1, fromBlock: 0, contracts: [{ name: 'A', address: A }], factories: [] }

function block(number: number, logs = 1, address = A): LakeBlock {
  return {
    header: { number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number * 1000 },
    logs: Array.from({ length: logs }, (_, i) => ({ logIndex: i, transactionIndex: 0, transactionHash: `0xtx${number}`, address, topics: [EVENT], data: '0x' })),
    transactions: [{ transactionIndex: 0, hash: `0xtx${number}`, from: '0xf', to: address, input: '0x' }],
  }
}
const head = (n: number) => ({ height: n, hash: n < 0 ? '0x' : `0x${n}` })
const tx = (from: number, to: number) => ({ prevHead: head(from), nextHead: head(to), isOnTop: true }) as never

function tempDir(t: TestContext): string {
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-contracts-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  return dir
}

/** A complete lake of A to block 50, every commit receipted: logs at 10 and 30. */
async function lake(t: TestContext) {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A], complete: { height: 50 } }, null)
  const identity = await identityOf(store, config)
  const scans = ScanLog.reading()
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64, receipts: { identity, source: 'rpc', lowerBound: 0, scans, filters: new Filters(store), log: quiet } })
  await db.connect()
  const commit = async (from: number, to: number, numbers: number[]) => {
    scans.add(from + 1, to, [A])
    await db.transact(tx(from, to), async (s) => {
      writeBlocks(s as never, numbers.map((n) => block(n)))
      s.setForceFlush(true)
    })
  }
  await commit(-1, 20, [10])
  await commit(20, 40, [30])
  await commit(40, 50, [])
  return { dir, store, identity }
}

async function portal(t: TestContext, views: (dataset: string) => Promise<ChainView> | undefined): Promise<string> {
  const server = createPortal(views)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  t.after(() => server.close())
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}/datasets/${DATASET}`
}

const query = (from: number, to: number, address = A) => ({ type: 'evm', fromBlock: from, toBlock: to, fields: { log: { logIndex: true } }, logs: [{ address: [address] }] })
async function stream(base: string, route: string, body: unknown) {
  const res = await fetch(`${base}/${route}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
  const lines = (await res.text()).split('\n').filter(Boolean).map((l) => JSON.parse(l) as { header: { number: number }; logs?: unknown[] })
  return { status: res.status, revision: res.headers.get('x-lake-coverage-revision'), blocks: lines.filter((l) => l.logs?.length).map((l) => [l.header.number, l.logs!.length]) }
}

/** A store whose reads of some files wait for `release`, or fail once, as the test arranges. */
function holding(store: Store, matches: (name: string) => boolean) {
  let release: () => void = () => {}
  let held = new Promise<void>((resolve) => (release = resolve))
  let enter: () => void = () => {}
  const entered = new Promise<void>((resolve) => (enter = resolve))
  let failOnce = false
  const wrapped: Store = {
    async read(name) {
      if (matches(name)) {
        enter()
        if (failOnce) {
          failOnce = false
          throw new Error('storage unavailable')
        }
        await held
      }
      return store.read(name)
    },
    version: (name) => store.version(name),
    write: (name, value, version) => store.write(name, value, version),
  }
  return {
    store: wrapped,
    /** Resolves once a read the wrapper holds has started. */
    entered,
    release: () => release(),
    hold: () => (held = new Promise<void>((resolve) => (release = resolve))),
    failNext: () => (failOnce = true),
  }
}
const isFilter = (name: string) => name.startsWith('filter-')

test('a coverage answer and a page read from one manifest carry one revision', async (t) => {
  const { dir } = await lake(t)
  const base = await portal(t, staticViews(dir))
  const answer = (await (await fetch(`${base}/contracts`)).json()) as ContractsAnswer
  assert.deepEqual({ ...answer, coverageRevision: typeof answer.coverageRevision }, {
    schemaVersion: 1,
    dataset: DATASET,
    chainId: 1,
    coverageRevision: 'string',
    finalized: { number: 50, hash: '0x50' },
    contracts: [{ address: A, from: 0 }],
  })
  for (const route of ['stream', 'finalized-stream']) {
    const page = await stream(base, route, query(0, 50))
    assert.deepEqual([page.status, page.blocks, page.revision], [200, [[10, 1], [30, 1]], answer.coverageRevision])
  }
})

test('a block too large for a page is read again from the manifest the page started with, on both routes', async (t) => {
  // M0: block 30 holds three logs. M1, an absorb of the same height: a log at 20, and a fourth at 30.
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  const writeChunk = async (lakeDir: string, blocks: LakeBlock[]) => {
    const db = openWriter(lakeDir, DATASET, openStore(lakeDir, DATASET), { chunkSizeMb: 64 })
    await db.connect()
    await db.transact(tx(-1, 40), async (s) => {
      writeBlocks(s as never, blocks)
      s.setForceFlush(true)
    })
    return (await openStore(lakeDir, DATASET).read<Manifest>(MANIFEST_FILE))!.value
  }
  const m0 = await writeChunk(dir, [block(30, 3)])
  const other = tempDir(t)
  const written = await writeChunk(other, [block(20), block(30, 4)])
  for (const c of written.chunks) cpSync(join(other, DATASET, 'chunks', c.dir), join(dir, DATASET, 'chunks', c.dir), { recursive: true })
  const m1: Manifest = { ...m0, chunks: written.chunks }

  const lakeView = await LakeDataset.open(dir, DATASET)
  lakeView.maxLogs = 2
  lakeView.manifestTtlMs = -1
  let switched = false
  const internals = lakeView as unknown as { store: Store; rows: (sql: string) => Promise<unknown[]> }
  internals.store = {
    read: async <T>(name: string) => (name === MANIFEST_FILE ? { value: (switched ? m1 : m0) as T, version: switched ? 'm1' : 'm0' } : store.read<T>(name)),
    version: async (name) => (name === MANIFEST_FILE ? (switched ? 'm1' : 'm0') : store.version(name)),
    write: () => Promise.reject(new Error('read only')),
  }
  // The manifest moves to M1 once the page's capped query has read M0.
  const rows = internals.rows.bind(lakeView)
  internals.rows = async (sql) => {
    const result = await rows(sql)
    if (sql.includes('LIMIT')) switched = true
    return result
  }
  const base = await portal(t, () => Promise.resolve(lakeView))
  for (const route of ['stream', 'finalized-stream']) {
    switched = false
    const page = await stream(base, route, query(0, 40))
    assert.deepEqual([page.status, page.blocks, page.revision], [200, [[30, 3]], revisionOf(m0)], route)
    assert.ok(switched, 'the manifest did move during the page')
  }
})

/** Rebuilds the lake at `dir` as another one at the same height and hash: other chunk folders, a receipt for X. */
async function rebuild(dir: string, store: Store): Promise<Identity> {
  const coverage = (await store.read<Coverage>(COVERAGE_FILE))!
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A, X], complete: { height: 50 }, lakeId: 'b'.repeat(32) }, coverage.version)
  const manifest = (await store.read<Manifest>(MANIFEST_FILE))!
  const chunks = manifest.value.chunks.map((c) => {
    const dir2 = c.dir.replace(/-[0-9a-f]+$/, '-' + 'f'.repeat(32))
    cpSync(join(dir, DATASET, 'chunks', c.dir), join(dir, DATASET, 'chunks', dir2), { recursive: true })
    return { ...c, dir: dir2 }
  })
  await store.write<Manifest>(MANIFEST_FILE, { ...manifest.value, chunks }, manifest.version)
  const identity: Identity = { dataset: DATASET, chainId: 1, lakeId: 'b'.repeat(32) }
  await appendReceipt(store, identity, { kind: 'commit', source: 'rpc', segments: [{ from: 0, to: 50, filter: await new Filters(store).digest([X]) }], to: 50, toHash: '0x50', wrote: [], replaced: {} })
  return identity
}

test('a view bound to one lake answers 503 once the location holds another at the same height, and drops only itself', async (t) => {
  const { dir, store } = await lake(t)
  const views = staticViews(dir)
  const first = views(DATASET)!
  const before = await (await first).contracts!()
  assert.deepEqual(before.contracts, [{ address: A, from: 0 }])

  await rebuild(dir, store)
  await assert.rejects((await first).contracts!(), LakeReplaced)
  const second = views(DATASET)!
  assert.notEqual(second, first, 'the next request opens the location anew')
  const after = await (await second).contracts!()
  assert.deepEqual(after.contracts, [{ address: X, from: 0 }])
  assert.notEqual(after.coverageRevision, before.coverageRevision)

  // A late request of the replaced view leaves the new one in place.
  await assert.rejects((await first).contracts!(), LakeReplaced)
  assert.equal(views(DATASET), second)
})

test('an answer in flight when the location switches to another lake is a 503, never the other lake’s coverage', async (t) => {
  const { dir, store, identity } = await lake(t)
  const reads = holding(store, isFilter)
  const provider = new CoverageProvider(reads.store, identity, { static: true })
  const answer = provider.contracts()
  await reads.entered
  await rebuild(dir, store)
  reads.release()
  await assert.rejects(answer, LakeReplaced)
})

test('an answer in flight when the lake is marked partial is a 503, and so is every later one', async (t) => {
  const { dir, store, identity } = await lake(t)
  const reads = holding(store, isFilter)
  const provider = new CoverageProvider(reads.store, identity, { static: true })
  const answer = provider.contracts()
  await reads.entered
  const coverage = (await store.read<Coverage>(COVERAGE_FILE))!
  await store.write<Coverage>(COVERAGE_FILE, { ...coverage.value, partial: 'a development run left history out' }, coverage.version)
  reads.release()
  await assert.rejects(answer, NotReady)
  await assert.rejects(provider.contracts(), NotReady)
})

test('a live replica states no coverage past the block it serves from its files', async (t) => {
  const { dir, store, identity } = await lake(t)
  const view = await LakeDataset.open(dir, DATASET)
  view.manifestTtlMs = -1
  let files = { height: 50, hash: '0x50' }
  const provider = new CoverageProvider(store, identity, { live: view, files: () => files })
  assert.deepEqual((await provider.contracts()).finalized, { number: 50, hash: '0x50' })

  // Another writer extends the lake, with its receipts, while this replica's files stay at 50.
  const pinned = (await store.read<Manifest>(MANIFEST_FILE))!
  const scans = ScanLog.reading()
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64, startAt: pinned.version, receipts: { identity, source: 'rpc', lowerBound: 51, scans, filters: new Filters(store), log: quiet } })
  await db.connect()
  scans.add(51, 60, [A])
  await db.transact(tx(50, 60), async (s) => s.setForceFlush(true))
  await assert.rejects(provider.contracts(), /serves the lake's files up to 50/)

  files = { height: 60, hash: '0x60' }
  assert.deepEqual((await provider.contracts()).finalized, { number: 60, hash: '0x60' })
})

test('a filter read that fails is a 503, and the answer once it reads is whole', async (t) => {
  const { dir, store, identity } = await lake(t)
  const reads = holding(store, isFilter)
  reads.release()
  reads.failNext()
  const provider = new CoverageProvider(reads.store, identity, { static: true })
  await assert.rejects(provider.contracts(), /a filter could not be read/)
  assert.deepEqual((await provider.contracts()).contracts, [{ address: A, from: 0 }])
})

test('with no receipt counting the answer has no height; with receipts that prove no address, it has one and lists none', async (t) => {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A], complete: { height: 50 } }, null)
  const identity = await identityOf(store, config)
  await store.write<Manifest>(MANIFEST_FILE, { height: 50, hash: '0x50', chunks: [] }, null)
  const provider = new CoverageProvider(store, identity, { static: true })
  const none = await provider.contracts()
  assert.deepEqual([none.finalized, none.contracts], [null, []])
  await appendReceipt(store, identity, { kind: 'commit', source: 'rpc', segments: [{ from: 0, to: 50, filter: await new Filters(store).digest([]) }], to: 50, toHash: '0x50', wrote: [], replaced: {} })
  const empty = await provider.contracts()
  assert.deepEqual([empty.finalized, empty.contracts], [{ number: 50, hash: '0x50' }, []])
})

test('a view that states no coverage answers 503; another method 405; an unknown dataset 404', async (t) => {
  const { dir } = await lake(t)
  const durable = new LiveView(await LakeDataset.open(dir, DATASET), new Tail({ height: 50, hash: '0x50' }))
  const base = await portal(t, (dataset) => (dataset === DATASET ? Promise.resolve(durable) : undefined))
  assert.equal((await fetch(`${base}/contracts`)).status, 503)
  assert.equal((await fetch(`${base}/contracts`, { method: 'POST', body: '{}' })).status, 405)
  assert.equal((await fetch(base.replace(DATASET, 'polygon-mainnet') + '/contracts')).status, 404)
})

test('a lake not complete, marked partial, or without receipts states no coverage', async (t) => {
  for (const coverage of [{ contracts: [A] }, { contracts: [A], complete: { height: 50 }, partial: 'left out' }, { contracts: [A], complete: { height: 50 } }] as Coverage[]) {
    const { dir, store } = await lake(t)
    const current = (await store.read<Coverage>(COVERAGE_FILE))!
    const lakeId = 'lakeId' in coverage ? undefined : current.value.lakeId
    await store.write<Coverage>(COVERAGE_FILE, { ...coverage, ...(coverage.complete && !coverage.partial ? {} : { lakeId }) }, current.version)
    const view = await openStaticView(dir, DATASET, () => {})
    await assert.rejects(view.contracts!(), NotReady)
  }
})

test('an address no read asked for, as MANA on a dataset that does not follow it, is not listed', async (t) => {
  const { dir } = await lake(t)
  const answer = await (await openStaticView(dir, DATASET, () => {})).contracts!()
  assert.equal(answer.contracts.some((c) => c.address === X), false)
  assert.ok(readdirSync(join(dir, DATASET)).some((f) => f.startsWith('filter-')))
  const filters = readdirSync(join(dir, DATASET)).filter((f) => f.startsWith('filter-'))
  for (const f of filters) assert.ok(!((await openStore(dir, DATASET).read<FilterObject>(f))!.value.add.includes(X)))
})

/**
 * A complete lake at its own location to block 50 (hash 0x50), with X's log at 20 and a receipt that
 * covers X from block 0; named `lakeId`, or by itself.
 */
async function lakeWithX(t: TestContext, lakeId?: string, height = 50) {
  const dir = tempDir(t)
  const store = openStore(dir, DATASET)
  await store.write<Coverage>(COVERAGE_FILE, { contracts: [A, X], complete: { height }, ...(lakeId ? { lakeId } : {}) }, null)
  const identity = await identityOf(store, config)
  const scans = ScanLog.reading()
  const db = openWriter(dir, DATASET, store, { chunkSizeMb: 64, receipts: { identity, source: 'rpc', lowerBound: 0, scans, filters: new Filters(store), log: quiet } })
  await db.connect()
  scans.add(0, height, [A, X])
  await db.transact(tx(-1, height), async (s) => {
    writeBlocks(s as never, [block(10), block(20, 1, X)])
    s.setForceFlush(true)
  })
  return { dir, identity, manifest: (await store.read<Manifest>(MANIFEST_FILE))!.value }
}

function switchLakes(dir: string, replacement: string) {
  renameSync(join(dir, DATASET), join(dir, `${DATASET}.before`))
  renameSync(join(replacement, DATASET), join(dir, DATASET))
}

test('a static view warmed on one lake states the coverage of another switched in only under that one’s revision', async (t) => {
  const { dir } = await lake(t)
  const b = await lakeWithX(t)
  const base = await portal(t, staticViews(dir))
  // A page warms the view on lake A, which holds no log of X.
  const warm = await stream(base, 'finalized-stream', query(0, 50, X))
  assert.deepEqual(warm.blocks, [])
  switchLakes(dir, b.dir)
  const answer = (await (await fetch(`${base}/contracts`)).json()) as ContractsAnswer
  assert.ok(answer.contracts.some((c) => c.address === X), 'B covers X')
  assert.equal(answer.coverageRevision, revisionOf(b.manifest), 'under B’s revision, never A’s')
  assert.notEqual(answer.coverageRevision, warm.revision)
})

test('a static view bound to a lake states the coverage of a copy of it with other files only under the copy’s revision', async (t) => {
  const { dir, identity } = await lake(t)
  const base = await portal(t, staticViews(dir))
  const before = (await (await fetch(`${base}/contracts`)).json()) as ContractsAnswer
  const warm = await stream(base, 'finalized-stream', query(0, 50, X))
  assert.equal(warm.revision, before.coverageRevision)
  // A copy naming the same lake, with other files and another ledger.
  const copy = await lakeWithX(t, identity.lakeId)
  switchLakes(dir, copy.dir)
  const answer = (await (await fetch(`${base}/contracts`)).json()) as ContractsAnswer
  assert.ok(answer.contracts.some((c) => c.address === X))
  assert.equal(answer.coverageRevision, revisionOf(copy.manifest), 'under the copy’s revision, never the cached one')
})

test('a static view serves an older copy of its lake at that copy’s lower height, pages and coverage alike', async (t) => {
  const { dir, identity } = await lake(t)
  const base = await portal(t, staticViews(dir, { manifestTtlMs: 1 }))
  // A page warms the view at height 50.
  assert.equal((await stream(base, 'finalized-stream', query(0, 50))).status, 200)
  assert.equal(((await (await fetch(`${base}/contracts`)).json()) as ContractsAnswer).finalized?.number, 50)
  const older = await lakeWithX(t, identity.lakeId, 40)
  switchLakes(dir, older.dir)
  await new Promise((resolve) => setTimeout(resolve, 5))
  const answer = (await (await fetch(`${base}/contracts`)).json()) as ContractsAnswer
  assert.deepEqual([answer.finalized, answer.coverageRevision], [{ number: 40, hash: '0x40' }, revisionOf(older.manifest)])
  // Once its cache expires, the view serves the copy: its head, and X's log with the admitted revision.
  assert.deepEqual(await (await fetch(`${base}/finalized-head`)).json(), { number: 40, hash: '0x40' })
  const page = await stream(base, 'finalized-stream', query(0, 40, X))
  assert.deepEqual([page.status, page.blocks, page.revision], [200, [[20, 1]], answer.coverageRevision])
})

test('a static answer whose lake is switched for a copy of it while the ledger is read is a 503', async (t) => {
  const { dir, store, identity } = await lake(t)
  const copy = await lakeWithX(t, identity.lakeId)
  const reads = holding(store, (name) => name.startsWith('scans-'))
  const provider = new CoverageProvider(reads.store, identity, { static: true })
  const answer = provider.contracts()
  await reads.entered
  // The manifest was read from A; the ledger is read from the copy, whose receipts cover X.
  switchLakes(dir, copy.dir)
  reads.release()
  await assert.rejects(answer, /the manifest changed while the answer was computed/)
  const settled = await provider.contracts()
  assert.equal(settled.coverageRevision, revisionOf(copy.manifest))
})
