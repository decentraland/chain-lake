import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test } from 'node:test'
import { DatasetConfig } from '../config'
import { Coverage, COVERAGE_FILE } from '../coverage'
import { Registry, REGISTRY_FILE } from '../discover'
import { Upstream } from '../serve/headers'
import { parseQuery } from '../serve/query'
import { openStore } from '../store'
import { LakeBlock, writeBlocks } from '../write'
import { openWriter } from '../writer'
import { DatasetState, Script, start } from './start'
import { LiveView } from './view'

const DATASET = 'ethereum-mainnet'
const A = '0x' + 'aa'.repeat(20)
const B = '0x' + 'bb'.repeat(20)
/** The config gained B after the lake was complete: starting absorbs it. */
const config: DatasetConfig = { dataset: DATASET, chainId: 1, fromBlock: 0, contracts: [{ name: 'A', address: A }, { name: 'B', address: B }], factories: [] }

const block = (number: number): LakeBlock => ({
  header: { number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number * 1000 },
  logs: [{ logIndex: 0, transactionIndex: 0, transactionHash: `0xtx${number}`, address: A, topics: ['0x' + '01'.repeat(32)], data: '0x' }],
  transactions: [{ transactionIndex: 0, hash: `0xtx${number}`, from: '0xf', to: A, input: '0x' }],
})
const head = (n: number) => ({ height: n, hash: n < 0 ? '0x' : `0x${n}` })

async function until(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 200 && !condition(); i++) await new Promise((resolve) => setTimeout(resolve, 5))
  assert.ok(condition(), 'it never happened')
}

test('a lake served while it absorbs reads no header from its RPC until this process checked it, and does once checked', async (t) => {
  let fetches = 0
  const realFetch = globalThis.fetch
  globalThis.fetch = (async () => {
    fetches++
    return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { number: '0x14', hash: '0xfromrpc', parentHash: '0x13', timestamp: '0x14' } }))
  }) as typeof fetch
  t.after(() => (globalThis.fetch = realFetch))

  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-start-'))
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

  let pass: () => void = () => {}
  const checked = new Promise<void>((resolve) => (pass = resolve))
  const states: DatasetState[] = []
  const ran: Script[] = []
  let served: (view: LiveView) => void = () => {}
  const view = new Promise<LiveView>((resolve) => (served = resolve))
  void start({
    lakeDest: dir,
    store,
    config,
    report: (state) => states.push(state),
    serve: served,
    // The absorb runs, and does not end within this test.
    runScript: async (script) => {
      ran.push(script)
      return new Promise(() => {})
    },
    checkSource: () => checked,
  })

  const absorbing = await view
  await until(() => ran.length > 0)
  assert.deepEqual([states, ran], [['absorbing'], ['absorb.js']])
  const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true } }, logs: [{ address: [A] }] })
  assert.deepEqual((await absorbing.page(query, 0, 10)).logs.map((l) => l.block_number), [10])
  await assert.rejects(absorbing.page(query, 0, 20), Upstream)
  assert.equal(fetches, 0, 'no header asked of the unchecked RPC')

  // This process checks the RPC while the absorb still runs: the header it lacks is read from there.
  pass()
  await new Promise((resolve) => setImmediate(resolve))
  const page = await absorbing.page(query, 0, 20)
  assert.deepEqual([page.logs.map((l) => l.block_number), page.upper.hash, fetches], [[10], '0xfromrpc', 1])
  assert.deepEqual(states, ['absorbing'], 'the absorb is still running')
})

test('a check that refuses the RPC makes the start fail', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'chain-lake-start-'))
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
  const followed: DatasetConfig = { ...config, contracts: [{ name: 'A', address: A }] }
  await assert.rejects(
    start({ lakeDest: dir, store, config: followed, report: () => {}, serve: () => {}, runScript: async () => {}, checkSource: () => Promise.reject(new Error('the RPC serves chain 137')) }),
    /chain 137/
  )
})
