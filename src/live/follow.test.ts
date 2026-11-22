import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test } from 'node:test'
import { DatasetConfig } from '../config'
import { Coverage, COVERAGE_FILE } from '../coverage'
import { Registry, REGISTRY_FILE } from '../discover'
import { Manifest, MANIFEST_FILE } from '../manifest'
import { parseQuery } from '../serve/query'
import { openStore } from '../store'
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

test('the history a lake holds is served while its RPC is being checked, and nothing is written until it is', async (t) => {
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
  assert.deepEqual((await durable.page(query, 0, 20)).logs.map((l) => l.block_number), [10])
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(followed, false, 'the follower waits for the check')
  assert.equal((await store.read<Manifest>(MANIFEST_FILE))!.version, before!.version, 'and nothing advanced the lake')
})
