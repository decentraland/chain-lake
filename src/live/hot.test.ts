import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DatasetConfig } from '../config'
import { Registry } from '../discover'
import { RpcCall, RpcClient } from '../follow/rpc'
import { parseQuery } from '../serve/query'
import { HotChain } from './hot'

const STATIC = '0x' + '5a'.repeat(20)
const FACTORY = '0x' + 'fa'.repeat(20)
const CREATED = '0x' + 'c0'.repeat(20)
const PROXY_CREATED = '0x' + 'cf'.repeat(32)
const EVENT = '0x' + '01'.repeat(32)
const hex = (n: number) => '0x' + n.toString(16)

interface FakeBlock {
  hash: string
  parentHash: string
  logs: { address: string; topics: string[] }[]
}

/** A chain the test can extend and reorganize, behind an RpcClient. */
class FakeChain {
  blocks = new Map<number, FakeBlock>()
  latest = 0

  extend(to: number, fork = 'a', logs: Record<number, FakeBlock['logs']> = {}) {
    for (let n = this.latest + 1; n <= to; n++) this.set(n, fork, logs[n] ?? [])
    this.latest = to
  }

  /** Replaces every block from `from` on with a fork. */
  reorg(from: number, fork: string, logs: Record<number, FakeBlock['logs']> = {}) {
    for (let n = from; n <= this.latest; n++) this.set(n, fork, logs[n] ?? [])
  }

  private set(n: number, fork: string, logs: FakeBlock['logs']) {
    this.blocks.set(n, { hash: `0x${fork}${n}`, parentHash: this.blocks.get(n - 1)?.hash ?? '0xgenesis', logs })
  }

  client(): RpcClient {
    const answer = (c: RpcCall): unknown => {
      if (c.method === 'eth_getBlockByNumber') {
        const n = c.params[0] === 'latest' ? this.latest : parseInt(c.params[0] as string, 16)
        const b = this.blocks.get(n)!
        return { number: hex(n), hash: b.hash, parentHash: b.parentHash, timestamp: hex(n) }
      }
      if (c.method === 'eth_getLogs') {
        const f = c.params[0] as { address: string[]; topics?: string[][]; fromBlock: string; toBlock: string }
        const out: unknown[] = []
        for (let n = parseInt(f.fromBlock, 16); n <= parseInt(f.toBlock, 16); n++) {
          this.blocks.get(n)!.logs.forEach((l, i) => {
            if (!f.address.includes(l.address) || (f.topics && !f.topics[0].includes(l.topics[0]))) return
            out.push({ logIndex: hex(i), transactionIndex: hex(i), transactionHash: `0xtx${n}-${i}`, blockHash: this.blocks.get(n)!.hash, blockNumber: hex(n), address: l.address, data: '0x', topics: l.topics })
          })
        }
        return out
      }
      if (c.method === 'eth_getTransactionByHash') {
        const [n, i] = (c.params[0] as string).slice(4).split('-').map(Number)
        return { hash: c.params[0], from: '0xf', to: null, input: '0x', transactionIndex: hex(i), blockNumber: hex(n), blockHash: this.blocks.get(n)!.hash }
      }
      throw new Error(`unexpected ${c.method}`)
    }
    return {
      call: async (method: string, params: unknown[]) => answer({ method, params }),
      batch: async (calls: RpcCall[]) => calls.map(answer),
    } as unknown as RpcClient
  }
}

const config: DatasetConfig = {
  dataset: 'ethereum-mainnet',
  fromBlock: 0,
  contracts: [{ name: 'Static', address: STATIC }],
  factories: [{ name: 'Factory', address: FACTORY, fromBlock: 0, topic0: PROXY_CREATED, addressTopic: 1 }],
}

function setup(chain: FakeChain, baseHeight: number, maxBlocks = 100) {
  const registry: Registry = { dataset: config.dataset, height: baseHeight, contracts: [] }
  const base = { height: baseHeight, hash: chain.blocks.get(baseHeight)!.hash }
  const hot = new HotChain(chain.client(), config, registry, () => base, { addressesPerCall: 10, maxBlocks })
  return { hot, base }
}

const query = parseQuery({ type: 'evm', fromBlock: 0, fields: { log: { address: true } }, logs: [{}] })

test('hot blocks run from the finalized base to the chain head', async () => {
  const chain = new FakeChain()
  chain.extend(20, 'a', { 18: [{ address: STATIC, topics: [EVENT] }] })
  const { hot } = setup(chain, 15)
  await hot.poll()

  assert.deepEqual(hot.head(), { number: 20, hash: '0xa20' })
  assert.equal(hot.size, 5)
  const page = hot.page(query, 16, 20)!
  assert.deepEqual(page.logs.map((l) => l.block_number), [18])
  assert.equal(page.upper.number, 20)
})

test('hot blocks finalized while a request waited are left to the finalized blocks', async () => {
  const chain = new FakeChain()
  chain.extend(20, 'a', { 18: [{ address: STATIC, topics: [EVENT] }] })
  const { hot, base } = setup(chain, 15)
  await hot.poll()

  // The follower finalizes up to block 19 before the next poll trims the hot blocks.
  Object.assign(base, { height: 19, hash: '0xa19' })
  assert.equal(hot.page(query, 16, 20), undefined, 'block 16 is no longer hot')
  assert.deepEqual(hot.page(query, 20, 20)!.upper.number, 20)

  // Nor is the head ever reported below the base.
  chain.extend(22)
  Object.assign(base, { height: 21, hash: '0xa21' })
  assert.deepEqual(hot.head(), { number: 21, hash: '0xa21' })
})

test('a reorg rebuilds the hot blocks from the base, with the new fork', async () => {
  const chain = new FakeChain()
  chain.extend(20, 'a', { 18: [{ address: STATIC, topics: [EVENT] }] })
  const { hot } = setup(chain, 15)
  await hot.poll()

  chain.reorg(17, 'b', { 19: [{ address: STATIC, topics: [EVENT] }] })
  chain.extend(22, 'b')
  await hot.poll()

  assert.deepEqual(hot.head(), { number: 22, hash: '0xb22' })
  assert.equal(hot.hashOf(18), '0xb18')
  assert.deepEqual(hot.page(query, 16, 22)!.logs.map((l) => l.block_number), [19])
})

test('a contract created in a hot block keeps being followed in later polls', async () => {
  const chain = new FakeChain()
  chain.extend(20, 'a', { 17: [{ address: FACTORY, topics: [PROXY_CREATED, '0x' + '0'.repeat(24) + CREATED.slice(2)] }] })
  const { hot } = setup(chain, 15)
  await hot.poll()

  chain.extend(23, 'a', { 22: [{ address: CREATED, topics: [EVENT] }] })
  await hot.poll()
  assert.deepEqual(hot.page(query, 16, 23)!.logs.map((l) => `${l.block_number}:${l.address.slice(0, 4)}`), ['17:0xfa', '22:0xc0'])
})

test('hot blocks are not served while the follower is far behind the head', async () => {
  const chain = new FakeChain()
  chain.extend(500)
  const { hot } = setup(chain, 100, 50)
  await hot.poll()
  assert.equal(hot.size, 0)
  assert.deepEqual(hot.head(), { number: 100, hash: '0xa100' })
})

test('a contract created in a hot block has its logs from that same block', async () => {
  const chain = new FakeChain()
  chain.extend(20, 'a', {
    17: [
      { address: FACTORY, topics: [PROXY_CREATED, '0x' + '0'.repeat(24) + CREATED.slice(2)] },
      { address: CREATED, topics: [EVENT] },
    ],
  })
  const { hot } = setup(chain, 15)
  await hot.poll()
  assert.deepEqual(hot.page(query, 16, 20)!.logs.map((l) => `${l.block_number}:${l.address.slice(0, 4)}`), ['17:0xfa', '17:0xc0'])
})

test('a reorg that does not make the chain longer is still noticed', async () => {
  const chain = new FakeChain()
  chain.extend(20, 'a')
  const { hot } = setup(chain, 15)
  await hot.poll()
  assert.deepEqual(hot.head(), { number: 20, hash: '0xa20' })

  // The head is replaced at the same height.
  chain.reorg(20, 'b')
  await hot.poll()
  assert.deepEqual(hot.head(), { number: 20, hash: '0xb20' })

  // And then by a shorter branch.
  chain.reorg(19, 'c')
  chain.latest = 19
  await hot.poll()
  assert.deepEqual(hot.head(), { number: 19, hash: '0xc19' })
})

test('hot blocks never answer for heights the follower has finalized', async () => {
  const chain = new FakeChain()
  chain.extend(20, 'a')
  const { hot, base } = setup(chain, 15)
  await hot.poll()
  Object.assign(base, { height: 18, hash: '0xa18' })
  assert.equal(hot.hashOf(17), undefined)
  assert.equal(hot.hashOf(18), undefined)
  assert.equal(hot.hashOf(19), '0xa19')
})
