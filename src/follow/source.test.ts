import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DatasetConfig } from '../config'
import { Registry } from '../discover'
import { RpcCall, RpcClient, RpcError } from './rpc'
import { NodeBehind, rangeTooWide } from './fetch'
import { RpcSource } from './source'

const FACTORY = '0x' + 'fa'.repeat(20)
const STATIC = '0x' + '5a'.repeat(20)
const CREATED = '0x' + 'c0'.repeat(20)
const PROXY_CREATED = '0x' + 'cf'.repeat(32)
const hex = (n: number) => '0x' + n.toString(16)
const word = (address: string) => '0x' + '0'.repeat(24) + address.slice(2)

interface FakeLog {
  address: string
  block: number
  logIndex: number
  topics: string[]
  tx: string
}

/** A chain of a few blocks behind an RpcClient, with a provider limit on eth_getLogs ranges. */
function fakeRpc(logs: FakeLog[], options: { maxRange?: number; corruptHashOf?: number; headAt?: number; error?: string } = {}) {
  const calls: string[] = []
  const blockHash = (n: number) => `0xhash${n}`
  const answer = (c: RpcCall): unknown => {
    if (c.method === 'eth_getBlockByNumber') {
      const n = c.params[0] === 'finalized' ? 100 : parseInt(c.params[0] as string, 16)
      if (options.headAt !== undefined && n > options.headAt) return null
      return { number: hex(n), hash: blockHash(n), parentHash: blockHash(n - 1), timestamp: hex(1000 + n) }
    }
    if (c.method === 'eth_getTransactionByHash') {
      const log = logs.find((l) => l.tx === c.params[0])!
      return { hash: log.tx, from: '0xfrom', to: log.address, input: '0x', transactionIndex: hex(0), blockNumber: hex(log.block) }
    }
    if (c.method === 'eth_getLogs') {
      const f = c.params[0] as { address: string[]; topics?: string[][]; fromBlock: string; toBlock: string }
      const from = parseInt(f.fromBlock, 16)
      const to = parseInt(f.toBlock, 16)
      calls.push(`${from}-${to}:${f.address.length}`)
      if (options.error) return new RpcError(-32005, options.error)
      if (options.maxRange && to - from + 1 > options.maxRange) return new RpcError(-32602, `range ${to - from + 1} exceeds limit of ${options.maxRange}`)
      return logs
        .filter((l) => f.address.includes(l.address) && l.block >= from && l.block <= to && (!f.topics || f.topics[0].includes(l.topics[0])))
        .map((l) => ({
          logIndex: hex(l.logIndex),
          transactionIndex: hex(0),
          transactionHash: l.tx,
          blockHash: l.block === options.corruptHashOf ? '0xfork' : blockHash(l.block),
          blockNumber: hex(l.block),
          address: l.address,
          data: '0x',
          topics: l.topics,
        }))
    }
    throw new Error(`unexpected ${c.method}`)
  }
  const client = {
    async call(method: string, params: unknown[]) {
      const result = answer({ method, params })
      if (result instanceof RpcError) throw result
      return result
    },
    async batch(cs: RpcCall[]) {
      return cs.map(answer)
    },
  } as unknown as RpcClient
  return { client, calls }
}

const config: DatasetConfig = {
  dataset: 'polygon-mainnet',
  fromBlock: 0,
  contracts: [{ name: 'Static', address: STATIC }],
  factories: [{ name: 'Factory', address: FACTORY, fromBlock: 0, topic0: PROXY_CREATED, addressTopic: 1 }],
}

function source(client: RpcClient, options: Partial<{ maxRange: number; addressesPerCall: number }> = {}) {
  const registry: Registry = { dataset: config.dataset, height: 9, contracts: [{ name: 'Static', address: STATIC }] }
  const saved: number[] = []
  const s = new RpcSource(client, config, registry, async (r) => void saved.push(r.height), {
    maxRange: options.maxRange ?? 100,
    addressesPerCall: options.addressesPerCall ?? 500,
    pollMs: 1,
  })
  return { s, registry, saved }
}

const chain: FakeLog[] = [
  { address: STATIC, block: 12, logIndex: 0, topics: ['0x' + '01'.repeat(32)], tx: '0xt1' },
  { address: FACTORY, block: 15, logIndex: 2, topics: [PROXY_CREATED, word(CREATED)], tx: '0xt2' },
  { address: CREATED, block: 15, logIndex: 5, topics: ['0x' + '02'.repeat(32)], tx: '0xt2' },
  { address: CREATED, block: 18, logIndex: 1, topics: ['0x' + '03'.repeat(32)], tx: '0xt3' },
]

test('a contract created in a range has its logs from the creating block on', async () => {
  const { client } = fakeRpc(chain)
  const { s, registry, saved } = source(client)
  const blocks = await s.range(10, 20)

  assert.deepEqual(registry.contracts.map((c) => c.address), [STATIC, CREATED])
  assert.equal(registry.contracts[1].createdAt?.block, 15)
  assert.deepEqual(saved, [20])
  assert.deepEqual(
    blocks.map((b) => [b.header.number, b.logs.map((l) => `${l.address.slice(0, 4)}:${l.logIndex}`)]),
    [
      [12, ['0x5a:0']],
      [15, ['0xfa:2', '0xc0:5']],
      [18, ['0xc0:1']],
      [20, []],
    ]
  )
  assert.equal(blocks[0].header.timestamp, (1000 + 12) * 1000)
  assert.deepEqual(blocks[1].transactions.map((t) => t.hash), ['0xt2'])
})

test('a range the provider calls too wide is split until it accepts it, with the same result', async () => {
  const unlimited = await source(fakeRpc(chain).client).s.range(10, 20)
  const { client, calls } = fakeRpc(chain, { maxRange: 3 })
  const limited = await source(client).s.range(10, 20)
  assert.deepEqual(limited, unlimited)
  assert.ok(calls.some((c) => c.startsWith('10-20:')), 'the whole range is tried first')
  assert.ok(calls.some((c) => c.startsWith('10-12:')), 'then smaller ones')
})

test('addresses are asked in groups the provider accepts', async () => {
  const { client, calls } = fakeRpc(chain)
  const { s, registry } = source(client, { addressesPerCall: 1 })
  registry.contracts.push({ name: 'Other', address: '0x' + '0b'.repeat(20) })
  await s.range(10, 11)
  assert.deepEqual(calls.filter((c) => !c.endsWith(':1')), [])
})

test('a log whose block hash differs from its header is refused', async () => {
  const { client } = fakeRpc(chain, { corruptHashOf: 12 })
  const { s } = source(client)
  await assert.rejects(s.range(10, 20), /block hash/)
})

test('a node behind the range is refused, even when it answers the logs it has', async () => {
  const { client } = fakeRpc(chain, { headAt: 16 })
  const { s, registry } = source(client)
  await assert.rejects(s.range(10, 20), NodeBehind)
  assert.deepEqual(registry.contracts.map((c) => c.address), [STATIC], 'nothing is kept from a refused range')
})

test('a rate limit is not taken for a range too wide', async () => {
  assert.equal(rangeTooWide(new RpcError(-32005, 'query returned more than 10000 results')), true)
  assert.equal(rangeTooWide(new RpcError(-32005, 'block range exceeds limit of 10000')), true)
  assert.equal(rangeTooWide(new RpcError(-32005, 'rate limit exceeded')), false)
  assert.equal(rangeTooWide(new RpcError(429, 'Too Many Requests')), false)
  const { client, calls } = fakeRpc(chain, { error: 'rate limit exceeded' })
  await assert.rejects(source(client).s.range(10, 20), /rate limit/)
  assert.equal(calls.length, 1, 'the range is not split')
})

test('a failure that is not RPC trouble ends the stream instead of being retried', async () => {
  const { client } = fakeRpc(chain)
  const registry: Registry = { dataset: config.dataset, height: 9, contracts: [{ name: 'Static', address: STATIC }] }
  const failing = async () => {
    throw new Error('AccessDenied')
  }
  const s = new RpcSource(client, config, registry, failing, { maxRange: 100, addressesPerCall: 500, pollMs: 1 })
  await assert.rejects(async () => {
    for await (const _ of s.getFinalizedStream({ from: 10, to: 20 })) void _
  }, /AccessDenied/)
  assert.deepEqual(registry.contracts.map((c) => c.address), [STATIC], 'a contract is never followed before it is saved')
})
