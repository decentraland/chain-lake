import { test } from 'node:test'
import assert from 'node:assert/strict'
import { LakeDataset, Page } from '../serve/lake'
import { parseQuery } from '../serve/query'
import { LakeBlock } from '../write'
import { HotChain } from './hot'
import { matches, Tail } from './tail'
import { LiveView } from './view'

const A = '0x' + 'aa'.repeat(20)
const B = '0x' + 'bb'.repeat(20)
const T0 = '0x' + '01'.repeat(32)
const T1 = '0x' + '02'.repeat(32)

function block(number: number, logs: { address: string; topics: string[]; tx: number }[] = []): LakeBlock {
  return {
    header: { number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number * 1000 },
    logs: logs.map((l, i) => ({ logIndex: i, transactionIndex: l.tx, transactionHash: `0xtx${number}-${l.tx}`, address: l.address, topics: l.topics, data: '0x' })),
    transactions: [...new Set(logs.map((l) => l.tx))].map((tx) => ({ transactionIndex: tx, hash: `0xtx${number}-${tx}`, from: '0xf', to: A, input: '0x' })),
  }
}

const query = (logs: unknown[]) =>
  parseQuery({ type: 'evm', fromBlock: 0, fields: { block: { timestamp: true }, log: { address: true, topics: true }, transaction: { hash: true } }, logs })

test('an in-memory log matches a request exactly as the SQL condition does', () => {
  const log = { address: A.toUpperCase().replace('0X', '0x'), topics: [T0, T1] }
  assert.equal(matches({ address: [A] }, log), true)
  assert.equal(matches({ address: [B] }, log), false)
  assert.equal(matches({ topic1: [T1] }, log), true)
  assert.equal(matches({ topic2: [T0] }, log), false)
  assert.equal(matches({ address: [] }, log), false)
  assert.equal(matches({}, log), true)
})

test('the tail serves what it holds and drops what the lake files now have', () => {
  const tail = new Tail({ height: 10, hash: '0x10' })
  tail.add([block(12, [{ address: A, topics: [T0], tx: 0 }, { address: B, topics: [T0], tx: 1 }]), block(15)])
  tail.add([block(18, [{ address: A, topics: [T1], tx: 3 }]), block(20)])
  assert.deepEqual(tail.processed, { height: 20, hash: '0x20' })

  const page = tail.page(query([{ address: [A], transaction: true }]), 11, 20, { number: 20, hash: '0x20', parentHash: '0x19', timestamp: 20 })
  assert.deepEqual(page.logs.map((l) => [l.block_number, l.address === A]), [[12, true], [18, true]])
  assert.deepEqual(page.transactions.map((t) => t.hash), ['0xtx12-0', '0xtx18-3'])

  tail.written({ height: 15, hash: '0x15' })
  assert.equal(tail.size, 2)
  assert.equal(tail.headerOf(12), undefined)
  assert.equal(tail.headerOf(18)?.timestamp, 18)
  assert.throws(() => tail.add([block(19)]), /not after/)
})

test('a page never spans the lake files and the tail', async () => {
  const asked: [number, number][] = []
  const lake = { dataset: 'polygon-mainnet', page: async (_q: unknown, from: number, to: number) => (asked.push([from, to]), { from, to } as unknown as Page) } as unknown as LakeDataset
  const tail = new Tail({ height: 100, hash: '0x100' })
  tail.add([block(150)])
  const view = new LiveView(lake, tail)
  const q = query([{ address: [A] }])

  await view.page(q, 10, 90)
  await view.page(q, 90, 150)
  const fromTail = await view.page(q, 101, 150)
  assert.deepEqual(asked, [[10, 90], [90, 100]])
  assert.equal(fromTail.upper.number, 150)
  assert.deepEqual(await view.head(), { number: 150, hash: '0x150' })
})

test('the fork check trusts finalized blocks over hot ones kept from a branch that lost', async () => {
  const lake = { dataset: 'polygon-mainnet' } as unknown as LakeDataset
  const tail = new Tail({ height: 0, hash: '0x0' })
  // Hot blocks A1-A3 were served; then the follower finalized B1 and B2, before the next hot poll.
  const stale: Record<number, string> = { 1: '0xa1', 2: '0xa2', 3: '0xa3' }
  const chain = { hashOf: (n: number) => stale[n] } as unknown as HotChain
  const b = (number: number) => ({ ...block(number), header: { ...block(number).header, hash: `0xb${number}`, parentHash: `0xb${number - 1}` } })
  tail.add([b(1), b(2)])
  const view = new LiveView(lake, tail, chain)

  assert.equal(await view.hot!.hashAt(1), '0xb1', 'a client that continues from A1 must be told it forked')
  assert.equal(await view.hot!.hashAt(2), '0xb2')
  assert.equal(await view.hot!.hashAt(3), '0xa3', 'above the finalized head, the hot blocks decide')
})
