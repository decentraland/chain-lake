import { test } from 'node:test'
import assert from 'node:assert/strict'
import { AddressInfo } from 'net'
import { Page } from './lake'
import { blockLines, ChainView, createPortal } from './server'
import { BadQuery, logCondition, parseQuery } from './query'

const ADDRESS = '0x1c436c1efb4608dffdc8bace99d2b03c314f3348'
const TOPIC = '0x' + 'ab'.repeat(32)

const base = {
  type: 'evm',
  fromBlock: 10,
  fields: { block: { timestamp: true }, log: { topics: true, data: true }, transaction: { to: true } },
  logs: [{ address: [ADDRESS.toUpperCase().replace('0X', '0x')], topic0: [TOPIC], transaction: true }],
}

test('a query is parsed with lowercase filters and the fields it asks for', () => {
  const q = parseQuery(base)
  assert.deepEqual(q.logs[0].address, [ADDRESS])
  assert.equal(q.logs[0].transaction, true)
  assert.deepEqual([...q.fields.log], ['topics', 'data'])
  assert.equal(logCondition(q.logs[0]), `(address IN ('${ADDRESS}') AND topic0 IN ('${TOPIC}'))`)
})

test('what the lake cannot serve is rejected, not answered with less data', () => {
  assert.throws(() => parseQuery({ ...base, traces: [{}] }), BadQuery)
  assert.throws(() => parseQuery({ ...base, includeAllBlocks: true }), BadQuery)
  assert.throws(() => parseQuery({ ...base, fields: { block: { gasUsed: true } } }), BadQuery)
  assert.throws(() => parseQuery({ ...base, logs: [{ address: ['0x12'] }] }), BadQuery)
  assert.throws(() => parseQuery({ ...base, logs: [{ transactionTraces: true }] }), BadQuery)
})

test('an empty filter list matches nothing, a missing one matches everything', () => {
  assert.equal(logCondition({ address: [] }), '(FALSE)')
  assert.equal(logCondition({}), 'TRUE')
})

test('blocks carry the requested fields, transactions before logs, and end with the covered block', () => {
  const q = parseQuery(base)
  const header = (number: number) => ({ number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number * 2 })
  const lines = [
    ...blockLines(
      q,
      header(20),
      new Map([[12, header(12)]]),
      [{ block_number: 12, log_index: 3, transaction_index: 1, transaction_hash: '0xt', address: ADDRESS, topic0: TOPIC, topic1: null, topic2: null, topic3: null, data: '0x', with_transaction: true }],
      [{ block_number: 12, transaction_index: 1, hash: '0xt', from: '0xf', to: '0xto', input: '0x' }]
    ),
  ].map((l) => JSON.parse(l))

  assert.deepEqual(lines, [
    {
      header: { number: 12, hash: '0x12', timestamp: 24 },
      transactions: [{ transactionIndex: 1, to: '0xto' }],
      logs: [{ logIndex: 3, transactionIndex: 1, topics: [TOPIC], data: '0x' }],
    },
    { header: { number: 20, hash: '0x20', timestamp: 40 } },
  ])
  assert.deepEqual(Object.keys(lines[0]), ['header', 'transactions', 'logs'])
})

/** A portal over one dataset, on a free port, for the duration of `f`. */
async function withPortal(chain: ChainView, f: (url: string) => Promise<void>) {
  const server = createPortal((dataset) => (dataset === 'polygon-mainnet' ? Promise.resolve(chain) : undefined))
  await new Promise<void>((resolve) => server.listen(0, resolve))
  try {
    await f(`http://localhost:${(server.address() as AddressInfo).port}/datasets/polygon-mainnet`)
  } finally {
    server.close()
  }
}

const header = (number: number) => ({ number, hash: `0x${number}`, parentHash: `0x${number - 1}`, timestamp: number })
const emptyPage = (to: number): Page => ({ upper: header(to), logs: [], transactions: [], headers: new Map() })

test('a fork lookup that fails is a 500, and the portal keeps serving', async () => {
  const chain: ChainView = {
    head: async () => ({ number: 100, hash: '0x100' }),
    page: async (_q, _from, to) => emptyPage(to),
    hot: {
      head: () => ({ number: 105, hash: '0x105' }),
      page: (_q, _from, to) => emptyPage(to),
      hashAt: async () => '0xcanonical',
      previousBlocks: async () => {
        throw new Error('the node has no such block')
      },
    },
  }
  await withPortal(chain, async (url) => {
    const forked = await fetch(`${url}/stream`, { method: 'POST', body: JSON.stringify({ ...base, fromBlock: 103, parentBlockHash: '0xother' }) })
    assert.equal(forked.status, 500)
    assert.equal((await fetch(`${url}/head`)).status, 200)
  })
})

test('hot blocks finalized while a request waited are served as finalized ones', async () => {
  const asked: [number, number][] = []
  let finalized = 100
  const chain: ChainView = {
    head: async () => ({ number: finalized, hash: `0x${finalized}` }),
    page: async (_q, from, to) => (asked.push([from, to]), emptyPage(to)),
    hot: {
      head: () => ({ number: 110, hash: '0x110' }),
      // The follower finalizes up to block 104 between reading the head and taking the page.
      page: () => ((finalized = 104), undefined),
      hashAt: async () => undefined,
      previousBlocks: async () => [],
    },
  }
  await withPortal(chain, async (url) => {
    const res = await fetch(`${url}/stream`, { method: 'POST', body: JSON.stringify({ ...base, fromBlock: 102 }) })
    assert.equal(res.status, 200)
    assert.deepEqual(asked, [[102, 104]])
    assert.equal(res.headers.get('x-sqd-finalized-head-number'), '104')
    assert.equal(JSON.parse((await res.text()).trim().split('\n').pop()!).header.number, 104)
  })
})

test('a request body too large is refused', async () => {
  const chain: ChainView = { head: async () => ({ number: 100, hash: '0x100' }), page: async (_q, _from, to) => emptyPage(to) }
  await withPortal(chain, async (url) => {
    const res = await fetch(`${url}/finalized-stream`, { method: 'POST', body: 'x'.repeat(17 * 1024 * 1024) })
    assert.equal(res.status, 400)
  })
})

test('a parent block reorged away while the page was read is answered with a fork, not the page', async () => {
  let checks = 0
  const chain: ChainView = {
    head: async () => ({ number: 100, hash: '0x100' }),
    page: async (_q, _from, to) => emptyPage(to),
    hot: {
      head: () => ({ number: 110, hash: '0x110' }),
      page: (_q, _from, to) => emptyPage(to),
      // The parent is canonical when the request arrives, and replaced by the time the page is read.
      hashAt: async () => (checks++ === 0 ? '0xparent' : '0xother'),
      previousBlocks: async () => [{ number: 101, hash: '0xother' }],
    },
  }
  await withPortal(chain, async (url) => {
    const res = await fetch(`${url}/stream`, { method: 'POST', body: JSON.stringify({ ...base, fromBlock: 102, parentBlockHash: '0xparent' }) })
    assert.equal(res.status, 409)
    assert.deepEqual((await res.json()).previousBlocks, [{ number: 101, hash: '0xother' }])
  })
})
