import { test } from 'node:test'
import assert from 'node:assert/strict'
import { blocksOf, canonical, compareBlock, compareStreams, StreamedBlock } from './compare'

const ADDRESS = '0x' + 'aa'.repeat(20)

function block(number: number, logs: Record<string, unknown>[] = [], transactions: Record<string, unknown>[] = []): StreamedBlock {
  return { header: { number, hash: `0x${number.toString(16).padStart(64, '0')}`, timestamp: 1700000000 + number }, logs, transactions }
}

const log = (logIndex: number, data = '0x01') => ({ logIndex, transactionIndex: 0, address: ADDRESS, topics: ['0x' + '01'.repeat(32)], data })

async function* stream(...blocks: StreamedBlock[]): AsyncGenerator<StreamedBlock> {
  for (const b of blocks) yield b
}

test('canonical sorts keys and writes numbers as decimal strings, and keeps strings as served', () => {
  assert.deepEqual(canonical({ b: '0xABcd', a: 7n, c: [12, 'Text'], d: undefined }), { a: '7', b: '0xABcd', c: ['12', 'Text'] })
  assert.equal(JSON.stringify(canonical({ x: 1, y: 2 })), JSON.stringify(canonical({ y: 2n, x: '1' })))
})

test('two copies of a block that differ only in key order and number encoding are the same block', () => {
  const left = block(10, [log(0), log(1)])
  const right = { ...block(10, [log(0), log(1)]), header: { timestamp: BigInt(1700000010), hash: block(10).header.hash, number: 10 } }
  assert.deepEqual(compareBlock(left, right), [])
})

test('hex in another case, or items in another order, is a difference: a squid would see it', () => {
  const hash = (block(10).header.hash as string).replace(/[a-f]/g, (c) => c.toUpperCase())
  assert.deepEqual(compareBlock(block(10), { ...block(10), header: { ...block(10).header, hash } }).map((d) => d.what), ['header'])
  assert.deepEqual(compareBlock(block(10, [log(0), log(1)]), block(10, [log(1), log(0)])).map((d) => d.what), ['logs'])
})

test('a block differs in its header, its logs and its transactions separately', () => {
  const left = block(10, [log(0)], [{ transactionIndex: 0, hash: '0x01' }])
  const right = { ...block(10, [log(0, '0x02')], []), header: { ...block(10).header, timestamp: 1 } }
  assert.deepEqual(
    compareBlock(left, right).map((d) => d.what),
    ['header', 'logs', 'transactions']
  )
})

test('a log missing on one side is reported with the count the other side has', () => {
  const [difference] = compareBlock(block(10, [log(0), log(1)]), block(10, [log(0)]))
  assert.equal(difference.what, 'logs')
  assert.deepEqual(difference.right, '1 logs')
})

test('the empty blocks that end each response are not compared', async () => {
  const tally = await compareStreams(stream(block(5), block(10, [log(0)]), block(99)), stream(block(10, [log(0)]), block(40)))
  assert.equal(tally.differences, 0)
  assert.deepEqual(tally.left, { blocks: 1, logs: 1, transactions: 0 })
  assert.deepEqual(tally.right, { blocks: 1, logs: 1, transactions: 0 })
})

test('a block only one side holds items in is a difference, and the streams stay aligned after it', async () => {
  const tally = await compareStreams(
    stream(block(10, [log(0)]), block(20, [log(0)]), block(30, [log(0)])),
    stream(block(10, [log(0)]), block(25, [log(0)]), block(30, [log(0)]))
  )
  assert.deepEqual(
    tally.samples.map((d) => [d.block, d.what]),
    [
      [20, 'only-left'],
      [25, 'only-right'],
    ]
  )
  assert.equal(tally.lastBlock, 30)
})

test('only the first differences are kept as samples, and all are counted', async () => {
  const left = Array.from({ length: 5 }, (_, i) => block(i + 1, [log(0)]))
  const tally = await compareStreams(stream(...left), stream(), { maxSamples: 2 })
  assert.equal(tally.differences, 5)
  assert.equal(tally.samples.length, 2)
})

test('a portal that goes back to an earlier block fails the comparison', async () => {
  await assert.rejects(compareStreams(stream(block(10, [log(0)]), block(9, [log(0)])), stream()), /yielded block 9 after block 10/)
})

test('the blocks of a stream of batches come out in order', async () => {
  async function* batches() {
    yield { blocks: [1, 2] }
    yield { blocks: [3] }
  }
  const out: number[] = []
  for await (const b of blocksOf(batches())) out.push(b)
  assert.deepEqual(out, [1, 2, 3])
})
