/**
 * Compares what two portals serve for one query, block by block, as a squid receives the blocks.
 */

/** A block as a portal client yields it; only the parts this portal can serve are compared. */
export interface StreamedBlock {
  header: { number: number } & Record<string, unknown>
  logs: Record<string, unknown>[]
  transactions: Record<string, unknown>[]
}

export interface Difference {
  block: number
  /** A block only one side holds items in, or the part of a block both hold that differs. */
  what: 'only-left' | 'only-right' | 'header' | 'logs' | 'transactions'
  left?: unknown
  right?: unknown
}

export interface Side {
  blocks: number
  logs: number
  transactions: number
}

export interface Tally {
  left: Side
  right: Side
  /** The last block either side yielded. */
  lastBlock?: number
  differences: number
  /** The first differences, up to the number asked for. */
  samples: Difference[]
}

/**
 * A value in one canonical form: keys sorted, and numbers and strings tagged with their kind, so a
 * number and a bigint compare equal (a bigint cannot be written as JSON) while a number served as a
 * string does not. Strings stay exactly as served, hex case included: a squid compares them as they
 * come, so a portal that changed their case would change what it sees.
 */
export function canonical(value: unknown): unknown {
  if (typeof value === 'bigint' || typeof value === 'number') return `n:${value}`
  if (typeof value === 'string') return `s:${value}`
  if (Array.isArray(value)) return value.map(canonical)
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const key of Object.keys(value).sort()) {
      const v = (value as Record<string, unknown>)[key]
      if (v !== undefined && typeof v !== 'function') out[key] = canonical(v)
    }
    return out
  }
  return value
}

const text = (value: unknown) => JSON.stringify(canonical(value))

/**
 * The first item that differs, or a count when only the number of items does. Items are compared in
 * the order they were served, which is the order a squid processes them in.
 */
function itemDifference(block: number, what: 'logs' | 'transactions', l: Record<string, unknown>[], r: Record<string, unknown>[]): Difference | undefined {
  for (let i = 0; i < Math.max(l.length, r.length); i++) {
    if (i >= l.length || i >= r.length || text(l[i]) !== text(r[i])) {
      return { block, what, left: l[i] ?? `${l.length} ${what}`, right: r[i] ?? `${r.length} ${what}` }
    }
  }
  return undefined
}

/** How two copies of one block differ: in the header, the logs, the transactions, or not at all. */
export function compareBlock(left: StreamedBlock, right: StreamedBlock): Difference[] {
  const block = left.header.number
  const differences: Difference[] = []
  if (text(left.header) !== text(right.header)) differences.push({ block, what: 'header', left: left.header, right: right.header })
  const logs = itemDifference(block, 'logs', left.logs, right.logs)
  if (logs) differences.push(logs)
  const transactions = itemDifference(block, 'transactions', left.transactions, right.transactions)
  if (transactions) differences.push(transactions)
  return differences
}

/** A block one side alone holds, as a sample: its header, its item counts and its first log. */
function summary(block: StreamedBlock) {
  return { header: block.header, logs: block.logs.length, transactions: block.transactions.length, firstLog: block.logs[0] }
}

/** The blocks of a stream of batches, in order. */
export async function* blocksOf<B>(batches: AsyncIterable<{ blocks: B[] }>): AsyncGenerator<B> {
  for await (const batch of batches) for (const block of batch.blocks) yield block
}

/**
 * The blocks holding items, which are what a squid processes. A portal also yields the last block
 * of each response, empty, so its client knows where to continue; two portals end their responses
 * at different blocks, so those are not compared.
 */
async function* withItems(blocks: AsyncIterable<StreamedBlock>, side: string): AsyncGenerator<StreamedBlock> {
  let previous = -1
  for await (const block of blocks) {
    if (block.header.number <= previous) throw new Error(`the ${side} portal yielded block ${block.header.number} after block ${previous}`)
    previous = block.header.number
    if (block.logs.length || block.transactions.length) yield block
  }
}

/**
 * Reads both streams to their end, comparing each block one side holds items in with the same block
 * on the other side. `progress` is called after every block.
 */
export async function compareStreams(
  left: AsyncIterable<StreamedBlock>,
  right: AsyncIterable<StreamedBlock>,
  options: { maxSamples?: number; progress?: (tally: Tally) => void } = {}
): Promise<Tally> {
  const maxSamples = options.maxSamples ?? 20
  const tally: Tally = { left: { blocks: 0, logs: 0, transactions: 0 }, right: { blocks: 0, logs: 0, transactions: 0 }, differences: 0, samples: [] }
  const count = (side: Side, block: StreamedBlock) => {
    side.blocks++
    side.logs += block.logs.length
    side.transactions += block.transactions.length
    tally.lastBlock = Math.max(tally.lastBlock ?? 0, block.header.number)
  }
  const record = (differences: Difference[]) => {
    tally.differences += differences.length
    for (const d of differences) if (tally.samples.length < maxSamples) tally.samples.push(d)
  }

  const l = withItems(left, 'left')
  const r = withItems(right, 'right')
  let [a, b] = await Promise.all([l.next(), r.next()])
  while (!a.done || !b.done) {
    if (!a.done && (b.done || a.value.header.number < b.value.header.number)) {
      count(tally.left, a.value)
      record([{ block: a.value.header.number, what: 'only-left', left: summary(a.value) }])
      a = await l.next()
    } else if (!b.done && (a.done || b.value.header.number < a.value.header.number)) {
      count(tally.right, b.value)
      record([{ block: b.value.header.number, what: 'only-right', right: summary(b.value) }])
      b = await r.next()
    } else if (!a.done && !b.done) {
      count(tally.left, a.value)
      count(tally.right, b.value)
      record(compareBlock(a.value, b.value))
      ;[a, b] = await Promise.all([l.next(), r.next()])
    }
    options.progress?.(tally)
  }
  return tally
}
