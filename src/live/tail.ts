import { BlockHeader } from '../serve/headers'
import { LogRow, MAX_LOGS, Page, TransactionRow } from '../serve/lake'
import { EvmQuery, LogRequest } from '../serve/query'
import { LakeBlock } from '../write'

export interface HashAndHeight {
  height: number
  hash: string
}

function header(block: LakeBlock): BlockHeader {
  return { number: block.header.number, hash: block.header.hash, parentHash: block.header.parentHash, timestamp: Math.floor(block.header.timestamp / 1000) }
}

/** Each request's lists as sets, built once per request: one lookup per list for each log. */
const compiled = new WeakMap<LogRequest, (Set<string> | undefined)[]>()

function setsOf(r: LogRequest): (Set<string> | undefined)[] {
  let sets = compiled.get(r)
  if (!sets) {
    sets = [r.address, r.topic0, r.topic1, r.topic2, r.topic3].map((list) => list && new Set(list))
    compiled.set(r, sets)
  }
  return sets
}

/** The same match as the SQL `logCondition`, for logs held in memory. */
export function matches(r: LogRequest, log: { address: string; topics: string[] }): boolean {
  const [address, ...topics] = setsOf(r)
  if (address && !address.has(log.address.toLowerCase())) return false
  return topics.every((wanted, i) => !wanted || (log.topics[i] !== undefined && wanted.has(log.topics[i].toLowerCase())))
}

/**
 * Finalized blocks the follower processed that are not in the lake's files yet. file-store writes
 * a chunk only when it fills up or is forced to; until then these blocks exist only here, and the
 * portal serves them from here.
 */
export class Tail {
  private blocks: LakeBlock[] = []
  /** The last block written to the lake's files. */
  lake: HashAndHeight
  /** The last finalized block the follower processed. */
  processed: HashAndHeight

  constructor(lake: HashAndHeight) {
    this.lake = lake
    this.processed = lake
  }

  /** Blocks of a processed batch, in order; the last one is the batch's end, with or without logs. */
  add(blocks: LakeBlock[]): void {
    if (blocks.length === 0) return
    for (const b of blocks) {
      if (b.header.number <= this.processed.height) throw new Error(`block ${b.header.number} is not after ${this.processed.height}`)
      this.blocks.push(b)
    }
    const last = blocks[blocks.length - 1].header
    this.processed = { height: last.number, hash: last.hash }
  }

  /** The lake's files now hold everything up to `state`. */
  written(state: HashAndHeight): void {
    this.lake = state
    this.blocks = this.blocks.filter((b) => b.header.number > state.height)
  }

  get size(): number {
    return this.blocks.length
  }

  headerOf(number: number): BlockHeader | undefined {
    const block = this.blocks.find((b) => b.header.number === number)
    return block && header(block)
  }

  /** The blocks held in [from, to]. They stay valid after a write takes them out of the tail. */
  between(from: number, to: number): LakeBlock[] {
    return this.blocks.filter((b) => b.header.number >= from && b.header.number <= to)
  }

  /** What the query asks for in [from, to], which must lie above the lake's files. */
  page(query: EvmQuery, from: number, to: number, upper: BlockHeader): Page {
    return pageOf(this.blocks, query, from, to, upper)
  }
}

/**
 * What the query asks for in [from, to] among blocks held in memory, in block order. Like a page of
 * the lake, it stops at a block boundary once it has MAX_LOGS logs.
 */
export function pageOf(blocks: LakeBlock[], query: EvmQuery, from: number, to: number, upper: BlockHeader): Page {
  const logs: LogRow[] = []
  const transactions: TransactionRow[] = []
  const headers = new Map<number, BlockHeader>()
  for (const block of blocks) {
    const n = block.header.number
    if (n < from || n > to) continue
    if (logs.length >= MAX_LOGS) {
      // The page is full: it ends at the last block it holds, and the client asks again from there.
      const last = [...headers.keys()].pop()!
      return { upper: headers.get(last)!, logs, transactions, headers }
    }
    const wanted = new Set<number>()
    let any = false
    for (const log of block.logs) {
      const matching = query.logs.filter((r) => matches(r, log))
      if (matching.length === 0) continue
      any = true
      const withTransaction = matching.some((r) => r.transaction)
      if (withTransaction) wanted.add(log.transactionIndex)
      logs.push({
        block_number: n,
        log_index: log.logIndex,
        transaction_index: log.transactionIndex,
        transaction_hash: log.transactionHash,
        address: log.address.toLowerCase(),
        topic0: log.topics[0] ?? null,
        topic1: log.topics[1] ?? null,
        topic2: log.topics[2] ?? null,
        topic3: log.topics[3] ?? null,
        data: log.data,
        with_transaction: withTransaction,
      })
    }
    for (const tx of block.transactions) {
      if (!wanted.has(tx.transactionIndex)) continue
      transactions.push({ block_number: n, transaction_index: tx.transactionIndex, hash: tx.hash, from: tx.from.toLowerCase(), to: tx.to ? tx.to.toLowerCase() : null, input: tx.input })
    }
    if (any) headers.set(n, header(block))
  }
  return { upper, logs, transactions, headers }
}

export { header as headerOfBlock }
