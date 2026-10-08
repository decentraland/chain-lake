/**
 * The block shape the lake writes, whichever source it comes from: the SQD portal (backfill) or
 * RPC (follower). It is the subset of an `@subsquid/evm-stream` block the lake keeps, so portal
 * blocks fit it as they are.
 */
export interface LakeBlock {
  header: {
    number: number
    hash: string
    parentHash: string
    /** Milliseconds since the epoch, as `@subsquid/evm-stream` reports it. */
    timestamp: number
  }
  logs: {
    logIndex: number
    transactionIndex: number
    transactionHash: string
    address: string
    topics: string[]
    data: string
  }[]
  transactions: {
    transactionIndex: number
    hash: string
    from: string
    to?: string | null
    input: string
  }[]
}

export interface LakeStore {
  blocks: { write(row: Record<string, unknown>): unknown }
  logs: { write(row: Record<string, unknown>): unknown }
  transactions: { write(row: Record<string, unknown>): unknown }
}

/** Writes the blocks that carry logs; blocks without any only move the lake's height forward. */
export function writeBlocks(store: LakeStore, blocks: LakeBlock[]): void {
  for (const block of blocks) {
    if (block.logs.length === 0) continue
    store.blocks.write({
      number: block.header.number,
      hash: block.header.hash,
      parent_hash: block.header.parentHash,
      timestamp: Math.floor(block.header.timestamp / 1000),
    })
    for (const log of block.logs) {
      store.logs.write({
        block_number: block.header.number,
        log_index: log.logIndex,
        transaction_index: log.transactionIndex,
        transaction_hash: log.transactionHash,
        address: log.address.toLowerCase(),
        topic0: log.topics[0] ?? null,
        topic1: log.topics[1] ?? null,
        topic2: log.topics[2] ?? null,
        topic3: log.topics[3] ?? null,
        data: log.data,
      })
    }
    for (const tx of block.transactions) {
      store.transactions.write({
        block_number: block.header.number,
        transaction_index: tx.transactionIndex,
        hash: tx.hash,
        from: tx.from.toLowerCase(),
        to: tx.to ? tx.to.toLowerCase() : null,
        input: tx.input,
      })
    }
  }
}
