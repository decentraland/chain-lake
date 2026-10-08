import { createLogger } from '@subsquid/logger'
import { DatasetConfig } from '../config'
import { configuredContracts, Registry } from '../discover'
import { createdContracts, fetchRange, ForkedLog, getHeaders, int, RpcBlock } from '../follow/fetch'
import { RpcClient } from '../follow/rpc'
import { BlockHeader } from '../serve/headers'
import { Page } from '../serve/lake'
import { EvmQuery } from '../serve/query'
import { LakeBlock } from '../write'
import { headerOfBlock, HashAndHeight, pageOf } from './tail'

const logger = createLogger('lake:hot')

export interface BlockRef {
  number: number
  hash: string
}

export interface HotOptions {
  addressesPerCall: number
  /** Hot blocks held at most; beyond this (a follower far behind) hot blocks are not served. */
  maxBlocks: number
}

/**
 * The blocks above the last finalized one the follower processed, up to the chain head. Every
 * block is held, with or without logs, so the chain can be checked link by link: a parentHash that
 * does not match means a reorg, and the hot blocks are rebuilt from the finalized base. Contracts
 * created in hot blocks are followed here but never added to the registry, since a reorg may undo
 * them.
 */
export class HotChain {
  private blocks: LakeBlock[] = []
  private ready = false
  /** Contracts created in hot blocks, by address, with their creating block. */
  private created = new Map<string, number>()

  constructor(
    private readonly rpc: RpcClient,
    private readonly config: DatasetConfig,
    private readonly registry: Registry,
    private readonly base: () => HashAndHeight,
    private readonly options: HotOptions
  ) {}

  /**
   * The newest block known, or the finalized base while hot blocks are not served. Never below the
   * base: the follower may finalize past the newest hot block before the next poll.
   */
  head(): BlockRef {
    const last = this.ready ? this.blocks[this.blocks.length - 1] : undefined
    const b = this.base()
    if (last && last.header.number > b.height) return { number: last.header.number, hash: last.header.hash }
    return { number: b.height, hash: b.hash }
  }

  get size(): number {
    return this.ready ? this.blocks.length : 0
  }

  /**
   * What the query asks for among hot blocks in [from, to], or undefined once `from` is no longer
   * above the base: those blocks were finalized while the request waited, and are served as such.
   */
  page(query: EvmQuery, from: number, to: number): Page | undefined {
    if (from <= this.base().height) return undefined
    const upper = this.blocks.find((b) => b.header.number === to)
    if (!upper) throw new Error(`block ${to} is not a hot block`)
    return pageOf(this.blocks, query, from, to, headerOfBlock(upper))
  }

  /** The hash this chain has at `number`, when it knows it. */
  hashOf(number: number): string | undefined {
    const b = this.base()
    if (number === b.height) return b.hash
    return this.blocks.find((x) => x.header.number === number)?.header.hash
  }

  /** Brings the hot blocks up to the chain head. Safe to call again after a failure. */
  async poll(): Promise<void> {
    const base = this.base()
    this.blocks = this.blocks.filter((b) => b.header.number > base.height)
    // Once finalized, the follower has added them to the registry.
    for (const [address, block] of this.created) if (block <= base.height) this.created.delete(address)
    const latest = int((await this.rpc.call<RpcBlock>('eth_getBlockByNumber', ['latest', false])).number)
    if (latest <= base.height) {
      this.blocks = []
      this.ready = true
      return
    }
    if (latest - base.height > this.options.maxBlocks) {
      // The follower is catching up; hot blocks resume once it is close to the head.
      this.blocks = []
      this.ready = false
      return
    }

    // The kept blocks must still hang from the base; otherwise start over from it.
    const first = this.blocks[0]
    if (first && first.header.parentHash !== base.hash && first.header.number === base.height + 1) this.blocks = []
    const top = this.blocks[this.blocks.length - 1]
    const from = top ? top.header.number + 1 : base.height + 1
    if (from > latest) {
      this.ready = true
      return
    }

    const fresh = await this.fetch(from, latest, top ? { number: top.header.number, hash: top.header.hash } : { number: base.height, hash: base.hash })
    if (fresh) {
      this.blocks.push(...fresh)
      this.ready = true
      return
    }
    // A link broke: a reorg. Rebuild everything above the base.
    logger.warn(`${this.config.dataset}: reorg below block ${from}; rebuilding hot blocks from ${base.height + 1}`)
    this.created.clear()
    const rebuilt = await this.fetch(base.height + 1, latest, { number: base.height, hash: base.hash })
    this.blocks = rebuilt ?? []
    this.ready = rebuilt !== undefined
  }

  /** Every block in [from, to] hanging from `parent`, or undefined if the chain does not link. */
  private async fetch(from: number, to: number, parent: BlockRef): Promise<LakeBlock[] | undefined> {
    const numbers = Array.from({ length: to - from + 1 }, (_, i) => from + i)
    const headers = await getHeaders(this.rpc, numbers)
    let prev = parent
    for (const n of numbers) {
      const h = headers.get(n)!
      if (h.parentHash !== prev.hash) return undefined
      prev = { number: n, hash: h.hash }
    }

    const known = new Set([...configuredContracts(this.config), ...this.registry.contracts].map((c) => c.address))
    for (const c of await createdContracts(this.rpc, this.config.factories, from, to, new Set([...known, ...this.created.keys()]))) {
      this.created.set(c.address, c.createdAt!.block)
    }
    const addresses = [...new Set([...known, ...this.created.keys()])]
    try {
      const blocks = await fetchRange(this.rpc, addresses, this.options.addressesPerCall, from, to, true)
      // The logs must belong to the headers just checked.
      for (const b of blocks) if (b.header.hash !== headers.get(b.header.number)!.hash) return undefined
      return blocks
    } catch (e) {
      if (e instanceof ForkedLog) return undefined
      throw e
    }
  }

  /** The canonical blocks below and at `number`, for a client to find where it forked. */
  async previousBlocks(number: number, count = 50): Promise<BlockRef[]> {
    const numbers = Array.from({ length: count }, (_, i) => number - count + 1 + i).filter((n) => n >= 0)
    const headers = await getHeaders(this.rpc, numbers)
    return numbers.map((n) => ({ number: n, hash: headers.get(n)!.hash }))
  }
}

export function toHeader(b: LakeBlock): BlockHeader {
  return headerOfBlock(b)
}
