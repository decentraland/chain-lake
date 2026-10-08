import { headerFromRpc } from '../serve/headers'
import { LakeDataset, Page } from '../serve/lake'
import { EvmQuery } from '../serve/query'
import { ChainView, HotView } from '../serve/server'
import { HotChain } from './hot'
import { pageOf, Tail } from './tail'

/** How far below the finalized head a client's parent block is still checked for a fork. */
const FORK_CHECK_DEPTH = 5000

/**
 * The lake's files plus the finalized blocks the follower processed but has not written yet. A
 * page never spans both: one that starts in the files ends where they end, and the client asks
 * again from there.
 */
export class LiveView implements ChainView {
  readonly hot?: HotView

  constructor(private readonly lake: LakeDataset, private readonly tail: Tail, chain?: HotChain) {
    if (chain) {
      this.hot = {
        head: () => chain.head(),
        page: (query, from, to) => chain.page(query, from, to),
        previousBlocks: (number) => chain.previousBlocks(number),
        hashAt: async (number) => {
          const known = chain.hashOf(number) ?? this.tail.headerOf(number)?.hash
          if (known !== undefined) return known
          // A recently finalized block the client may have seen while it was hot.
          if (number <= this.tail.processed.height && number > this.tail.processed.height - FORK_CHECK_DEPTH) {
            return (await headerFromRpc(this.lake.dataset, number)).hash
          }
          return undefined
        },
      }
    }
  }

  async head() {
    return { number: this.tail.processed.height, hash: this.tail.processed.hash }
  }

  async page(query: EvmQuery, from: number, to: number): Promise<Page> {
    const written = this.tail.lake.height
    if (to <= written) return this.lake.page(query, from, to)
    if (from <= written) return this.lake.page(query, from, written)
    // Take the blocks before any wait: a chunk written meanwhile takes them out of the tail.
    const blocks = this.tail.between(from, to)
    const upper = this.tail.headerOf(to) ?? (await headerFromRpc(this.lake.dataset, to))
    return pageOf(blocks, query, from, to, upper)
  }
}
