import { createLogger } from '@subsquid/logger'
import { DatasetConfig } from '../config'
import { configuredContracts, Registry } from '../discover'
import { LakeBlock } from '../write'
import { fetchFollowing, RpcBlock } from './fetch'
import { RpcClient, RpcTrouble } from './rpc'

const logger = createLogger('lake:follow')

export interface FollowOptions {
  /** Blocks per eth_getLogs; providers cap it (10,000 on ours). */
  maxRange: number
  /** Addresses per eth_getLogs; providers cap it (1,000 on ours). */
  addressesPerCall: number
  /** How often to look for newly finalized blocks once caught up. */
  pollMs: number
  /** Stop after this block instead of following the chain, for bounded runs and comparisons. */
  stopBlock?: number
}

const int = (h: string) => parseInt(h, 16)

/**
 * Finalized blocks over RPC, in the lake's block shape, for every contract the registry follows.
 * New contracts are picked up from the factories' creation events in each range before the
 * range's logs are read, so a contract's logs are complete from the block that created it.
 */
export class RpcSource {
  constructor(
    private readonly rpc: RpcClient,
    private readonly config: DatasetConfig,
    readonly registry: Registry,
    private readonly onRegistryChange: (registry: Registry) => Promise<void>,
    private readonly options: FollowOptions
  ) {}

  async getFinalizedHead(): Promise<{ number: number; hash: string }> {
    return this.retrying('reading the finalized block', async () => {
      const block = await this.rpc.call<RpcBlock>('eth_getBlockByNumber', ['finalized', false])
      return { number: int(block.number), hash: block.hash }
    })
  }

  getHead() {
    return this.getFinalizedHead()
  }

  getStream(req: { from: number; to?: number }) {
    return this.getFinalizedStream(req)
  }

  async *getFinalizedStream(req: { from: number; to?: number }): AsyncIterable<{ blocks: LakeBlock[]; finalizedHead: { number: number; hash: string } }> {
    let next = req.from
    const last = Math.min(req.to ?? Infinity, this.options.stopBlock ?? Infinity)
    for (;;) {
      if (next > last) return
      const head = await this.getFinalizedHead()
      const end = Math.min(last, head.number)
      if (next > end) {
        await new Promise((resolve) => setTimeout(resolve, this.options.pollMs))
        continue
      }
      const upper = Math.min(end, next + this.options.maxRange - 1)
      yield { blocks: await this.retrying(`reading blocks ${next}-${upper}`, () => this.range(next, upper)), finalizedHead: head }
      next = upper + 1
    }
  }

  /**
   * Every followed log in [from, to], their blocks and transactions, and always the header of `to`.
   * The contracts the factories created in the range are followed from their creating block, and
   * saved to the registry before the blocks are handed over to be written, so none is ever followed
   * without being on file. The registry is saved only when it grows: it is large, and a range is read
   * every few seconds at the head. The height on file may then lag, which only means fewer blocks are
   * known to hold no new contract.
   */
  async range(from: number, to: number): Promise<LakeBlock[]> {
    // The configured contracts and factories are always followed, even by an older registry.
    const followed = new Set([...configuredContracts(this.config), ...this.registry.contracts].map((c) => c.address))
    const { blocks, created } = await fetchFollowing(this.rpc, followed, this.config.factories, this.options.addressesPerCall, from, to)
    const known = new Set(this.registry.contracts.map((c) => c.address))
    const added = created.filter((c) => !known.has(c.address))
    if (added.length) {
      await this.onRegistryChange({ ...this.registry, height: Math.max(this.registry.height, to), contracts: [...this.registry.contracts, ...added] })
      this.registry.contracts.push(...added)
      logger.info(`${this.config.dataset}: ${added.length} new contracts in blocks ${from}-${to}`)
    }
    this.registry.height = Math.max(this.registry.height, to)
    return blocks
  }

  /**
   * Runs `f` until it gets past any RPC trouble. That trouble stalls this dataset's follower, which
   * picks up where it was once the node answers again, while the portal keeps serving what it has.
   * Any other error is a bug or a broken setup, and ends the run.
   */
  private async retrying<T>(what: string, f: () => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await f()
      } catch (e) {
        if (!(e instanceof RpcTrouble)) throw e
        const delay = Math.min(60_000, 1000 * 2 ** Math.min(attempt, 6))
        logger.warn({ err: e }, `${this.config.dataset}: ${what} failed (attempt ${attempt}); retrying in ${delay / 1000} s`)
        await new Promise((resolve) => setTimeout(resolve, delay))
      }
    }
  }
}
