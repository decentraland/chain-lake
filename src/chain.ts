import { DatasetConfig } from './config'
import { int, NodeBehind, RpcBlock } from './follow/fetch'
import { RpcClient, untilAnswered } from './follow/rpc'

type Log = { warn(fields: object, message: string): void }

/** Refuses an RPC endpoint that serves another chain than the dataset's: what it reads would be receipted as the dataset's. */
export async function checkRpcChain(rpc: RpcClient, config: DatasetConfig, log: Log): Promise<void> {
  const id = await untilAnswered(`${config.dataset}: reading the chain id`, () => rpc.call<string>('eth_chainId', []), log)
  const chain = Number(BigInt(id))
  if (chain !== config.chainId) throw new Error(`${config.dataset}: the RPC serves chain ${chain}, and the dataset is chain ${config.chainId}`)
}

/**
 * Refuses a portal that serves another chain than the dataset's: its finalized block must be the block
 * the dataset's RPC holds at that height, once that RPC is checked to serve the dataset's chain.
 */
export async function checkPortalChain(head: { number: number; hash: string }, rpc: RpcClient, config: DatasetConfig, log: Log): Promise<void> {
  await checkRpcChain(rpc, config, log)
  const block = await untilAnswered(
    `${config.dataset}: reading block ${head.number}`,
    async () => {
      const block = await rpc.call<RpcBlock | null>('eth_getBlockByNumber', ['0x' + head.number.toString(16), false])
      if (!block) throw new NodeBehind(`the node answering has no block ${head.number} yet`)
      return block
    },
    log
  )
  if (int(block.number) !== head.number || block.hash.toLowerCase() !== head.hash.toLowerCase()) {
    throw new Error(`${config.dataset}: the portal's block ${head.number} is ${head.hash}, and the RPC's is ${block.hash}: they serve different chains`)
  }
}
