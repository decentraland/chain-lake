export interface BlockHeader {
  number: number
  hash: string
  parentHash: string
  /** Seconds since the epoch. */
  timestamp: number
}

const DEFAULT_RPC: Record<string, string> = {
  'ethereum-mainnet': 'https://rpc.decentraland.org/mainnet',
  'polygon-mainnet': 'https://rpc.decentraland.org/polygon',
  'ethereum-sepolia': 'https://rpc.decentraland.org/sepolia',
  'polygon-amoy-testnet': 'https://rpc.decentraland.org/amoy',
}

/** `RPC_URL_POLYGON_MAINNET` and the like override the default endpoint of a dataset. */
export function rpcUrl(dataset: string): string {
  const override = process.env[`RPC_URL_${dataset.replace(/-/g, '_').toUpperCase()}`]
  const url = override || DEFAULT_RPC[dataset]
  if (!url) throw new Error(`no RPC endpoint for ${dataset}`)
  return url
}

const cache = new Map<string, BlockHeader>()
const MAX_CACHED = 10_000

/**
 * A block header the lake does not hold (it keeps only blocks with followed logs), from RPC.
 * Headers of finalized blocks never change, so they are cached.
 */
export async function headerFromRpc(dataset: string, number: number): Promise<BlockHeader> {
  const key = `${dataset}:${number}`
  const cached = cache.get(key)
  if (cached) return cached
  const response = await fetch(rpcUrl(dataset), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'eth_getBlockByNumber', params: ['0x' + number.toString(16), false] }),
    signal: AbortSignal.timeout(30_000),
  })
  const body = (await response.json()) as { result?: { number: string; hash: string; parentHash: string; timestamp: string } }
  if (!response.ok || !body.result) throw new Error(`RPC has no block ${number} of ${dataset}`)
  const header = {
    number: parseInt(body.result.number, 16),
    hash: body.result.hash,
    parentHash: body.result.parentHash,
    timestamp: parseInt(body.result.timestamp, 16),
  }
  if (cache.size >= MAX_CACHED) cache.delete(cache.keys().next().value as string)
  cache.set(key, header)
  return header
}
