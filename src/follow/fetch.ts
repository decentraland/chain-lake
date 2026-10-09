import { Factory } from '../config'
import type { RegisteredContract } from '../discover'
import { LakeBlock } from '../write'
import { RpcClient, RpcError, RpcRejected, RpcTrouble } from './rpc'

export interface RpcLog {
  removed?: boolean
  logIndex: string
  transactionIndex: string
  transactionHash: string
  blockHash: string
  blockNumber: string
  address: string
  data: string
  topics: string[]
}

export interface RpcBlock {
  number: string
  hash: string
  parentHash: string
  timestamp: string
}

export interface RpcTransaction {
  hash: string
  from: string
  to: string | null
  input: string
  transactionIndex: string
  blockNumber: string
  blockHash: string | null
}

export const hex = (n: number) => '0x' + n.toString(16)
export const int = (h: string) => parseInt(h, 16)

/** The answers of a provider that mean "ask for a smaller range"; a rate limit is not one of them. */
export function rangeTooWide(e: RpcError): boolean {
  if (/rate limit|too many requests|throttl/i.test(e.message)) return false
  if (tooManyAddresses(e)) return false
  return /range|too many|limit|exceed|response size|query returned more than/i.test(e.message)
}

/** A provider's cap on the addresses of one call: a setting to lower, not a range to split. */
export function tooManyAddresses(e: RpcError): boolean {
  return /address/i.test(e.message) && /too many|limit|exceed|maximum/i.test(e.message)
}

/** A node that has not reached the block a call asks about yet. */
export class NodeBehind extends RpcTrouble {}

/**
 * eth_getLogs over [from, to], halving the range while the provider says it is too wide.
 *
 * The header of `to` is asked first, in the same batch. A node behind `to` may answer eth_getLogs with the
 * logs it has so far and no error, but it answers that header with null, so the range is refused
 * and asked again. This holds when a batch is answered by one node, as it is behind a load balancer.
 */
export async function getLogs(rpc: RpcClient, filter: { address?: string[]; topics?: (string[] | null)[] }, from: number, to: number): Promise<RpcLog[]> {
  // The header first: a node answers a batch in order, so a node that has block `to` when it answers
  // the header still has it when it answers the logs after it.
  const [upper, logs] = await rpc.batch<unknown>([
    { method: 'eth_getBlockByNumber', params: [hex(to), false] },
    { method: 'eth_getLogs', params: [{ ...filter, fromBlock: hex(from), toBlock: hex(to) }] },
  ])
  if (logs instanceof RpcError) {
    if (tooManyAddresses(logs)) throw new RpcRejected(`the node takes fewer addresses per call than ${filter.address?.length}: lower ADDRESSES_PER_CALL (${logs.message})`)
    if (!rangeTooWide(logs) || from === to) throw logs
    const mid = Math.floor((from + to) / 2)
    return [...(await getLogs(rpc, filter, from, mid)), ...(await getLogs(rpc, filter, mid + 1, to))]
  }
  if (!upper || upper instanceof RpcError) throw new NodeBehind(`the node answering has no block ${to} yet`)
  return logs as RpcLog[]
}

/** The logs of `addresses` in [from, to], asked in groups the provider accepts. */
export async function followedLogs(rpc: RpcClient, addresses: string[], perCall: number, from: number, to: number): Promise<RpcLog[]> {
  const logs: RpcLog[] = []
  for (let i = 0; i < addresses.length; i += perCall) {
    logs.push(...(await getLogs(rpc, { address: addresses.slice(i, i + perCall) }, from, to)))
  }
  return logs.filter((l) => !l.removed)
}

/** The address a factory log announces, from the topic the factory puts it in. */
export function createdAddress(topics: string[], addressTopic: number): string {
  const topic = topics[addressTopic]
  if (!topic || !/^0x[0-9a-fA-F]{64}$/.test(topic)) throw new Error(`topic ${addressTopic} is not an address word`)
  return ('0x' + topic.slice(-40)).toLowerCase()
}

interface FactoryLogs {
  header: { number: number }
  logs: { address: string; topics: string[]; logIndex: number; transactionHash: string }[]
}

/**
 * The contracts the factories created in these blocks, in chain order, from the factory logs they
 * hold. Discovery over the SQD portal and the follower over RPC both read creations this way.
 */
export function createdIn(blocks: FactoryLogs[], factories: Factory[]): RegisteredContract[] {
  const created: RegisteredContract[] = []
  const seen = new Set<string>()
  for (const block of blocks) {
    for (const log of block.logs) {
      const factory = factories.find((f) => f.address === log.address.toLowerCase() && f.topic0 === log.topics[0] && f.fromBlock <= block.header.number)
      if (!factory) continue
      const address = createdAddress(log.topics, factory.addressTopic)
      if (seen.has(address)) continue
      seen.add(address)
      created.push({ name: 'collection', address, factory: factory.name, createdAt: { block: block.header.number, logIndex: log.logIndex, transactionHash: log.transactionHash } })
    }
  }
  return created
}

export async function getHeaders(rpc: RpcClient, numbers: number[]): Promise<Map<number, RpcBlock>> {
  const headers = new Map<number, RpcBlock>()
  const answers = await rpc.batch<RpcBlock>(numbers.map((n) => ({ method: 'eth_getBlockByNumber', params: [hex(n), false] })))
  answers.forEach((b, i) => {
    if (b instanceof RpcError) throw b
    if (!b) throw new NodeBehind(`the node answering has no block ${numbers[i]}`)
    headers.set(numbers[i], b)
  })
  return headers
}

export async function getTransactions(rpc: RpcClient, hashes: string[]): Promise<Map<string, RpcTransaction>> {
  const transactions = new Map<string, RpcTransaction>()
  const answers = await rpc.batch<RpcTransaction>(hashes.map((h) => ({ method: 'eth_getTransactionByHash', params: [h] })))
  answers.forEach((t, i) => {
    if (t instanceof RpcError) throw t
    if (!t) throw new NodeBehind(`the node answering has no transaction ${hashes[i]}`)
    transactions.set(hashes[i], t)
  })
  return transactions
}

/** A log or transaction placed elsewhere than its header says: the node answered across a reorg. */
export class ForkedLog extends RpcTrouble {}

/**
 * The lake blocks of `numbers` (each needs a header) with their logs and those logs'
 * transactions. Every log must carry its header's hash.
 */
export function assemble(numbers: number[], headers: Map<number, RpcBlock>, logs: RpcLog[], transactions: Map<string, RpcTransaction>): LakeBlock[] {
  const blocks = new Map<number, LakeBlock>()
  for (const n of numbers) {
    const h = headers.get(n)
    if (!h) throw new RpcTrouble(`no header for block ${n}`)
    blocks.set(n, { header: { number: n, hash: h.hash, parentHash: h.parentHash, timestamp: int(h.timestamp) * 1000 }, logs: [], transactions: [] })
  }
  for (const l of logs) {
    const block = blocks.get(int(l.blockNumber))
    if (!block) throw new RpcTrouble(`log of block ${int(l.blockNumber)}, which was not asked for`)
    if (l.blockHash !== block.header.hash) {
      throw new ForkedLog(`log ${l.transactionHash}:${int(l.logIndex)} has block hash ${l.blockHash}, header has ${block.header.hash}`)
    }
    block.logs.push({ logIndex: int(l.logIndex), transactionIndex: int(l.transactionIndex), transactionHash: l.transactionHash, address: l.address.toLowerCase(), topics: l.topics, data: l.data })
  }
  for (const block of blocks.values()) {
    block.logs.sort((a, b) => a.logIndex - b.logIndex)
    const seen = new Set<string>()
    for (const log of block.logs) {
      if (seen.has(log.transactionHash)) continue
      seen.add(log.transactionHash)
      const t = transactions.get(log.transactionHash)
      if (!t) throw new RpcTrouble(`no transaction ${log.transactionHash}`)
      // The transaction must sit where its log does: a node that answered across a reorg may place it
      // in another block, or at another index, which the portal would then fail to match to the log.
      if (t.hash !== log.transactionHash || t.blockHash !== block.header.hash || int(t.blockNumber) !== block.header.number || int(t.transactionIndex) !== log.transactionIndex) {
        throw new ForkedLog(`transaction ${log.transactionHash} is at ${t.blockHash}:${t.transactionIndex}, its log at ${block.header.hash}:${log.transactionIndex}`)
      }
      block.transactions.push({ transactionIndex: int(t.transactionIndex), hash: t.hash, from: t.from, to: t.to, input: t.input })
    }
    block.transactions.sort((a, b) => a.transactionIndex - b.transactionIndex)
  }
  return [...blocks.values()]
}

/**
 * The blocks of [from, to] for the `followed` addresses and for every contract the factories create
 * within the range. The new contracts are read from the factory logs of the fetched blocks themselves,
 * so they come from the same checked chain as every other log; a fetch that shows a contract it did
 * not follow is repeated with it, so its logs in the creating block are there too.
 */
export async function fetchFollowing(
  rpc: RpcClient,
  followed: Set<string>,
  factories: Factory[],
  perCall: number,
  from: number,
  to: number,
  everyBlock = false
): Promise<{ blocks: LakeBlock[]; created: RegisteredContract[] }> {
  const addresses = new Set(followed)
  for (let attempt = 0; attempt < 5; attempt++) {
    const blocks = await fetchRange(rpc, [...addresses], perCall, from, to, everyBlock)
    const created = createdIn(blocks, factories)
    const missing = created.filter((c) => !addresses.has(c.address))
    if (missing.length === 0) return { blocks, created }
    for (const c of missing) addresses.add(c.address)
  }
  throw new RpcTrouble(`blocks ${from}-${to} kept showing contracts their fetch did not follow`)
}

/** Logs, headers and transactions of [from, to] for `addresses`, with the header of `to` always present. */
export async function fetchRange(rpc: RpcClient, addresses: string[], perCall: number, from: number, to: number, everyBlock = false): Promise<LakeBlock[]> {
  const logs = await followedLogs(rpc, addresses, perCall, from, to)
  const numbers = everyBlock
    ? Array.from({ length: to - from + 1 }, (_, i) => from + i)
    : [...new Set([...logs.map((l) => int(l.blockNumber)), to])].sort((a, b) => a - b)
  const headers = await getHeaders(rpc, numbers)
  const transactions = await getTransactions(rpc, [...new Set(logs.map((l) => l.transactionHash))])
  return assemble(numbers, headers, logs, transactions)
}
