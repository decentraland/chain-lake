/**
 * The subset of the SQD portal's EVM query this portal serves: log requests, optionally with each
 * log's transaction, over the fields the lake stores. Anything else is rejected with an explicit
 * error rather than answered with less data than was asked for.
 */

export interface LogRequest {
  address?: string[]
  topic0?: string[]
  topic1?: string[]
  topic2?: string[]
  topic3?: string[]
  transaction?: boolean
  transactionTraces?: boolean
  transactionLogs?: boolean
  transactionStateDiffs?: boolean
}

export interface EvmQuery {
  type: 'evm'
  fromBlock: number
  toBlock?: number
  parentBlockHash?: string
  includeAllBlocks?: boolean
  fields: { block: Set<string>; log: Set<string>; transaction: Set<string> }
  logs: LogRequest[]
}

/** The fields the lake has, per item. */
export const SERVED_FIELDS = {
  block: ['number', 'hash', 'parentHash', 'timestamp'],
  log: ['address', 'topics', 'data', 'transactionHash', 'logIndex', 'transactionIndex'],
  transaction: ['hash', 'from', 'to', 'input', 'transactionIndex'],
}

export class BadQuery extends Error {}

/**
 * Bounds on one query. The squids' largest asks for a few dozen log requests; the bounds stop a single
 * query from tying up the process, whose event loop every dataset's follower shares.
 */
export const MAX_LOG_REQUESTS = 1000
export const MAX_LIST = 10_000
export const MAX_VALUES = 20_000

const HEX = /^0x[0-9a-f]*$/

function hexList(value: unknown, what: string, length: number): string[] | undefined {
  if (value === undefined) return undefined
  if (!Array.isArray(value)) throw new BadQuery(`${what} must be a list`)
  if (value.length > MAX_LIST) throw new BadQuery(`${what} lists at most ${MAX_LIST} values`)
  return value.map((v) => {
    const s = String(v).toLowerCase()
    if (!HEX.test(s) || s.length !== length) throw new BadQuery(`${what}: ${v} is not valid`)
    return s
  })
}

function fieldSet(value: unknown, kind: keyof typeof SERVED_FIELDS): Set<string> {
  const requested = Object.entries((value ?? {}) as Record<string, unknown>)
    .filter(([, on]) => on === true)
    .map(([name]) => name)
  const missing = requested.filter((f) => !SERVED_FIELDS[kind].includes(f))
  if (missing.length) throw new BadQuery(`${kind} fields not served by this portal: ${missing.join(', ')}`)
  return new Set(requested)
}

const isObject = (v: unknown): v is Record<string, any> => typeof v === 'object' && v !== null && !Array.isArray(v)

export function parseQuery(body: unknown): EvmQuery {
  if (!isObject(body)) throw new BadQuery('the query must be a JSON object')
  const q = body
  if (q.type !== 'evm') throw new BadQuery('only "evm" queries are served')
  for (const unsupported of ['transactions', 'traces', 'stateDiffs']) {
    if (Array.isArray(q[unsupported]) && q[unsupported].length > 0) {
      throw new BadQuery(`${unsupported} requests are not served by this portal`)
    }
  }
  if (q.includeAllBlocks) throw new BadQuery('includeAllBlocks is not served by this portal')
  const fromBlock = q.fromBlock ?? 0
  if (!Number.isSafeInteger(fromBlock) || fromBlock < 0) throw new BadQuery('fromBlock must be a block number')
  if (q.toBlock !== undefined && (!Number.isSafeInteger(q.toBlock) || q.toBlock < 0)) {
    throw new BadQuery('toBlock must be a block number')
  }
  if (q.parentBlockHash !== undefined && (typeof q.parentBlockHash !== 'string' || !HEX.test(q.parentBlockHash) || q.parentBlockHash.length !== 66)) {
    throw new BadQuery('parentBlockHash must be a block hash')
  }
  if (q.logs !== undefined && !Array.isArray(q.logs)) throw new BadQuery('logs must be a list')
  if ((q.logs ?? []).length > MAX_LOG_REQUESTS) throw new BadQuery(`at most ${MAX_LOG_REQUESTS} log requests are served`)
  const logs: LogRequest[] = (q.logs ?? []).map((r: unknown) => {
    if (!isObject(r)) throw new BadQuery('each log request must be an object')
    for (const flag of ['transactionTraces', 'transactionLogs', 'transactionStateDiffs']) {
      if (r[flag]) throw new BadQuery(`log requests with ${flag} are not served by this portal`)
    }
    return {
      address: hexList(r.address, 'address', 42),
      topic0: hexList(r.topic0, 'topic0', 66),
      topic1: hexList(r.topic1, 'topic1', 66),
      topic2: hexList(r.topic2, 'topic2', 66),
      topic3: hexList(r.topic3, 'topic3', 66),
      transaction: r.transaction === true,
    }
  })
  const values = logs.reduce((n, r) => n + [r.address, r.topic0, r.topic1, r.topic2, r.topic3].reduce((m, l) => m + (l?.length ?? 0), 0), 0)
  if (values > MAX_VALUES) throw new BadQuery(`the log requests list at most ${MAX_VALUES} addresses and topics in all`)
  return {
    type: 'evm',
    fromBlock,
    toBlock: q.toBlock,
    parentBlockHash: q.parentBlockHash,
    fields: {
      block: fieldSet(q.fields?.block, 'block'),
      log: fieldSet(q.fields?.log, 'log'),
      transaction: fieldSet(q.fields?.transaction, 'transaction'),
    },
    logs,
  }
}

/** The SQL condition selecting the logs one request matches. Values are validated hex above. */
export function logCondition(r: LogRequest): string {
  const parts: string[] = []
  const inList = (column: string, values?: string[]) => {
    if (values === undefined) return
    parts.push(values.length ? `${column} IN (${values.map((v) => `'${v}'`).join(', ')})` : 'FALSE')
  }
  inList('address', r.address)
  inList('topic0', r.topic0)
  inList('topic1', r.topic1)
  inList('topic2', r.topic2)
  inList('topic3', r.topic3)
  return parts.length ? `(${parts.join(' AND ')})` : 'TRUE'
}
