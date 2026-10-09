import { createServer, IncomingMessage, ServerResponse } from 'http'
import { createLogger } from '@subsquid/logger'
import { BlockHeader } from './headers'
import { LogRow, Page, TransactionRow } from './lake'
import { BadQuery, EvmQuery, parseQuery } from './query'

const logger = createLogger('lake:portal')

export interface BlockRef {
  number: number
  hash: string
}

/** What a portal serves for one dataset: its head, and pages of blocks below it. */
export interface ChainView {
  /** The highest finalized block this view can serve. */
  head(): Promise<BlockRef>
  /** What the query asks for in [from, to], `to` at most the head; it may end before `to`. */
  page(query: EvmQuery, from: number, to: number): Promise<Page>
  /** Blocks above the finalized head, for `stream`; absent when only finalized blocks are served. */
  hot?: HotView
}

export interface HotView {
  head(): BlockRef
  /**
   * What the query asks for in [from, to], both above the finalized head; undefined when `from`
   * was finalized since the head was read.
   */
  page(query: EvmQuery, from: number, to: number): Page | undefined
  /** The canonical hash at `number`, when it can be told (recent blocks only). */
  hashAt(number: number): Promise<string | undefined>
  /** Canonical blocks at and below `number`, for a client to find where it forked. */
  previousBlocks(number: number): Promise<BlockRef[]>
}

/**
 * A portal that speaks the SQD portal protocol, so a squid switches to it by changing its portal
 * URL. `finalized-stream` serves finalized blocks; `stream` adds the hot blocks above them when the
 * view has them, and answers 409 with the canonical blocks when a client's parent block was
 * reorged away, as the SQD portal does.
 */
/** A dataset the portal will serve but cannot yet: answered with a 503, which squids retry. */
export class NotReady extends Error {}

type Views = (dataset: string) => Promise<ChainView | undefined> | undefined

/** Whether the portal serves what it should, and the state of each dataset. */
export interface Health {
  ready: boolean
  datasets: Record<string, string>
}

/** `health`, when given, is served at `GET /health`: 200 when ready, 503 otherwise, with each dataset's state. */
export function createPortal(view: Views, health?: () => Health) {
  return createServer(async (req, res) => {
    try {
      if (health && req.url === '/health') {
        const { ready, datasets } = health()
        return send(res, ready ? 200 : 503, datasets)
      }
      await answer(view, req, res)
    } catch (e) {
      if (res.headersSent) {
        // Part of the answer is out: cut the connection, so the client sees a failed request and retries.
        logger.error({ err: e }, 'request failed while answering')
        return res.destroy()
      }
      if (e instanceof NotReady) return send(res, 503, { error: e.message })
      if (e instanceof BadQuery) return send(res, 400, { error: e.message })
      if (e instanceof TooLarge) return send(res, 413, { error: e.message }, { connection: 'close' })
      // RPC or storage trouble, most likely passing: a 503, which squids retry, where a 500 stops them.
      logger.error({ err: e }, 'request failed')
      send(res, 503, { error: 'temporarily unavailable' })
    }
  })
}

async function answer(view: Views, req: IncomingMessage, res: ServerResponse) {
  const match = req.url?.match(/^\/datasets\/([^/]+)\/(stream|finalized-stream|head|finalized-head)\/?$/)
  if (!match) return send(res, 404, { error: 'not found' })
  const [, name, endpoint] = match
  const chain = /^[a-z0-9-]+$/.test(name) ? await view(name) : undefined
  if (!chain) return send(res, 404, { error: `unknown dataset ${name}` })

  if (endpoint === 'head' || endpoint === 'finalized-head') {
    if (req.method !== 'GET') return send(res, 405, { error: 'use GET' })
    const finalized = await chain.head()
    const head = endpoint === 'head' ? latestOf(chain, finalized) : finalized
    return send(res, 200, { number: head.number, hash: head.hash })
  }
  if (req.method !== 'POST') return send(res, 405, { error: 'use POST' })

  const text = await readBody(req)
  let body: unknown
  try {
    body = JSON.parse(text)
  } catch {
    throw new BadQuery('the request body is not JSON')
  }
  const query = parseQuery(body)
  const hot = endpoint === 'stream' ? chain.hot : undefined

  // The client's parent block must be canonical, both before the page is read and once it is: the
  // chain may reorganize meanwhile, and a page must never continue a branch that lost. Checked on
  // `finalized-stream` too, against the finalized blocks.
  const forks = chain.hot
  const forked = async () => {
    if (!forks || !query.parentBlockHash || query.fromBlock === 0) return false
    const canonical = await forks.hashAt(query.fromBlock - 1)
    return canonical !== undefined && canonical !== query.parentBlockHash
  }
  const answerFork = async () => {
    const previousBlocks = await forks!.previousBlocks(query.fromBlock - 1)
    const finalized = await chain.head()
    return send(res, 409, { previousBlocks }, headHeaders(finalized, latestOf(chain, finalized)))
  }
  if (await forked()) return answerFork()

  // The heads are read after every wait above: the follower keeps moving meanwhile.
  let finalized = await chain.head()
  const latest = latestOf(chain, finalized)
  const to = Math.min(query.toBlock ?? Infinity, (hot ? latest : finalized).number)
  if (query.fromBlock > to) {
    res.writeHead(204, headHeaders(finalized, latest))
    return res.end()
  }

  let page = hot && query.fromBlock > finalized.number ? hot.page(query, query.fromBlock, to) : undefined
  if (!page) {
    // Finalized blocks, including hot ones finalized since the head was read. When the hot blocks
    // were dropped instead (a reorg the next poll rebuilds), there may be nothing to serve yet.
    finalized = await chain.head()
    const end = Math.min(to, finalized.number)
    if (query.fromBlock > end) {
      res.writeHead(204, headHeaders(finalized, latestOf(chain, finalized)))
      return res.end()
    }
    page = await chain.page(query, query.fromBlock, end)
  }
  if (await forked()) return answerFork()
  res.writeHead(200, { 'content-type': 'application/x-ndjson', ...headHeaders(finalized, latest) })
  for (const line of blockLines(query, page.upper, page.headers, page.logs, page.transactions)) res.write(line + '\n')
  res.end()
}

function latestOf(chain: ChainView, finalized: BlockRef): BlockRef {
  const hot = chain.hot?.head()
  return hot && hot.number > finalized.number ? hot : finalized
}

function headHeaders(finalized: BlockRef, latest: BlockRef) {
  return {
    'x-sqd-finalized-head-number': String(finalized.number),
    'x-sqd-finalized-head-hash': finalized.hash,
    'x-sqd-head-number': String(Math.max(latest.number, finalized.number)),
  }
}

function send(res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}) {
  res.writeHead(status, { 'content-type': 'application/json', ...headers })
  res.end(JSON.stringify(body))
}

/** Far larger than any query the squids send, which are a few KB. */
const MAX_BODY = 1024 * 1024
/** Past this, a body is not even read to its end: the connection is cut. */
const MAX_DRAINED = 16 * MAX_BODY

class TooLarge extends Error {}

/**
 * The request body, up to MAX_BODY bytes. A larger one is no longer kept: it is read to its end, so
 * the client can receive the 413, unless it goes on past MAX_DRAINED, which cuts the connection.
 */
function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    let size = 0
    req.on('data', (chunk: Buffer) => {
      size += chunk.length
      if (size <= MAX_BODY) chunks.push(chunk)
      else if (size > MAX_DRAINED) req.destroy()
    })
    req.on('end', () => (size > MAX_BODY ? reject(new TooLarge(`the request body is larger than ${MAX_BODY} bytes`)) : resolve(Buffer.concat(chunks).toString('utf8'))))
    req.on('error', reject)
  })
}

function pick(fields: Set<string>, always: Record<string, unknown>, optional: Record<string, unknown>) {
  const out: Record<string, unknown> = { ...always }
  for (const [k, v] of Object.entries(optional)) if (fields.has(k)) out[k] = v
  return out
}

function headerJson(query: EvmQuery, h: BlockHeader) {
  return pick(query.fields.block, { number: h.number, hash: h.hash }, { parentHash: h.parentHash, timestamp: h.timestamp })
}

function logJson(query: EvmQuery, l: LogRow) {
  return pick(
    query.fields.log,
    { logIndex: l.log_index, transactionIndex: l.transaction_index },
    {
      address: l.address,
      topics: [l.topic0, l.topic1, l.topic2, l.topic3].filter((t): t is string => t !== null),
      data: l.data,
      transactionHash: l.transaction_hash,
    }
  )
}

function transactionJson(query: EvmQuery, t: TransactionRow) {
  return pick(query.fields.transaction, { transactionIndex: t.transaction_index }, { hash: t.hash, from: t.from, to: t.to, input: t.input })
}

/** One JSON line per block: the blocks with matching items, and always the last block covered. */
export function* blockLines(
  query: EvmQuery,
  upper: BlockHeader,
  headers: Map<number, BlockHeader>,
  logs: LogRow[],
  transactions: TransactionRow[]
): Generator<string> {
  const byBlock = new Map<number, { logs: LogRow[]; transactions: TransactionRow[] }>()
  const entry = (n: number) => byBlock.get(n) ?? byBlock.set(n, { logs: [], transactions: [] }).get(n)!
  for (const l of logs) entry(l.block_number).logs.push(l)
  for (const t of transactions) entry(t.block_number).transactions.push(t)
  entry(upper.number)
  headers.set(upper.number, upper)

  for (const number of [...byBlock.keys()].sort((a, b) => a - b)) {
    const items = byBlock.get(number)!
    const header = headers.get(number)
    if (!header) throw new Error(`the lake has logs but no header for block ${number}`)
    // Same key order as the SQD portal: header, transactions, logs.
    const block: Record<string, unknown> = { header: headerJson(query, header) }
    if (items.transactions.length) block.transactions = items.transactions.map((t) => transactionJson(query, t))
    if (items.logs.length) block.logs = items.logs.map((l) => logJson(query, l))
    yield JSON.stringify(block)
  }
}
