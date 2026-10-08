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
export function createPortal(view: (dataset: string) => Promise<ChainView | undefined> | undefined) {
  return createServer(async (req, res) => {
    try {
      await answer(view, req, res)
    } catch (e) {
      if (res.headersSent) {
        // Part of the answer is out: cut the connection, so the client sees a failed request and retries.
        logger.error({ err: e }, 'request failed while answering')
        return res.destroy()
      }
      if (e instanceof BadQuery || e instanceof SyntaxError) return send(res, 400, { error: e.message })
      logger.error({ err: e }, 'request failed')
      send(res, 500, { error: 'internal error' })
    }
  })
}

async function answer(view: (dataset: string) => Promise<ChainView | undefined> | undefined, req: IncomingMessage, res: ServerResponse) {
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

  const query = parseQuery(JSON.parse(await readBody(req)))
  const hot = endpoint === 'stream' ? chain.hot : undefined

  // The client's parent block must be canonical, both before the page is read and once it is: the
  // chain may reorganize meanwhile, and a page must never continue a branch that lost.
  const forked = async () => {
    if (!hot || !query.parentBlockHash || query.fromBlock === 0) return false
    const canonical = await hot.hashAt(query.fromBlock - 1)
    return canonical !== undefined && canonical !== query.parentBlockHash
  }
  const answerFork = async () => {
    const previousBlocks = await hot!.previousBlocks(query.fromBlock - 1)
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
    // Finalized blocks, including hot ones finalized since the head was read.
    finalized = await chain.head()
    page = await chain.page(query, query.fromBlock, Math.min(to, finalized.number))
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

/** Larger than any query the squids send: the longest, every collection of a chain, is a few hundred KB. */
const MAX_BODY = 16 * 1024 * 1024

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = ''
    let tooLarge = false
    req.on('data', (chunk) => {
      if (!tooLarge) body += chunk
      if (body.length > MAX_BODY) {
        tooLarge = true
        body = ''
      }
    })
    req.on('end', () => (tooLarge ? reject(new BadQuery(`the request body is larger than ${MAX_BODY} bytes`)) : resolve(body)))
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
