import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api'
import { Chunk, Manifest, MANIFEST_FILE } from '../manifest'
import { openStore, Store } from '../store'
import { BlockHeader, headerFromRpc } from './headers'
import { EvmQuery, logCondition } from './query'

/** Logs per response; a response stops at a block boundary once it has this many. */
export const MAX_LOGS = 20_000

export interface LogRow {
  block_number: number
  log_index: number
  transaction_index: number
  transaction_hash: string
  address: string
  topic0: string | null
  topic1: string | null
  topic2: string | null
  topic3: string | null
  data: string
  with_transaction: boolean
}

export interface TransactionRow {
  block_number: number
  transaction_index: number
  hash: string
  from: string
  to: string | null
  input: string
}

export interface Page {
  /** The last block this page covers; the client continues from the next one. */
  upper: BlockHeader
  logs: LogRow[]
  transactions: TransactionRow[]
  headers: Map<number, BlockHeader>
}

function num(value: unknown): number {
  return typeof value === 'bigint' ? Number(value) : (value as number)
}

let instance: Promise<DuckDBInstance> | undefined
let s3: Promise<void> | undefined

/**
 * One DuckDB for every dataset a process serves, so they share one memory budget. DuckDB would
 * otherwise take most of the machine's memory for each instance.
 */
function duckdb(): Promise<DuckDBInstance> {
  instance ??= DuckDBInstance.create(':memory:', { memory_limit: process.env.DUCKDB_MEMORY_LIMIT || '1GB' })
  return instance
}

/** Extensions and secrets belong to the instance, so they are set up once. */
async function readFromS3(db: DuckDBConnection): Promise<void> {
  // Credentials come from the usual AWS chain: the task role on ECS.
  await db.run('INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;')
  const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION
  await db.run(`CREATE SECRET lake (TYPE s3, PROVIDER credential_chain${region ? `, REGION '${region}'` : ''})`)
}

/** A chunk folder as the writer names it: `<from>-<to>-<writer>`. */
const CHUNK_DIR = /^\d{10}-\d{10}-[0-9a-f]+$/
/** How long the manifest is trusted before it is read again. */
const MANIFEST_TTL_MS = 5000

/**
 * A dataset of the lake, `<lakeDest>/<dataset>/`, in a local directory or an `s3://` location. It
 * serves exactly the chunks its manifest lists, and each query reads only the ones whose block range
 * it touches.
 */
export class LakeDataset {
  private manifest?: Manifest & { readAt: number }

  private constructor(
    readonly dataset: string,
    private readonly base: string,
    private readonly store: Store,
    private readonly db: DuckDBConnection
  ) {}

  static async open(lakeDest: string, dataset: string): Promise<LakeDataset> {
    const db = await (await duckdb()).connect()
    const base = `${lakeDest.replace(/\/$/, '')}/${dataset}/chunks`
    if (base.startsWith('s3://')) {
      s3 ??= readFromS3(db)
      await s3
    }
    return new LakeDataset(dataset, base, openStore(lakeDest, dataset), db)
  }

  private async rows<T>(sql: string): Promise<T[]> {
    const result = await this.db.runAndReadAll(sql)
    return result.getRowObjects() as unknown as T[]
  }

  /** What the lake holds, from its manifest. */
  private async current(): Promise<Manifest> {
    if (!this.manifest || Date.now() - this.manifest.readAt > MANIFEST_TTL_MS) {
      const read = await this.store.read<Manifest>(MANIFEST_FILE)
      if (!read) throw new Error(`${this.dataset}: the lake has no manifest yet`)
      const bad = read.value.chunks.find((c) => !CHUNK_DIR.test(c.dir))
      if (bad) throw new Error(`${this.dataset}: the manifest lists an invalid chunk folder "${bad.dir}"`)
      // The lake only grows. A read that started before the writer's last commit must not take it back.
      if (!this.manifest || read.value.height >= this.manifest.height) this.manifest = { ...read.value, readAt: Date.now() }
      else this.manifest.readAt = Date.now()
    }
    return this.manifest
  }

  /** The last block the lake holds. */
  async written(): Promise<{ height: number; hash: string }> {
    const { height, hash } = await this.current()
    return { height, hash }
  }

  /**
   * What the writer just committed, from the writer itself. The follower drops those blocks from
   * memory at the same moment, so the new chunk has to be served at once, not after the manifest is
   * read again.
   */
  setWritten(manifest: Manifest): void {
    this.manifest = { ...manifest, readAt: Date.now() }
  }

  async head(): Promise<{ number: number; hash: string }> {
    const { height, hash } = await this.written()
    return { number: height, hash }
  }

  /** The chunks overlapping [from, to]. */
  private async chunksIn(from: number, to: number): Promise<Chunk[]> {
    return (await this.current()).chunks.filter((c) => c.to >= from && c.from <= to)
  }

  private source(table: string, chunks: Chunk[]): string {
    return `read_parquet([${chunks.map((c) => `'${this.base}/${c.dir}/${table}.parquet'`).join(', ')}])`
  }

  async header(number: number): Promise<BlockHeader> {
    const chunks = await this.chunksIn(number, number)
    const [row] = chunks.length
      ? await this.rows<Record<string, unknown>>(
          `SELECT number, hash, parent_hash, timestamp FROM ${this.source('blocks', chunks)} WHERE number = ${number}`
        )
      : []
    if (row) return { number: num(row.number), hash: row.hash as string, parentHash: row.parent_hash as string, timestamp: num(row.timestamp) }
    return headerFromRpc(this.dataset, number)
  }

  /** The logs, transactions and headers of the blocks in [from, to] that the query asks for. */
  page(query: EvmQuery, from: number, to: number): Promise<Page> {
    return this.read(query, from, to, true)
  }

  private async read(query: EvmQuery, from: number, to: number, capped: boolean): Promise<Page> {
    const chunks = await this.chunksIn(from, to)
    if (query.logs.length === 0 || chunks.length === 0) return { upper: await this.header(to), logs: [], transactions: [], headers: new Map() }
    const matches = query.logs.map(logCondition).join(' OR ')
    const withTransaction = query.logs.filter((r) => r.transaction).map(logCondition).join(' OR ') || 'FALSE'
    let logs = (
      await this.rows<Record<string, unknown>>(
        `SELECT *, (${withTransaction}) AS with_transaction FROM ${this.source('logs', chunks)}
         WHERE block_number BETWEEN ${from} AND ${to} AND (${matches})
         ORDER BY block_number, log_index${capped ? ` LIMIT ${MAX_LOGS + 1}` : ''}`
      )
    ).map((r) => ({ ...r, block_number: num(r.block_number), log_index: num(r.log_index), transaction_index: num(r.transaction_index) }) as LogRow)

    let upper = to
    if (capped && logs.length > MAX_LOGS) {
      // Stop before the last block, which may be cut short, unless it is the only one.
      const last = logs[MAX_LOGS].block_number
      const complete = logs.filter((l) => l.block_number < last)
      if (complete.length > 0) {
        logs = complete
        upper = last - 1
      } else {
        // One block holds more than a page: it is served whole, since a block is never split.
        return this.read(query, last, last, false)
      }
    }

    const blocks = [...new Set(logs.map((l) => l.block_number))]
    const headers = new Map<number, BlockHeader>()
    if (blocks.length) {
      for (const row of await this.rows<Record<string, unknown>>(
        `SELECT number, hash, parent_hash, timestamp FROM ${this.source('blocks', chunks)} WHERE number IN (${blocks.join(', ')})`
      )) {
        headers.set(num(row.number), { number: num(row.number), hash: row.hash as string, parentHash: row.parent_hash as string, timestamp: num(row.timestamp) })
      }
    }

    const wanted = new Set(logs.filter((l) => l.with_transaction).map((l) => `${l.block_number}:${l.transaction_index}`))
    let transactions: TransactionRow[] = []
    if (wanted.size) {
      const txBlocks = [...new Set(logs.filter((l) => l.with_transaction).map((l) => l.block_number))]
      transactions = (
        await this.rows<Record<string, unknown>>(
          `SELECT * FROM ${this.source('transactions', chunks)} WHERE block_number IN (${txBlocks.join(', ')}) ORDER BY block_number, transaction_index`
        )
      )
        .map((r) => ({ ...r, block_number: num(r.block_number), transaction_index: num(r.transaction_index) }) as TransactionRow)
        .filter((t) => wanted.has(`${t.block_number}:${t.transaction_index}`))
    }

    return { upper: headers.get(upper) ?? (await this.header(upper)), logs, transactions, headers }
  }
}
