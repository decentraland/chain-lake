import { randomBytes } from 'crypto'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api'
import { TableSchema } from '@subsquid/file-store-parquet'
import { createLogger, Logger } from '@subsquid/logger'
import { DatasetConfig, Factory, loadConfig, required } from './config'
import { Coverage, COVERAGE_FILE, factoriesOf, readCoverage } from './coverage'
import { configuredContracts, RegisteredContract, Registry, REGISTRY_FILE } from './discover'
import { fetchFollowing } from './follow/fetch'
import { RpcClient, untilAnswered } from './follow/rpc'
import { Chunk, Manifest, MANIFEST_FILE } from './manifest'
import { rpcUrl } from './serve/headers'
import { Conflict, openStore, Store } from './store'
import { columns } from './tables'
import { LakeBlock, writeBlocks } from './write'

const logger = createLogger('lake:absorb')

/** The history of `followed` in [from, to], with the contracts `factories` created there. */
export type FetchHistory = (
  followed: Set<string>,
  factories: Factory[],
  from: number,
  to: number
) => Promise<{ blocks: LakeBlock[]; created: RegisteredContract[] }>

/**
 * The configured contracts and factories whose history the lake does not hold: those it lacks, and the
 * factories the config follows from an earlier block than the lake holds them from.
 */
export function missingContracts(config: DatasetConfig, coverage: Coverage): RegisteredContract[] {
  const covered = new Set(coverage.contracts)
  const earlier = new Set(config.factories.filter((f) => f.fromBlock < (coverage.factories?.[f.address] ?? f.fromBlock)).map((f) => f.address))
  return configuredContracts(config).filter((c) => !covered.has(c.address) || earlier.has(c.address))
}

/**
 * Adds to a complete lake the contracts its config gained afterwards, with their whole history: the
 * new contracts and factories, and every contract those factories created up to the lake's height.
 *
 * Their history is read over RPC, from the first block that can hold it up to the lake's height, and
 * merged into the chunks that cover each block; blocks no chunk covers get chunks of their own, in
 * the gaps. Each changed chunk is written to a new folder as soon as all its blocks are read, so the
 * history is never held in memory whole, and the lake moves to the new folders only when the manifest
 * listing them is written, by compare-and-swap, so readers see the lake before or after, never
 * in between. The registry is written first, as the follower does, and the coverage last: a run that
 * stops halfway is run again, and merging the same history twice changes nothing.
 *
 * A follower still running against the lake (the task a deployment replaces) may extend it meanwhile:
 * the run then reads the new blocks too, merges them, and tries again from the newer manifest. That
 * follower finds the manifest changed at its next commit, and stops.
 */
export async function absorb(options: {
  lakeDest: string
  config: DatasetConfig
  store: Store
  fetch: FetchHistory
  /** Blocks read per fetch. */
  maxRange: number
  /** Fetches run at once. */
  concurrency?: number
  log?: Logger
  maxAttempts?: number
}): Promise<void> {
  const { lakeDest, config, store, fetch, maxRange } = options
  const concurrency = Math.max(1, options.concurrency ?? 1)
  const maxAttempts = options.maxAttempts ?? 5
  const log = options.log ?? logger
  const coverage = await readCoverage(store)
  if (!coverage?.value.complete) throw new Error(`${config.dataset}: only a complete lake takes new contracts`)
  const missing = missingContracts(config, coverage.value)
  if (missing.length === 0) return

  const missingAddresses = new Set(missing.map((c) => c.address))
  const factories = config.factories.filter((f) => missingAddresses.has(f.address))
  const fromBlockOf = (address: string) =>
    factories.find((f) => f.address === address)?.fromBlock ?? config.contracts.find((c) => c.address === address)?.fromBlock ?? config.fromBlock
  const start = Math.min(...missing.map((c) => fromBlockOf(c.address)))
  log.info(`absorbing ${missing.map((c) => c.name).join(', ')} from block ${start}, over RPC`)

  const created = new Map<string, RegisteredContract>()
  // Only the blocks of chunks not yet written: a chunk is written as soon as all its blocks are read.
  const blocks = new Map<number, LakeBlock>()
  let fetchedTo = start - 1
  let absorbed = 0
  const writer = randomBytes(16).toString('hex')
  // The chunks written while reading, by the folder of the chunk each replaces.
  const replaced = new Map<string, Chunk>()
  const written = new Map<string, Chunk>()
  const chunks = await ChunkFiles.open(lakeDest, config.dataset)

  /** A new folder for `chunk`, named by its range and this run. */
  const folder = (chunk: Chunk): Chunk => ({ ...chunk, dir: `${pad(chunk.from)}-${pad(chunk.to)}-${writer}` })

  /**
   * Writes every chunk of `existing` that ends by `upTo` and takes new history, and drops its blocks from
   * memory. The lake does not list the new folders until the manifest is written, at the end.
   */
  async function flush(existing: Chunk[], upTo: number): Promise<void> {
    for (const target of planChunks(existing, [...blocks.keys()])) {
      if (!target.original || target.blocks.length === 0 || target.original.to > upTo) continue
      const chunk = folder(target.chunk)
      await chunks.write(chunk, target.original, target.blocks.map((n) => blocks.get(n)!))
      replaced.set(target.original.dir, chunk)
      for (const n of target.blocks) blocks.delete(n)
    }
  }

  /**
   * Reads the new history of [from, to]: first the contracts the new factories created there, then the
   * logs of all of them. Knowing every contract before reading any logs lets the ranges be read at once.
   * The logs are read a few ranges at a time, in block order, and each chunk of `existing` is written once
   * all of it is read, so memory holds the history of a few chunks, never the whole of it.
   */
  async function read(from: number, to: number, existing: Chunk[]): Promise<void> {
    const ranges: [number, number][] = []
    for (let a = from; a <= to; a += maxRange) ranges.push([a, Math.min(a + maxRange - 1, to)])
    if (factories.length) {
      await inParallel(ranges, concurrency, async ([a, b]) => {
        for (const c of (await fetch(missingAddresses, factories, a, b)).created) {
          if (!missingAddresses.has(c.address) && !created.has(c.address)) created.set(c.address, c)
        }
      })
      log.info(`blocks ${from}-${to}: ${created.size} contracts created`)
    }
    const followed = new Set([...missingAddresses, ...created.keys()])
    let fetches = 0
    for (let i = 0; i < ranges.length; i += concurrency) {
      const group = ranges.slice(i, i + concurrency)
      await inParallel(group, concurrency, async ([a, b]) => {
        const range = await fetch(followed, factories, a, b)
        const unknown = range.created.find((c) => !followed.has(c.address))
        if (unknown) throw new Error(`${config.dataset}: block ${unknown.createdAt?.block} creates ${unknown.address}, which reading the creations did not find`)
        for (const block of range.blocks) {
          if (block.logs.length === 0 || blocks.has(block.header.number)) continue
          blocks.set(block.header.number, block)
          absorbed++
        }
        if (++fetches % 100 === 0) log.info(`read ${fetches} of ${ranges.length} ranges: ${absorbed} blocks with logs`)
      })
      await flush(existing, group[group.length - 1][1])
    }
  }

  try {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const manifest = await store.read<Manifest>(MANIFEST_FILE)
      const registry = await store.read<Registry>(REGISTRY_FILE)
      if (!manifest || !registry) throw new Error(`${config.dataset}: the lake has no manifest or registry`)
      // A follower only adds chunks past its height, so every chunk written while reading is still listed.
      const listed = new Set(manifest.value.chunks.map((c) => c.dir))
      const gone = [...replaced.keys()].find((dir) => !listed.has(dir))
      if (gone) throw new Error(`${config.dataset}: chunk ${gone} left the lake while absorbing; run it again`)
      if (manifest.value.height > fetchedTo) {
        await read(fetchedTo + 1, manifest.value.height, manifest.value.chunks)
        fetchedTo = manifest.value.height
      }

      // What is still in memory: chunks a follower added past the height read first, and the gaps.
      const plan = planChunks(manifest.value.chunks, [...blocks.keys()])
      const next: Chunk[] = []
      for (const target of plan) {
        const done = target.original && replaced.get(target.original.dir)
        if (done || target.blocks.length === 0) {
          next.push(done || target.chunk)
          continue
        }
        // A chunk already written on an earlier attempt, for the same blocks, is not written again.
        const key = `${target.original?.dir ?? 'gap'}:${target.chunk.from}-${target.chunk.to}:${target.blocks.length}`
        let chunk = written.get(key)
        if (!chunk) {
          chunk = folder(target.chunk)
          await chunks.write(chunk, target.original, target.blocks.map((n) => blocks.get(n)!))
          written.set(key, chunk)
        }
        next.push(chunk)
      }

      const known = new Set(registry.value.contracts.map((c) => c.address))
      const children = [...created.values()].sort((x, y) => x.createdAt!.block - y.createdAt!.block || x.createdAt!.logIndex - y.createdAt!.logIndex)
      const added = [...missing, ...children].filter((c) => !known.has(c.address))
      try {
        if (added.length) await store.write<Registry>(REGISTRY_FILE, { ...registry.value, contracts: [...registry.value.contracts, ...added] }, registry.version)
        await store.write<Manifest>(MANIFEST_FILE, { ...manifest.value, chunks: next }, manifest.version)
      } catch (e) {
        if (!(e instanceof Conflict)) throw e
        log.info(`the lake moved while absorbing (attempt ${attempt + 1}); reading the new blocks too`)
        continue
      }
      await recordCoverage(store, config, factories, missingAddresses, maxAttempts)
      log.info(`absorbed ${missing.length} contracts and ${created.size} they created: ${absorbed} blocks into ${replaced.size + written.size} chunks`)
      return
    }
    throw new Error(`${config.dataset}: the lake kept moving while absorbing; run it again`)
  } finally {
    chunks.close()
  }
}

/**
 * Records that the lake holds the absorbed contracts, and each factory from the block it was read from.
 * Another process may record coverage meanwhile (a second task absorbing the same contracts): the
 * record is then read again and merged, as both hold the same history.
 */
async function recordCoverage(store: Store, config: DatasetConfig, absorbed: Factory[], addresses: Set<string>, maxAttempts: number): Promise<void> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const current = (await readCoverage(store))!
    const contracts = [...new Set([...current.value.contracts, ...addresses])].sort()
    const factories = { ...factoriesOf(config), ...current.value.factories }
    for (const f of absorbed) factories[f.address] = f.fromBlock
    try {
      await store.write<Coverage>(COVERAGE_FILE, { ...current.value, contracts, factories }, current.version)
      return
    } catch (e) {
      if (!(e instanceof Conflict)) throw e
    }
  }
  throw new Error(`${config.dataset}: the coverage kept changing while recording it; run it again`)
}

/** Runs `f` on every item, `concurrency` at a time. */
async function inParallel<T>(items: T[], concurrency: number, f: (item: T) => Promise<void>): Promise<void> {
  let next = 0
  const worker = async () => {
    while (next < items.length) await f(items[next++])
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker))
}

/** A chunk of the new manifest, and the blocks of new history it takes, if any. */
interface Target {
  chunk: Chunk
  original?: Chunk
  blocks: number[]
}

/**
 * Where each block of new history goes: into the chunk that covers it, or, in a gap between chunks, into
 * a new chunk spanning exactly the gap's new blocks. Chunks stay in block order and never overlap. The
 * history goes into the chunks it falls in whatever their size: absorbing is meant for contracts with
 * little history next to what the lake holds.
 */
export function planChunks(existing: Chunk[], numbers: number[]): Target[] {
  const sorted = [...existing].sort((a, b) => a.from - b.from)
  const pending = [...numbers].sort((a, b) => a - b)
  const result: Target[] = []
  let i = 0
  let gap: number[] = []
  const closeGap = () => {
    if (gap.length) result.push({ chunk: { dir: '', from: gap[0], to: gap[gap.length - 1] }, blocks: gap })
    gap = []
  }
  for (const chunk of sorted) {
    while (i < pending.length && pending[i] < chunk.from) gap.push(pending[i++])
    closeGap()
    const blocks: number[] = []
    while (i < pending.length && pending[i] <= chunk.to) blocks.push(pending[i++])
    result.push({ chunk, original: chunk, blocks })
  }
  while (i < pending.length) gap.push(pending[i++])
  closeGap()
  return result
}

const pad = (n: number) => String(n).padStart(10, '0')

/** DuckDB's type for each Parquet primitive type the lake's columns use. */
const DUCKDB_TYPES: Record<number, string> = { 1: 'INTEGER', 2: 'BIGINT', 6: 'VARCHAR' }

/** A table's columns as `read_json` takes them, from the schema the follower writes with. */
function jsonColumns(schema: TableSchema): string {
  const entries = Object.entries(schema).map(([name, column]) => {
    const type = column.type.isNested ? undefined : DUCKDB_TYPES[column.type.primitiveType]
    if (!type) throw new Error(`column ${name} has a type absorb cannot stage`)
    return `"${name}": '${type}'`
  })
  return `{${entries.join(', ')}}`
}

/** The chunk files of a dataset, read and written with DuckDB, in a local directory or on S3. */
class ChunkFiles {
  private constructor(
    private readonly instance: DuckDBInstance,
    private readonly db: DuckDBConnection,
    private readonly base: string,
    private readonly temp: string
  ) {}

  static async open(lakeDest: string, dataset: string): Promise<ChunkFiles> {
    const instance = await DuckDBInstance.create(':memory:', { memory_limit: process.env.DUCKDB_MEMORY_LIMIT || '1GB' })
    const db = await instance.connect()
    const base = `${lakeDest.replace(/\/$/, '')}/${dataset}/chunks`
    if (base.startsWith('s3://')) {
      await db.run('INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;')
      const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION
      await db.run(`CREATE SECRET lake (TYPE s3, PROVIDER credential_chain, REFRESH auto${region ? `, REGION '${region}'` : ''})`)
    }
    return new ChunkFiles(instance, db, base, mkdtempSync(join(tmpdir(), 'chain-lake-absorb-')))
  }

  /** Writes `chunk`: the rows of `original`, if any, and those of `blocks`, without repeats, in chain order. */
  async write(chunk: Chunk, original: Chunk | undefined, blocks: LakeBlock[]): Promise<void> {
    const rows = toRows(blocks)
    const staged = (name: string, lines: object[]) => {
      const file = join(this.temp, `${name}.json`)
      writeFileSync(file, lines.map((l) => JSON.stringify(l)).join('\n') + '\n')
      return file
    }
    const files = {
      blocks: staged('blocks', rows.blocks),
      logs: staged('logs', rows.logs),
      transactions: staged('transactions', rows.transactions),
    }
    const dir = `${this.base}/${chunk.dir}`
    if (!dir.startsWith('s3://')) mkdirSync(dir, { recursive: true })
    // By name, not by position, and in the follower's column order.
    const old = (table: string) => (original ? `SELECT * FROM read_parquet('${this.base}/${original.dir}/${table}.parquet') UNION ALL BY NAME ` : '')
    const copy = async (table: keyof typeof columns, key: string, file: string) => {
      const schema = columns[table]
      const names = Object.keys(schema).map((n) => `"${n}"`).join(', ')
      const json = `SELECT * FROM read_json('${file}', format = 'newline_delimited', columns = ${jsonColumns(schema)})`
      await this.db.run(
        `COPY (SELECT ${names} FROM (${old(table)}${json}) QUALIFY row_number() OVER (PARTITION BY ${key}) = 1 ORDER BY ${key})
         TO '${dir}/${table}.parquet' (FORMAT PARQUET, COMPRESSION GZIP)`
      )
    }
    await copy('blocks', 'number', files.blocks)
    await copy('logs', 'block_number, log_index', files.logs)
    await copy('transactions', 'block_number, transaction_index', files.transactions)
  }

  close(): void {
    this.db.closeSync()
    this.instance.closeSync()
    rmSync(this.temp, { recursive: true, force: true })
  }
}

/** The rows of the lake's tables for these blocks, written by `writeBlocks`, as the follower writes them. */
function toRows(blocks: LakeBlock[]) {
  const rows = { blocks: [] as object[], logs: [] as object[], transactions: [] as object[] }
  const collect = (into: object[]) => ({ write: (row: Record<string, unknown>) => into.push(row) })
  writeBlocks({ blocks: collect(rows.blocks), logs: collect(rows.logs), transactions: collect(rows.transactions) }, blocks)
  return rows
}

async function main() {
  const config = loadConfig()
  const lakeDest = required('LAKE_DEST')
  const store = openStore(lakeDest, config.dataset)
  const rpc = new RpcClient(rpcUrl(config.dataset))
  const perCall = Number(process.env.ADDRESSES_PER_CALL || 500)
  await absorb({
    lakeDest,
    config,
    store,
    maxRange: Number(process.env.ABSORB_MAX_RANGE || 10_000),
    concurrency: Number(process.env.ABSORB_CONCURRENCY || 4),
    // RPC trouble is waited out however long it lasts, as the follower does: giving up would start the
    // whole absorb over.
    fetch: (followed, factories, from, to) =>
      untilAnswered(`${config.dataset}: reading blocks ${from}-${to}`, () => fetchFollowing(rpc, followed, factories, perCall, from, to), logger),
  })
}

if (require.main === module) {
  main().catch((e) => {
    logger.fatal(e)
    process.exit(1)
  })
}
