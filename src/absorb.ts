import { randomBytes } from 'crypto'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api'
import { createLogger, Logger } from '@subsquid/logger'
import { DatasetConfig, Factory, loadConfig, required } from './config'
import { Coverage, COVERAGE_FILE, readCoverage } from './coverage'
import { configuredContracts, RegisteredContract, Registry, REGISTRY_FILE } from './discover'
import { fetchFollowing } from './follow/fetch'
import { RpcClient, RpcTrouble } from './follow/rpc'
import { Chunk, Manifest, MANIFEST_FILE } from './manifest'
import { rpcUrl } from './serve/headers'
import { Conflict, openStore, Store } from './store'
import { LakeBlock } from './write'

const logger = createLogger('lake:absorb')

/** The history of `followed` in [from, to], with the contracts `factories` created there. */
export type FetchHistory = (
  followed: Set<string>,
  factories: Factory[],
  from: number,
  to: number
) => Promise<{ blocks: LakeBlock[]; created: RegisteredContract[] }>

/** The configured contracts and factories whose history the lake does not hold. */
export function missingContracts(config: DatasetConfig, coverage: Coverage): RegisteredContract[] {
  const covered = new Set(coverage.contracts)
  return configuredContracts(config).filter((c) => !covered.has(c.address))
}

/**
 * Adds to a complete lake the contracts its config gained afterwards, with their whole history: the
 * new contracts and factories, and every contract those factories created up to the lake's height.
 *
 * Their history is read over RPC, from the first block that can hold it up to the lake's height, and
 * merged into the chunks that cover each block; blocks no chunk covers get chunks of their own, in
 * the gaps. Each changed chunk is written to a new folder, and the lake moves to it only when the
 * manifest listing it is written, by compare-and-swap, so readers see the lake before or after, never
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
  log?: Logger
  maxAttempts?: number
}): Promise<void> {
  const { lakeDest, config, store, fetch, maxRange } = options
  const log = options.log ?? logger
  const coverage = await readCoverage(store)
  if (!coverage?.value.complete) throw new Error(`${config.dataset}: only a complete lake takes new contracts`)
  const missing = missingContracts(config, coverage.value)
  if (missing.length === 0) return

  const missingAddresses = new Set(missing.map((c) => c.address))
  const factories = config.factories.filter((f) => missingAddresses.has(f.address))
  const start = Math.min(...missing.map((c) => factories.find((f) => f.address === c.address)?.fromBlock ?? config.fromBlock))
  log.info(`absorbing ${missing.map((c) => c.name).join(', ')} from block ${start}, over RPC`)

  const followed = new Set(missingAddresses)
  const created: RegisteredContract[] = []
  const blocks = new Map<number, LakeBlock>()
  let fetchedTo = start - 1
  const writer = randomBytes(16).toString('hex')
  const written = new Map<string, Chunk>()
  const chunks = await ChunkFiles.open(lakeDest, config.dataset)

  try {
    for (let attempt = 0; attempt < (options.maxAttempts ?? 5); attempt++) {
      const manifest = await store.read<Manifest>(MANIFEST_FILE)
      const registry = await store.read<Registry>(REGISTRY_FILE)
      if (!manifest || !registry) throw new Error(`${config.dataset}: the lake has no manifest or registry`)

      let fetches = 0
      for (let from = fetchedTo + 1; from <= manifest.value.height; from += maxRange) {
        const to = Math.min(from + maxRange - 1, manifest.value.height)
        const range = await fetch(followed, factories, from, to)
        for (const c of range.created) {
          if (followed.has(c.address)) continue
          followed.add(c.address)
          created.push(c)
        }
        for (const block of range.blocks) if (block.logs.length > 0) blocks.set(block.header.number, block)
        if (++fetches % 100 === 0) log.info(`read to block ${to}: ${blocks.size} blocks with logs, ${created.length} contracts created`)
      }
      fetchedTo = Math.max(fetchedTo, manifest.value.height)

      const plan = planChunks(manifest.value.chunks, [...blocks.keys()])
      const next: Chunk[] = []
      for (const target of plan) {
        if (target.blocks.length === 0) {
          next.push(target.chunk)
          continue
        }
        // A chunk already written on an earlier attempt, for the same blocks, is not written again.
        const key = `${target.original?.dir ?? 'gap'}:${target.chunk.from}-${target.chunk.to}:${target.blocks.length}`
        let chunk = written.get(key)
        if (!chunk) {
          chunk = { ...target.chunk, dir: `${pad(target.chunk.from)}-${pad(target.chunk.to)}-${writer}` }
          await chunks.write(chunk, target.original, target.blocks.map((n) => blocks.get(n)!))
          written.set(key, chunk)
        }
        next.push(chunk)
      }

      const known = new Set(registry.value.contracts.map((c) => c.address))
      const added = [...missing, ...created].filter((c) => !known.has(c.address))
      try {
        if (added.length) await store.write<Registry>(REGISTRY_FILE, { ...registry.value, contracts: [...registry.value.contracts, ...added] }, registry.version)
        await store.write<Manifest>(MANIFEST_FILE, { ...manifest.value, chunks: next }, manifest.version)
      } catch (e) {
        if (!(e instanceof Conflict)) throw e
        log.info(`the lake moved while absorbing (attempt ${attempt + 1}); reading the new blocks too`)
        continue
      }
      const contracts = [...new Set([...coverage.value.contracts, ...missingAddresses])].sort()
      await store.write<Coverage>(COVERAGE_FILE, { ...coverage.value, contracts }, coverage.version)
      log.info(`absorbed ${missing.length} contracts and ${created.length} they created: ${blocks.size} blocks into ${written.size} chunks`)
      return
    }
    throw new Error(`${config.dataset}: the lake kept moving while absorbing; run it again`)
  } finally {
    chunks.close()
  }
}

/** A chunk of the new manifest, and the blocks of new history it takes, if any. */
interface Target {
  chunk: Chunk
  original?: Chunk
  blocks: number[]
}

/**
 * Where each block of new history goes: into the chunk that covers it, or, in a gap between chunks, into
 * a new chunk spanning exactly the gap's new blocks. Chunks stay in block order and never overlap.
 */
export function planChunks(existing: Chunk[], numbers: number[]): Target[] {
  const sorted = [...existing].sort((a, b) => a.from - b.from)
  const targets: Target[] = sorted.map((chunk) => ({ chunk, original: chunk, blocks: [] }))
  const gaps = new Map<number, number[]>()
  for (const n of [...numbers].sort((a, b) => a - b)) {
    const index = sorted.findIndex((c) => c.from <= n && n <= c.to)
    if (index >= 0) {
      targets[index].blocks.push(n)
    } else {
      // The gap is named by the first chunk after it, or by none past the last one.
      const after = sorted.findIndex((c) => c.from > n)
      const gap = after < 0 ? sorted.length : after
      gaps.set(gap, [...(gaps.get(gap) ?? []), n])
    }
  }
  const result: Target[] = []
  for (let i = 0; i <= sorted.length; i++) {
    const gap = gaps.get(i)
    if (gap) result.push({ chunk: { dir: '', from: gap[0], to: gap[gap.length - 1] }, blocks: gap })
    if (i < sorted.length) result.push(targets[i])
  }
  return result
}

const pad = (n: number) => String(n).padStart(10, '0')

const BLOCK_COLUMNS = `{number: 'BIGINT', hash: 'VARCHAR', parent_hash: 'VARCHAR', timestamp: 'BIGINT'}`
const LOG_COLUMNS = `{block_number: 'BIGINT', log_index: 'INTEGER', transaction_index: 'INTEGER', transaction_hash: 'VARCHAR', address: 'VARCHAR', topic0: 'VARCHAR', topic1: 'VARCHAR', topic2: 'VARCHAR', topic3: 'VARCHAR', data: 'VARCHAR'}`
const TRANSACTION_COLUMNS = `{block_number: 'BIGINT', transaction_index: 'INTEGER', hash: 'VARCHAR', "from": 'VARCHAR', "to": 'VARCHAR', input: 'VARCHAR'}`

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
    const old = (table: string) => (original ? `SELECT * FROM read_parquet('${this.base}/${original.dir}/${table}.parquet') UNION ALL ` : '')
    const json = (file: string, columns: string) => `SELECT * FROM read_json('${file}', format = 'newline_delimited', columns = ${columns})`
    const copy = (table: string, columns: string, key: string, file: string) =>
      this.db.run(
        `COPY (SELECT * FROM (${old(table)}${json(file, columns)}) QUALIFY row_number() OVER (PARTITION BY ${key}) = 1 ORDER BY ${key})
         TO '${dir}/${table}.parquet' (FORMAT PARQUET, COMPRESSION GZIP)`
      )
    await copy('blocks', BLOCK_COLUMNS, 'number', files.blocks)
    await copy('logs', LOG_COLUMNS, 'block_number, log_index', files.logs)
    await copy('transactions', TRANSACTION_COLUMNS, 'block_number, transaction_index', files.transactions)
  }

  close(): void {
    this.db.closeSync()
    this.instance.closeSync()
    rmSync(this.temp, { recursive: true, force: true })
  }
}

/** The rows of the lake's tables for these blocks, as `writeBlocks` writes them. */
function toRows(blocks: LakeBlock[]) {
  const rows = { blocks: [] as object[], logs: [] as object[], transactions: [] as object[] }
  for (const block of blocks) {
    const number = block.header.number
    rows.blocks.push({ number, hash: block.header.hash, parent_hash: block.header.parentHash, timestamp: Math.floor(block.header.timestamp / 1000) })
    for (const log of block.logs) {
      rows.logs.push({
        block_number: number,
        log_index: log.logIndex,
        transaction_index: log.transactionIndex,
        transaction_hash: log.transactionHash,
        address: log.address.toLowerCase(),
        topic0: log.topics[0] ?? null,
        topic1: log.topics[1] ?? null,
        topic2: log.topics[2] ?? null,
        topic3: log.topics[3] ?? null,
        data: log.data,
      })
    }
    for (const tx of block.transactions) {
      rows.transactions.push({
        block_number: number,
        transaction_index: tx.transactionIndex,
        hash: tx.hash,
        from: tx.from.toLowerCase(),
        to: tx.to ? tx.to.toLowerCase() : null,
        input: tx.input,
      })
    }
  }
  return rows
}

/** Retries what a node in trouble may answer next time: one behind, a reorg, a timeout. */
async function retrying<T>(what: () => Promise<T>, attempts = 8): Promise<T> {
  for (let i = 1; ; i++) {
    try {
      return await what()
    } catch (e) {
      if (!(e instanceof RpcTrouble) || i >= attempts) throw e
      await new Promise((resolve) => setTimeout(resolve, Math.min(30_000, 500 * 2 ** i)))
    }
  }
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
    fetch: (followed, factories, from, to) => retrying(() => fetchFollowing(rpc, followed, factories, perCall, from, to)),
  })
}

if (require.main === module) {
  main().catch((e) => {
    logger.fatal(e)
    process.exit(1)
  })
}
