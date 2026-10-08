import { spawn } from 'child_process'
import { join } from 'path'
import { run } from '@subsquid/batch-processor'
import { Database, Dest } from '@subsquid/file-store'
import { createLogger } from '@subsquid/logger'
import { datasetsFromEnv, loadConfig, required } from './config'
import { BACKFILL_FILE, checkCoverage, coverageOf, PARTIAL_FILE } from './coverage'
import { openDest } from './dest'
import { Registry } from './discover'
import { HotChain } from './live/hot'
import { Tail } from './live/tail'
import { LiveView } from './live/view'
import { RpcClient } from './follow/rpc'
import { RpcSource } from './follow/source'
import { rpcUrl } from './serve/headers'
import { LakeDataset } from './serve/lake'
import { createPortal } from './serve/server'
import { tables } from './tables'
import { LakeBlock, writeBlocks } from './write'

const logger = createLogger('lake:live')
const STATUS_FILE = 'status.txt'

/**
 * The datasets named by `DATASETS`, live. Each one has its own follower, which keeps its lake up
 * to date over RPC; one portal serves them all, each from its lake plus what its follower has
 * processed but not written yet. Squids point their portal URL here.
 *
 * RPC trouble stalls only the dataset it hits: its follower retries until the node answers again,
 * and the portal keeps serving what it has. Any other failure ends the process, so the service
 * restarts as a whole.
 */
async function main() {
  const lakeDest = required('LAKE_DEST')
  const datasets = datasetsFromEnv()
  if (datasets.length > 1) {
    for (const name of ['PROMETHEUS_PORT', 'PROCESSOR_PROMETHEUS_PORT']) {
      // Each follower would serve its metrics on that one port, and the second would fail to start.
      if (process.env[name]) logger.warn(`${name} is ignored: it serves the metrics of one dataset, and there are ${datasets.length}`)
      delete process.env[name]
    }
  }
  // A deployment starts this task before it stops the one it replaces, and each dataset must have a
  // single writer: nothing is written until the other task has had time to stop.
  const grace = Number(process.env.WRITER_GRACE_MS ?? 120_000)
  if (grace > 0) {
    logger.info(`waiting ${grace / 1000} s for a task being replaced to stop writing`)
    await new Promise((resolve) => setTimeout(resolve, grace))
  }
  await Promise.all(datasets.map((dataset) => bootstrap(lakeDest, dataset)))
  const views = new Map<string, LiveView>()
  for (const dataset of datasets) views.set(dataset, await follow(lakeDest, dataset))

  const port = Number(process.env.PORT || 8100)
  createPortal((dataset) => {
    const view = views.get(dataset)
    return view && Promise.resolve(view)
  }).listen(port, () => logger.info(`serving ${[...views.keys()].join(', ')} on ${port}`))
}

/**
 * A dataset whose backfill is not complete gets it first, from the SQD portal: `discover` when it
 * has no contracts yet, then `backfill` up to the height they were discovered at. Both resume where
 * an interrupted run stopped. Once the backfill is complete, the dataset is marked, and a restart
 * never reads the SQD portal again.
 */
async function bootstrap(lakeDest: string, dataset: string): Promise<void> {
  const log = logger.child(dataset)
  const config = loadConfig(dataset)
  const root = openDest(lakeDest, dataset)
  if (await root.exists(PARTIAL_FILE)) {
    throw new Error(`${dataset}: a development run wrote this lake with history left out (${PARTIAL_FILE}), so it is never served`)
  }
  if (await root.exists(BACKFILL_FILE)) return checkCoverage(config, JSON.parse(await root.readFile(BACKFILL_FILE)))
  if (!(await root.exists('contracts.json'))) {
    log.info('no lake yet: discovering the contracts the factories created, from the SQD portal')
    await runScript('discover.js', dataset)
  }
  const registry = JSON.parse(await root.readFile('contracts.json')) as Registry
  const chunks = openDest(lakeDest, dataset, 'chunks')
  const written = async () => ((await chunks.exists(STATUS_FILE)) ? Number((await chunks.readFile(STATUS_FILE)).split('\n')[0]) : -1)
  if ((await written()) < registry.height) {
    log.info(`backfilling from the SQD portal: the lake is at block ${await written()}, contracts are known up to ${registry.height}`)
    await runScript('backfill.js', dataset)
  }
  const reached = await written()
  if (reached < registry.height) throw new Error(`${dataset}: the backfill ended at block ${reached}, short of ${registry.height}`)
  await root.writeFile(BACKFILL_FILE, JSON.stringify(coverageOf(config, registry.height), null, 2) + '\n')
  log.info(`backfill complete up to block ${registry.height}`)
}

/** Runs one of this package's commands for a dataset, in a process of its own: they exit when done. */
function runScript(script: string, dataset: string): Promise<void> {
  return new Promise((resolve, reject) => {
    // The bounds meant for development runs are left out: a backfill here always covers the whole dataset.
    const { STOP_BLOCK, FROM_BLOCK, LAKE_ADDRESSES, ...env } = process.env
    const child = spawn(process.execPath, [join(__dirname, script)], { env: { ...env, DATASET: dataset }, stdio: 'inherit' })
    child.on('error', reject)
    child.on('exit', (code, signal) => (code === 0 ? resolve() : reject(new Error(`${script} for ${dataset} ended with ${signal ?? `code ${code}`}`))))
  })
}

/** Starts following one dataset, and returns what the portal serves of it. */
async function follow(lakeDest: string, dataset: string): Promise<LiveView> {
  const log = logger.child(dataset)
  const config = loadConfig(dataset)
  const root = openDest(lakeDest, dataset)
  const chunks = openDest(lakeDest, dataset, 'chunks')
  const registry = JSON.parse(await root.readFile('contracts.json')) as Registry
  const rpc = new RpcClient(rpcUrl(dataset))

  if (!(await chunks.exists(STATUS_FILE))) throw new Error(`${dataset} has no lake to continue: run the backfill first`)
  const [height, hash] = (await chunks.readFile(STATUS_FILE)).split('\n')
  const tail = new Tail({ height: Number(height), hash })
  const lake = await LakeDataset.open(lakeDest, dataset)

  const source = new RpcSource(rpc, config, registry, (r) => root.writeFile('contracts.json', JSON.stringify(r, null, 2) + '\n'), {
    maxRange: Number(process.env.MAX_RANGE || 2000),
    addressesPerCall: Number(process.env.ADDRESSES_PER_CALL || 500),
    pollMs: Number(process.env.POLL_MS || 5000),
  })

  // file-store's default status hooks, plus telling the portal what the files now hold. The lake
  // learns it first, so blocks the tail lets go of are already served from the new chunk.
  const db = new Database({
    tables,
    dest: chunks,
    chunkSizeMb: Number(process.env.CHUNK_SIZE_MB || 64),
    hooks: {
      async onStateRead(dest: Dest) {
        if (!(await dest.exists(STATUS_FILE))) return undefined
        const [h, x] = (await dest.readFile(STATUS_FILE)).split('\n')
        return { height: Number(h), hash: x || '0x' }
      },
      async onStateUpdate(dest: Dest, state: { height: number; hash: string }) {
        await dest.writeFile(STATUS_FILE, `${state.height}\n${state.hash}`)
        lake.setWritten(state)
        tail.written(state)
      },
    },
  })

  // Write a chunk at least this often, so the files in the lake never fall far behind and the
  // tail held in memory stays small.
  const flushEveryMs = Number(process.env.FLUSH_INTERVAL_MS || 30 * 60 * 1000)
  let lastFlush = Date.now()
  run(source as never, db, async (ctx) => {
    const blocks = ctx.blocks as unknown as LakeBlock[]
    writeBlocks(ctx.store, blocks)
    tail.add(blocks)
    if (Date.now() - lastFlush >= flushEveryMs) {
      ctx.store.setForceFlush(true)
      lastFlush = Date.now()
    }
  })

  // Hot blocks: everything above the last finalized block processed, polled from the chain head.
  const hot =
    process.env.HOT_BLOCKS === 'false'
      ? undefined
      : new HotChain(rpc, config, registry, () => tail.processed, {
          addressesPerCall: Number(process.env.ADDRESSES_PER_CALL || 500),
          maxBlocks: Number(process.env.HOT_MAX_BLOCKS || 1000),
        })
  if (hot) {
    const pollMs = Number(process.env.HOT_POLL_MS || 2000)
    const loop = async () => {
      try {
        await hot.poll()
      } catch (e) {
        log.warn({ err: e }, 'hot blocks poll failed; retrying')
      }
      setTimeout(loop, pollMs)
    }
    void loop()
  }

  log.info(`following ${registry.contracts.length} contracts; lake at block ${tail.lake.height}`)
  setInterval(
    () => log.info(`head ${hot?.head().number ?? '-'} (${hot?.size ?? 0} hot), finalized ${tail.processed.height}, written ${tail.lake.height}, ${tail.size} finalized blocks in memory`),
    60_000
  ).unref()
  return new LiveView(lake, tail, hot)
}

main().catch((e) => {
  logger.fatal(e)
  process.exit(1)
})
