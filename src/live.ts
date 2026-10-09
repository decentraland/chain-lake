import { ChildProcess, spawn } from 'child_process'
import { join } from 'path'
import { run } from '@subsquid/batch-processor'
import { createLogger } from '@subsquid/logger'
import { datasetsFromEnv, loadConfig, required } from './config'
import { bootstrap } from './bootstrap'
import { readCoverage } from './coverage'
import { openRegistry } from './discover'
import { HotChain } from './live/hot'
import { Tail } from './live/tail'
import { LiveView } from './live/view'
import { NodeBehind } from './follow/fetch'
import { RpcClient } from './follow/rpc'
import { RpcSource } from './follow/source'
import { Manifest, MANIFEST_FILE } from './manifest'
import { rpcUrl } from './serve/headers'
import { LakeDataset } from './serve/lake'
import { createPortal, NotReady } from './serve/server'
import { openStore, Store } from './store'
import { LakeBlock, writeBlocks } from './write'
import { openWriter } from './writer'

const logger = createLogger('lake:live')

/**
 * The datasets named by `DATASETS`, live. Each one has its own follower, which keeps its lake up
 * to date over RPC; one portal serves them all, each from its lake plus what its follower has
 * processed but not written yet. Squids point their portal URL here.
 *
 * - The portal listens at once. A dataset is served as soon as it is ready, and until then its
 *   requests get a 503, which squids retry.
 * - Any number of processes may run against the same lakes, during a deployment for instance: the
 *   lake's files are written by compare-and-swap, so one process extends each lake and any other
 *   fails before it publishes anything.
 * - RPC trouble stalls only the dataset it hits: its follower retries until the node answers again,
 *   and the portal keeps serving what it has. Any other failure ends the process, so the service
 *   restarts as a whole.
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

  const views = new Map<string, LiveView>()
  const states = new Map<string, DatasetState>(datasets.map((d) => [d, 'starting']))
  const port = Number(process.env.PORT || 8100)
  createPortal(
    (dataset) => {
      const view = views.get(dataset)
      if (view) return Promise.resolve(view)
      if (datasets.includes(dataset)) throw new NotReady(`${dataset} is ${states.get(dataset)}`)
      return undefined
    },
    // Ready once every dataset that was already complete is served again, so a deployment keeps the
    // task it replaces until then. A first backfill takes hours and does not hold the task back: the
    // portal had nothing to serve for that dataset before either.
    () => ({ ready: ![...states.values()].includes('starting'), datasets: Object.fromEntries(states) })
  ).listen(port, () => logger.info(`portal listening on ${port} for ${datasets.join(', ')}`))

  for (const dataset of datasets) {
    start(lakeDest, dataset, (state) => states.set(dataset, state)).then(
      (view) => {
        views.set(dataset, view)
        states.set(dataset, 'ready')
      },
      (e) => {
        logger.fatal(e)
        process.exit(1)
      }
    )
  }
}

/** `starting`: complete, and about to be served; `backfilling`: filling up from the SQD portal. */
type DatasetState = 'starting' | 'backfilling' | 'ready'

/** The commands this process runs, stopped with it. */
const children = new Set<ChildProcess>()
process.on('SIGTERM', () => {
  for (const child of children) child.kill('SIGTERM')
  process.exit(143)
})

async function start(lakeDest: string, dataset: string, report: (state: DatasetState) => void): Promise<LiveView> {
  const store = openStore(lakeDest, dataset)
  if (!(await readCoverage(store))?.value.complete) report('backfilling')
  await bootstrap(store, loadConfig(dataset), (script) => runScript(script, dataset), logger.child(dataset))
  return follow(lakeDest, store, dataset)
}

/** Runs one of this package's commands for a dataset, in a process of its own: they exit when done. */
function runScript(script: string, dataset: string): Promise<void> {
  return new Promise((resolve, reject) => {
    // The bounds meant for development runs are left out: a backfill here always covers the whole dataset.
    const { STOP_BLOCK, FROM_BLOCK, LAKE_ADDRESSES, ...env } = process.env
    const child = spawn(process.execPath, [join(__dirname, script)], { env: { ...env, DATASET: dataset }, stdio: 'inherit' })
    children.add(child)
    child.on('error', reject)
    child.on('exit', (code, signal) => {
      children.delete(child)
      if (code === 0) resolve()
      else reject(new Error(`${script} for ${dataset} ended with ${signal ?? `code ${code}`}`))
    })
  })
}

/** Starts following one dataset, and returns what the portal serves of it. */
async function follow(lakeDest: string, store: Store, dataset: string): Promise<LiveView> {
  const log = logger.child(dataset)
  const config = loadConfig(dataset)
  const rpc = new RpcClient(rpcUrl(dataset))

  // The lake's state first, then the registry: a contract is saved before any block that needs it is
  // committed, so a registry read after the manifest holds every contract up to the manifest's block.
  // The writer starts from exactly that manifest, or fails if another process moved it meanwhile.
  const pinned = await store.read<Manifest>(MANIFEST_FILE)
  if (!pinned) throw new Error(`${dataset} has no lake to continue: run the backfill first`)
  const { registry, save } = await openRegistry(store)
  const tail = new Tail({ height: pinned.value.height, hash: pinned.value.hash })
  const lake = await LakeDataset.open(lakeDest, dataset)

  const source = new RpcSource(rpc, config, registry, save, {
    maxRange: Number(process.env.MAX_RANGE || 2000),
    addressesPerCall: Number(process.env.ADDRESSES_PER_CALL || 500),
    pollMs: Number(process.env.POLL_MS || 5000),
  })

  // Each commit is told to the portal: the lake learns it first, so the blocks the tail lets go of
  // are already served from the new chunk.
  const db = openWriter(lakeDest, dataset, store, {
    chunkSizeMb: Number(process.env.CHUNK_SIZE_MB || 64),
    startAt: pinned.version,
    onCommit(committed) {
      lake.setWritten(committed)
      tail.written({ height: committed.height, hash: committed.hash })
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
        // At the head, a node behind the one that reported it is expected behind a load balancer.
        if (e instanceof NodeBehind) log.debug(`hot blocks poll: ${e.message}; retrying`)
        else log.warn({ err: e }, 'hot blocks poll failed; retrying')
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
