import { ChildProcess, spawn } from 'child_process'
import { join } from 'path'
import { createLogger } from '@subsquid/logger'
import { datasetsFromEnv, loadConfig, required } from './config'
import { oneAtATime, scriptArgs } from './live/scripts'
import { DatasetState, start } from './live/start'
import { LiveView } from './live/view'
import { createPortal, NotReady } from './serve/server'
import { openStore } from './store'

const logger = createLogger('lake:live')

/**
 * The datasets named by `DATASETS`, live. Each one has its own follower, which keeps its lake up
 * to date over RPC; one portal serves them all, each from its lake plus what its follower has
 * processed but not written yet. Squids point their portal URL here.
 *
 * - The portal listens at once. A dataset is served as soon as it is ready, and until then its
 *   requests get a 503, which squids retry.
 * - On S3, any number of processes may run against the same lakes, during a deployment for instance:
 *   the lake's files are written by compare-and-swap, so one process extends each lake and any other
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
    // portal had nothing to serve for that dataset before either. A dataset absorbing new contracts is
    // served meanwhile, as its lake stood (see `start`).
    () => ({ ready: ![...states.values()].includes('starting'), datasets: Object.fromEntries(states) })
  ).listen(port, () => logger.info(`portal listening on ${port} for ${datasets.join(', ')}`))

  for (const dataset of datasets) {
    Promise.resolve()
      .then(() =>
        start({
          lakeDest,
          store: openStore(lakeDest, dataset),
          config: loadConfig(dataset),
          report: (state) => states.set(dataset, state),
          serve: (view) => views.set(dataset, view),
          runScript: (script) => runScript(script, dataset),
        })
      )
      .then(
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

/** The commands this process runs, stopped with it. */
const children = new Set<ChildProcess>()
process.on('SIGTERM', () => {
  for (const child of children) child.kill('SIGTERM')
  process.exit(143)
})

const absorbQueue = oneAtATime()

/** Runs one of this package's commands for a dataset; absorbs wait for each other. */
function runScript(script: string, dataset: string): Promise<void> {
  return script === 'absorb.js' ? absorbQueue(() => spawnScript(script, dataset)) : spawnScript(script, dataset)
}

/** Runs one of this package's commands for a dataset, in a process of its own: they exit when done. */
function spawnScript(script: string, dataset: string): Promise<void> {
  return new Promise((resolve, reject) => {
    // The bounds meant for development runs are left out: a backfill here always covers the whole dataset.
    const { STOP_BLOCK, FROM_BLOCK, LAKE_ADDRESSES, ...env } = process.env
    const child = spawn(process.execPath, [...scriptArgs(script, env), join(__dirname, script)], { env: { ...env, DATASET: dataset }, stdio: 'inherit' })
    children.add(child)
    child.on('error', reject)
    child.on('exit', (code, signal) => {
      children.delete(child)
      if (code === 0) resolve()
      else reject(new Error(`${script} for ${dataset} ended with ${signal ?? `code ${code}`}`))
    })
  })
}

main().catch((e) => {
  logger.fatal(e)
  process.exit(1)
})
