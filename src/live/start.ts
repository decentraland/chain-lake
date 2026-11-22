import { createLogger } from '@subsquid/logger'
import { missingContracts } from '../absorb'
import { bootstrap } from '../bootstrap'
import { checkRpcChain } from '../chain'
import { DatasetConfig } from '../config'
import { readCoverage } from '../coverage'
import { RpcClient } from '../follow/rpc'
import { Manifest, MANIFEST_FILE } from '../manifest'
import { rpcUrl } from '../serve/headers'
import { LakeDataset } from '../serve/lake'
import { Store } from '../store'
import { follow } from './follow'
import { Tail } from './tail'
import { LiveView } from './view'

const logger = createLogger('lake:live')

/**
 * `starting`: complete, and about to be served; `backfilling`: filling up from the SQD portal;
 * `absorbing`: taking in the history of contracts its config gained, over RPC, and served meanwhile as
 * its lake stood. Neither of the last two holds the task back: they take longer than a health check
 * waits, and a task killed meanwhile would start them over.
 */
export type DatasetState = 'starting' | 'backfilling' | 'absorbing' | 'ready'

export type Script = 'discover.js' | 'backfill.js' | 'absorb.js'

/**
 * Makes one dataset ready and follows it, returning what the portal serves of it once followed.
 *
 * The dataset's RPC is checked once, by this process, as soon as it starts. Until it is, every view
 * this process serves of the dataset answers a header its lake's files lack with a retryable 503,
 * never with a header from what may be another chain; once it is, those views read such headers from
 * the RPC again, even while an absorb is still running.
 */
export async function start(options: {
  lakeDest: string
  store: Store
  config: DatasetConfig
  report: (state: DatasetState) => void
  serve: (view: LiveView) => void
  runScript: (script: Script) => Promise<void>
  /** Checks the dataset's RPC serves its chain; `eth_chainId` by default. */
  checkSource?: () => Promise<void>
}): Promise<LiveView> {
  const { lakeDest, store, config, report, serve, runScript } = options
  const log = logger.child(config.dataset)
  const checked = (options.checkSource ?? (() => checkRpcChain(new RpcClient(rpcUrl(config.dataset)), config, log)))()
  // A refusal ends the start where the follower waits for the check, not as an unhandled rejection.
  checked.catch(() => {})

  const coverage = (await readCoverage(store))?.value
  if (!coverage?.complete) {
    report('backfilling')
  } else if (!coverage.partial && missingContracts(config, coverage).length) {
    // The lake is served as it stands while the new contracts are absorbed, without new blocks: the
    // task a deployment replaces stops once this one is ready, and its other contracts' consumers then
    // wait for new blocks rather than get 503s. The new contracts' history is served once absorbed.
    serve(await asItStands(lakeDest, store, config.dataset, checked))
    report('absorbing')
  }
  await bootstrap(store, config, runScript, log)
  // The lake as it stands is served, and the dataset ready, while the RPC is checked.
  return follow(
    lakeDest,
    store,
    config.dataset,
    config,
    (view) => {
      serve(view)
      report('ready')
    },
    { checkSource: () => checked }
  )
}

/** A complete lake served as its manifest stands, with no follower, reading headers from the RPC once `checked`. */
async function asItStands(lakeDest: string, store: Store, dataset: string, checked: Promise<void>): Promise<LiveView> {
  const manifest = await store.read<Manifest>(MANIFEST_FILE)
  if (!manifest) throw new Error(`${dataset} has no lake to serve`)
  const lake = await LakeDataset.open(lakeDest, dataset)
  await lake.check()
  lake.holdRpcHeaders()
  checked.then(
    () => lake.allowRpcHeaders(),
    () => {}
  )
  return new LiveView(lake, new Tail({ height: manifest.value.height, hash: manifest.value.hash }))
}
