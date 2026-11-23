import { run } from '@subsquid/batch-processor'
import { DataSourceBuilder } from '@subsquid/evm-stream'
import { createLogger } from '@subsquid/logger'
import { checkPortalChain } from './chain'
import { blockFromEnv, DatasetConfig, loadConfig, required } from './config'
import { checkCoverage, markPartial, readCoverage } from './coverage'
import { followedAddresses, Registry, REGISTRY_FILE } from './discover'
import { MANIFEST_FILE } from './manifest'
import { RpcClient } from './follow/rpc'
import { portalSource } from './portal'
import { Filters, lazyIdentity, refuseReplacedLake, ScanLog } from './receipts'
import { rpcUrl } from './serve/headers'
import { openStore, Store } from './store'
import { writeBlocks } from './write'
import { openWriter } from './writer'

const logger = createLogger('lake:backfill')

/**
 * Refuses a backfill that would receipt another chain's history as this dataset's, or start a lake from
 * nothing where another one stood.
 */
export async function checkBackfill(store: Store, config: DatasetConfig, portalHead: { number: number; hash: string }, rpc: RpcClient): Promise<void> {
  await checkPortalChain(portalHead, rpc, config, logger)
  // A backfill with no manifest yet starts the lake: never where another lake stood. The lake is named
  // by its first receipt, once its manifest exists.
  if (!(await store.read(MANIFEST_FILE))) await refuseReplacedLake(store, config)
}

/**
 * The `backfill` command for one dataset, from the SQD portal. It is checked before anything is written:
 * a portal serving another chain, or a lake started where another stood, is refused with the coverage as
 * it was. Only then is a development run (`fromBlock`, `only`) marked partial, and the writer started.
 */
export async function backfillDataset(
  config: DatasetConfig,
  lakeDest: string,
  store: Store,
  options: { portalHead: () => Promise<{ number: number; hash: string }>; rpc: RpcClient; fromBlock?: number; stopBlock?: number; only?: string[] }
): Promise<void> {
  const registry = (await store.read<Registry>(REGISTRY_FILE))?.value
  const coverage = (await readCoverage(store))?.value
  if (!registry || !coverage) throw new Error(`${config.dataset} has not been discovered: run discover on an empty location first`)

  // Never past the height the registry was discovered at: a contract created later would be
  // missing from the filter, and its logs silently absent.
  const to = Math.min(options.stopBlock ?? registry.height, registry.height)
  // `only` narrows the run to a few contracts, for development.
  const only = options.only
  const addresses = followedAddresses(config, registry).filter((a) => !only || only.includes(a))
  if (addresses.length === 0) throw new Error('no contracts to backfill')
  const from = options.fromBlock

  await checkBackfill(store, config, await options.portalHead(), options.rpc)
  if (only || from !== undefined) {
    await markPartial(store, config, `a development backfill (LAKE_ADDRESSES=${only?.join(',') ?? ''} FROM_BLOCK=${from ?? ''}) left history out`)
  } else {
    checkCoverage(config, coverage)
  }
  const identity = lazyIdentity(store, config)
  const lowerBound = from ?? config.fromBlock

  const builder = new DataSourceBuilder()
    .setPortal(portalSource(config.dataset))
    .setBlockRange({ from: lowerBound, to })
    .setFields({
      block: { timestamp: true, parentHash: true },
      log: { address: true, topics: true, data: true, transactionHash: true },
      transaction: { hash: true, from: true, to: true, input: true },
    })
  // Every log of every followed contract, not only the topics the squids read today. It is one
  // request with every address, a few hundred KB on Polygon, which the shared SQD portal accepts.
  builder.addLog({ where: { address: addresses }, include: { transaction: true } })

  // Every block from the lower bound on is read with the one filter above, from the SQD portal.
  const db = openWriter(lakeDest, config.dataset, store, {
    chunkSizeMb: Number(process.env.CHUNK_SIZE_MB || 64),
    receipts: { identity, source: 'sqd-portal', lowerBound, scans: ScanLog.constant(addresses), filters: new Filters(store), log: logger },
  })

  logger.info(`backfilling ${addresses.length} contracts of ${config.dataset} up to block ${to}`)
  run(builder.build(), db, async (ctx) => {
    writeBlocks(ctx.store, ctx.blocks)
    // The last chunk is written when the run reaches its end, not only when it fills up.
    const last = ctx.blocks[ctx.blocks.length - 1]
    if (last && last.header.number >= to) ctx.store.setForceFlush(true)
  })
}

async function main() {
  const config = loadConfig()
  const lakeDest = required('LAKE_DEST')
  const source = new DataSourceBuilder().setPortal(portalSource(config.dataset)).build()
  await backfillDataset(config, lakeDest, openStore(lakeDest, config.dataset), {
    portalHead: () => source.getFinalizedHead(),
    rpc: new RpcClient(rpcUrl(config.dataset)),
    fromBlock: blockFromEnv('FROM_BLOCK'),
    stopBlock: blockFromEnv('STOP_BLOCK'),
    only: process.env.LAKE_ADDRESSES?.toLowerCase().split(',').filter(Boolean),
  })
}

if (require.main === module) {
  main().catch((e) => {
    logger.fatal(e)
    process.exit(1)
  })
}
