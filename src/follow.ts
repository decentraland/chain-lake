import { run } from '@subsquid/batch-processor'
import { createLogger } from '@subsquid/logger'
import { blockFromEnv, loadConfig, required } from './config'
import { markPartial } from './coverage'
import { openRegistry } from './discover'
import { RpcClient } from './follow/rpc'
import { RpcSource } from './follow/source'
import { Manifest, MANIFEST_FILE } from './manifest'
import { rpcUrl } from './serve/headers'
import { openStore } from './store'
import { writeBlocks } from './write'
import { openWriter } from './writer'

const logger = createLogger('lake:follow')

/**
 * Keeps a dataset of the lake up to date over RPC, without the SQD portal: it continues from the
 * height the backfill (or a previous run) left in the lake and appends finalized blocks.
 */
async function main() {
  const config = loadConfig()
  const lakeDest = required('LAKE_DEST')
  const store = openStore(lakeDest, config.dataset)
  const stop = blockFromEnv('STOP_BLOCK')

  // The follower continues a lake; on an empty one it would start from block 0 over RPC. It needs
  // the backfill's manifest, or an explicit FROM_BLOCK to start a development lake.
  if (!(await store.read(MANIFEST_FILE))) {
    const from = blockFromEnv('FROM_BLOCK')
    if (from === undefined) throw new Error(`${config.dataset} has no lake to continue: run the backfill first, or set FROM_BLOCK`)
    const parent = await new RpcClient(rpcUrl(config.dataset)).call<{ hash: string }>('eth_getBlockByNumber', ['0x' + (from - 1).toString(16), false])
    await markPartial(store, config, `a development lake started at block ${from} left out the history before it`)
    await store.write<Manifest>(MANIFEST_FILE, { height: from - 1, hash: parent.hash, chunks: [] }, null)
    logger.info(`starting a new lake at block ${from}`)
  }

  // The lake's state first, then the registry, as in live: the writer starts from exactly that state.
  const pinned = (await store.read<Manifest>(MANIFEST_FILE))!
  const { registry, save } = await openRegistry(store)
  const source = new RpcSource(new RpcClient(rpcUrl(config.dataset)), config, registry, save, {
    maxRange: Number(process.env.MAX_RANGE || 2000),
    addressesPerCall: Number(process.env.ADDRESSES_PER_CALL || 500),
    pollMs: Number(process.env.POLL_MS || 5000),
    // STOP_BLOCK bounds a run, for comparisons; without it the follower keeps up with the chain.
    stopBlock: stop,
  })

  const db = openWriter(lakeDest, config.dataset, store, { chunkSizeMb: Number(process.env.CHUNK_SIZE_MB || 64), startAt: pinned.version })

  logger.info(`following ${config.dataset}: ${registry.contracts.length} contracts, registry at block ${registry.height}`)
  run(source as never, db, async (ctx) => {
    writeBlocks(ctx.store, ctx.blocks as never)
    const last = ctx.blocks[ctx.blocks.length - 1] as { header: { number: number } } | undefined
    if (stop !== undefined && last && last.header.number >= stop) ctx.store.setForceFlush(true)
  })
}

main().catch((e) => {
  logger.fatal(e)
  process.exit(1)
})
