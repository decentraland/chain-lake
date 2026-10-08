import { run } from '@subsquid/batch-processor'
import { Database } from '@subsquid/file-store'
import { createLogger } from '@subsquid/logger'
import { blockFromEnv, loadConfig, required } from './config'
import { openDest } from './dest'
import { Registry } from './discover'
import { RpcClient } from './follow/rpc'
import { RpcSource } from './follow/source'
import { rpcUrl } from './serve/headers'
import { tables } from './tables'
import { writeBlocks } from './write'

const logger = createLogger('lake:follow')

/**
 * Keeps a dataset of the lake up to date over RPC, without the SQD portal: it continues from the
 * height the backfill (or a previous run) left in the lake and appends finalized blocks.
 */
async function main() {
  const config = loadConfig()
  const lakeDest = required('LAKE_DEST')
  const root = openDest(lakeDest, config.dataset)
  const registry = JSON.parse(await root.readFile('contracts.json')) as Registry
  const stop = blockFromEnv('STOP_BLOCK')

  const source = new RpcSource(
    new RpcClient(rpcUrl(config.dataset)),
    config,
    registry,
    (r) => root.writeFile('contracts.json', JSON.stringify(r, null, 2) + '\n'),
    {
      maxRange: Number(process.env.MAX_RANGE || 2000),
      addressesPerCall: Number(process.env.ADDRESSES_PER_CALL || 500),
      pollMs: Number(process.env.POLL_MS || 5000),
      // STOP_BLOCK bounds a run, for comparisons; without it the follower keeps up with the chain.
      stopBlock: stop,
    }
  )

  // The follower continues a lake; on an empty one it would start from block 0 over RPC. It needs
  // the backfill's status, or an explicit FROM_BLOCK to seed one.
  const chunks = openDest(lakeDest, config.dataset, 'chunks')
  if (!(await chunks.exists('status.txt'))) {
    const from = blockFromEnv('FROM_BLOCK')
    if (from === undefined) throw new Error(`${config.dataset} has no lake to continue: run the backfill first, or set FROM_BLOCK`)
    const parent = await new RpcClient(rpcUrl(config.dataset)).call<{ hash: string }>('eth_getBlockByNumber', ['0x' + (from - 1).toString(16), false])
    await chunks.writeFile('status.txt', `${from - 1}\n${parent.hash}`)
    logger.info(`starting a new lake at block ${from}`)
  }

  const db = new Database({
    tables,
    dest: openDest(lakeDest, config.dataset, 'chunks'),
    chunkSizeMb: Number(process.env.CHUNK_SIZE_MB || 64),
  })

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
