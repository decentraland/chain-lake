import { run } from '@subsquid/batch-processor'
import { DataSourceBuilder } from '@subsquid/evm-stream'
import { Database } from '@subsquid/file-store'
import { createLogger } from '@subsquid/logger'
import { blockFromEnv, loadConfig, required } from './config'
import { openDest } from './dest'
import { configuredContracts, Registry } from './discover'
import { portalSource } from './portal'
import { tables } from './tables'
import { writeBlocks } from './write'

const logger = createLogger('lake:backfill')

/** Addresses per log request; the portal caps the size of one query, not the number of requests. */
const ADDRESSES_PER_REQUEST = 1000

async function main() {
  const config = loadConfig()
  const lakeDest = required('LAKE_DEST')
  const registry = JSON.parse(
    await openDest(lakeDest, config.dataset).readFile('contracts.json')
  ) as Registry

  // Never past the height the registry was discovered at: a contract created later would be
  // missing from the filter, and its logs silently absent.
  const to = Math.min(blockFromEnv('STOP_BLOCK') ?? registry.height, registry.height)
  // LAKE_ADDRESSES narrows the run to a few contracts, for development.
  const only = process.env.LAKE_ADDRESSES?.toLowerCase().split(',').filter(Boolean)
  // The configured contracts and factories are always followed, even by an older registry.
  const followed = [...new Set([...configuredContracts(config), ...registry.contracts].map((c) => c.address))]
  const addresses = followed.filter((a) => !only || only.includes(a))
  if (addresses.length === 0) throw new Error('no contracts to backfill')

  const builder = new DataSourceBuilder()
    .setPortal(portalSource(config.dataset))
    .setBlockRange({ from: blockFromEnv('FROM_BLOCK') ?? config.fromBlock, to })
    .setFields({
      block: { timestamp: true, parentHash: true },
      log: { address: true, topics: true, data: true, transactionHash: true },
      transaction: { hash: true, from: true, to: true, input: true },
    })
  // Every log of every followed contract, not only the topics the squids read today.
  for (let i = 0; i < addresses.length; i += ADDRESSES_PER_REQUEST) {
    builder.addLog({ where: { address: addresses.slice(i, i + ADDRESSES_PER_REQUEST) }, include: { transaction: true } })
  }

  const db = new Database({
    tables,
    dest: openDest(lakeDest, config.dataset, 'chunks'),
    chunkSizeMb: Number(process.env.CHUNK_SIZE_MB || 64),
  })

  logger.info(`backfilling ${addresses.length} contracts of ${config.dataset} up to block ${to}`)
  run(builder.build(), db, async (ctx) => {
    writeBlocks(ctx.store, ctx.blocks)
    // The last chunk is written when the run reaches its end, not only when it fills up.
    const last = ctx.blocks[ctx.blocks.length - 1]
    if (last && last.header.number >= to) ctx.store.setForceFlush(true)
  })
}

main().catch((e) => {
  logger.fatal(e)
  process.exit(1)
})
