import { DataSourceBuilder } from '@subsquid/evm-stream'
import { createLogger } from '@subsquid/logger'
import { blockFromEnv, Contract, DatasetConfig, loadConfig, required } from './config'
import { openDest } from './dest'
import { portalSource } from './portal'

const logger = createLogger('lake:discover')

export interface RegisteredContract extends Contract {
  /** The factory that created it, for discovered contracts. */
  factory?: string
  /** Where it was created, for discovered contracts. */
  createdAt?: { block: number; logIndex: number; transactionHash: string }
}

/** Every contract the lake follows up to a block: the configured ones and those factories created. */
export interface Registry {
  dataset: string
  height: number
  contracts: RegisteredContract[]
}

/** The address a factory log announces, from the topic the factory puts it in. */
export function createdAddress(topics: string[], addressTopic: number): string {
  const topic = topics[addressTopic]
  if (!topic || !/^0x[0-9a-fA-F]{64}$/.test(topic)) throw new Error(`topic ${addressTopic} is not an address word`)
  return ('0x' + topic.slice(-40)).toLowerCase()
}

/** The contracts a dataset follows before discovery: its configured contracts and its factories. */
export function configuredContracts(config: DatasetConfig): RegisteredContract[] {
  // Factories are followed too: squids read their creation events (marketplace reads ProxyCreated).
  return [...config.contracts, ...config.factories].map((c) => ({ name: c.name, address: c.address }))
}

export async function discover(config: DatasetConfig, to: number): Promise<Registry> {
  const contracts: RegisteredContract[] = configuredContracts(config)
  if (config.factories.length > 0) {
    const factories = new Map(config.factories.map((f) => [f.address, f]))
    const builder = new DataSourceBuilder()
      .setPortal(portalSource(config.dataset))
      .setFields({ log: { address: true, topics: true, transactionHash: true } })
    for (const f of config.factories) {
      builder.addLog({ where: { address: [f.address], topic0: [f.topic0] }, range: { from: f.fromBlock } })
    }
    const from = Math.min(...config.factories.map((f) => f.fromBlock))
    for await (const batch of builder.build().getFinalizedStream({ from, to })) {
      for (const block of batch.blocks) {
        for (const log of block.logs) {
          const factory = factories.get(log.address.toLowerCase())
          if (!factory || log.topics[0] !== factory.topic0) continue
          contracts.push({
            name: 'collection',
            address: createdAddress(log.topics, factory.addressTopic),
            factory: factory.name,
            createdAt: { block: block.header.number, logIndex: log.logIndex, transactionHash: log.transactionHash },
          })
        }
      }
      const last = batch.blocks[batch.blocks.length - 1]
      if (last) logger.info(`scanned to block ${last.header.number}: ${contracts.length} contracts`)
    }
  }
  return { dataset: config.dataset, height: to, contracts: dedupe(contracts) }
}

function dedupe(contracts: RegisteredContract[]): RegisteredContract[] {
  const seen = new Set<string>()
  return contracts.filter((c) => !seen.has(c.address) && seen.add(c.address))
}

async function main() {
  const config = loadConfig()
  const lakeDest = required('LAKE_DEST')
  // Discover up to STOP_BLOCK, or up to the portal's finalized head.
  const head = await new DataSourceBuilder().setPortal(portalSource(config.dataset)).build().getFinalizedHead()
  const to = Math.min(blockFromEnv('STOP_BLOCK') ?? head.number, head.number)
  const registry = await discover(config, to)
  await openDest(lakeDest, config.dataset).writeFile('contracts.json', JSON.stringify(registry, null, 2) + '\n')
  logger.info(`${registry.contracts.length} contracts up to block ${to} written to ${config.dataset}/contracts.json`)
}

if (require.main === module) {
  main().catch((e) => {
    logger.fatal(e)
    process.exit(1)
  })
}
