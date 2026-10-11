import { readFileSync } from 'fs'
import { AddressInfo } from 'net'
import { DataSourceBuilder, FieldSelection } from '@subsquid/evm-stream'
import { createLogger } from '@subsquid/logger'
import { blockFromEnv, loadConfig } from './config'
import { configuredContracts, followedAddresses, Registry, REGISTRY_FILE } from './discover'
import { portalSource } from './portal'
import { blocksOf, compareStreams, StreamedBlock, Tally } from './parity/compare'
import { LakeDataset } from './serve/lake'
import { createPortal } from './serve/server'
import { openStore } from './store'

const logger = createLogger('lake:parity')

type LogOptions = Parameters<DataSourceBuilder['addLog']>[0]

/** What a squid asks a portal for, in DataSourceBuilder's terms: its fields and its log requests. */
export interface QuerySpec {
  fields: FieldSelection
  logs: LogOptions[]
}

/** Everything the lake holds, as backfill reads it from the SQD portal. */
function lakeQuery(addresses: string[]): QuerySpec {
  return {
    fields: {
      // number, hash and parentHash are always included.
      block: { timestamp: true },
      log: { address: true, topics: true, data: true, transactionHash: true },
      transaction: { hash: true, from: true, to: true, input: true },
    },
    logs: [{ where: { address: addresses }, include: { transaction: true } }],
  }
}

/**
 * A squid's query from a file. Only its fields and log requests can be compared, so a query with
 * anything else, or with no log request, is refused rather than compared in part, or not at all.
 */
function readQuery(file: string): QuerySpec {
  const spec = JSON.parse(readFileSync(file, 'utf8')) as QuerySpec
  const unknown = Object.keys(spec).filter((key) => key !== 'fields' && key !== 'logs')
  if (unknown.length) throw new Error(`${file}: only fields and logs can be compared, not ${unknown.join(', ')}`)
  if (!Array.isArray(spec.logs) || !spec.logs.length) throw new Error(`${file}: no log request to compare`)
  return spec
}

function source(portal: Parameters<DataSourceBuilder['setPortal']>[0], spec: QuerySpec, from: number, to?: number) {
  const builder = new DataSourceBuilder().setPortal(portal).setBlockRange({ from, to }).setFields(spec.fields)
  for (const request of spec.logs) builder.addLog(request)
  return builder.build()
}

/**
 * Compares what the lake serves with what the SQD portal serves for one dataset and one query, block
 * by block, over the finalized blocks both hold. It only reads: a lake at LAKE_DEST is served from
 * inside this process, read-only, and nothing is written anywhere.
 *
 * - DATASET: the dataset to compare.
 * - LAKE_DEST: the lake (a local directory or `s3://bucket/prefix`); or LAKE_PORTAL_URL, a portal
 *   that already serves it.
 * - QUERY_FILE: a squid's query as JSON, `{ fields, logs: [{ where, include, range }] }`. Without
 *   it, everything the lake holds: every log of every followed contract, with its transaction.
 * - FROM_BLOCK, STOP_BLOCK: narrow the range, which is otherwise the query's start up to the lower of
 *   the two finalized heads.
 * - SQD_PORTAL_URL, SQD_PORTAL_API_KEY: the SQD portal, as for backfill.
 *
 * The report goes to stdout as JSON. It exits with 0 when both serve the same, 1 when they differ and
 * 2 when the comparison could not run or compared nothing.
 */
async function main() {
  const config = loadConfig()
  const dataset = config.dataset
  const lakeDest = process.env.LAKE_DEST
  const lakePortalUrl = process.env.LAKE_PORTAL_URL?.replace(/\/$/, '')
  if (!lakeDest && !lakePortalUrl) throw new Error('LAKE_DEST or LAKE_PORTAL_URL is required')

  let lakeUrl = `${lakePortalUrl}/datasets/${dataset}`
  let close = () => {}
  if (!lakePortalUrl) {
    // Opened before serving, so a lake that cannot be read fails here, not as retried 503s.
    const view = await LakeDataset.open(lakeDest!, dataset)
    const server = createPortal((name) => (name === dataset ? Promise.resolve(view) : undefined)).listen(0, '127.0.0.1')
    await new Promise((resolve) => server.once('listening', resolve))
    lakeUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/datasets/${dataset}`
    close = () => server.close()
  }

  let spec: QuerySpec
  if (process.env.QUERY_FILE) {
    spec = readQuery(process.env.QUERY_FILE)
  } else {
    // The contracts the factories created are in the lake's registry; without the lake at hand, only
    // the configured ones are compared.
    const registry = lakeDest ? (await openStore(lakeDest, dataset).read<Registry>(REGISTRY_FILE))?.value : undefined
    if (!registry) logger.warn(`no ${REGISTRY_FILE} at hand: comparing the configured contracts only`)
    spec = lakeQuery(registry ? followedAddresses(config, registry) : configuredContracts(config).map((c) => c.address))
  }

  const sqd = portalSource(dataset)
  const lake = { url: lakeUrl, http: { retryAttempts: 10, headers: {} } }
  const starts = spec.logs.map((r) => r.range?.from ?? config.fromBlock)
  const from = blockFromEnv('FROM_BLOCK') ?? Math.min(...starts)
  const [lakeHead, sqdHead] = await Promise.all([source(lake, spec, from).getFinalizedHead(), source(sqd, spec, from).getFinalizedHead()])
  const to = Math.min(blockFromEnv('STOP_BLOCK') ?? Infinity, lakeHead.number, sqdHead.number)
  logger.info(`${dataset}: comparing blocks ${from} to ${to} (lake head ${lakeHead.number}, SQD portal head ${sqdHead.number})`)

  const started = Date.now()
  let logged = started
  const progress = (tally: Tally) => {
    if (Date.now() - logged < 15_000) return
    logged = Date.now()
    logger.info(`${dataset}: at block ${tally.lastBlock} of ${to}: ${tally.left.blocks} blocks, ${tally.left.logs} logs, ${tally.differences} differences`)
  }
  const stream = (portal: Parameters<DataSourceBuilder['setPortal']>[0]) =>
    blocksOf(source(portal, spec, from, to).getFinalizedStream({ from, to })) as AsyncIterable<StreamedBlock>
  const tally = await compareStreams(stream(lake), stream(sqd), { progress })
  close()

  const seconds = Math.round((Date.now() - started) / 1000)
  // The report goes to stdout as one JSON document; the logs go to stderr.
  process.stdout.write(JSON.stringify({ dataset, from, to, seconds, lake: tally.left, sqd: tally.right, differences: tally.differences, samples: tally.samples }, null, 2) + '\n')
  // Two empty streams are identical, and prove nothing: a range or a query that matches nothing.
  if (!tally.left.blocks && !tally.right.blocks) {
    logger.error(`${dataset}: neither portal served a block with items over blocks ${from} to ${to}`)
    process.exit(2)
  }
  logger.info(`${dataset}: ${tally.differences ? `${tally.differences} differences` : 'identical'} over blocks ${from} to ${to}, in ${seconds}s`)
  process.exit(tally.differences ? 1 : 0)
}

if (require.main === module) {
  main().catch((e) => {
    logger.fatal(e)
    process.exit(2)
  })
}
