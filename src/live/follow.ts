import { run } from '@subsquid/batch-processor'
import { createLogger } from '@subsquid/logger'
import { checkRpcChain } from '../chain'
import { DatasetConfig } from '../config'
import { openRegistry } from '../discover'
import { NodeBehind } from '../follow/fetch'
import { RpcClient } from '../follow/rpc'
import { RpcSource } from '../follow/source'
import { Manifest, MANIFEST_FILE } from '../manifest'
import { Filters, identityOf, ScanLog } from '../receipts'
import { rpcUrl } from '../serve/headers'
import { LakeDataset } from '../serve/lake'
import { Store } from '../store'
import { writeBlocks } from '../write'
import { openWriter } from '../writer'
import { HotChain } from './hot'
import { Tail } from './tail'
import { LiveView } from './view'

const logger = createLogger('lake:live')

/**
 * Starts following one dataset, and returns what the portal serves of it. The lake as it stands is
 * handed to `serve` first, before the RPC is checked: an RPC outage holds back new blocks, never the
 * history already written. The follower and the hot blocks start only once the RPC is checked to serve
 * the dataset's chain.
 */
export async function follow(
  lakeDest: string,
  store: Store,
  dataset: string,
  config: DatasetConfig,
  serve: (view: LiveView) => void,
  options: { checkSource?: () => Promise<void> } = {}
): Promise<LiveView> {
  const log = logger.child(dataset)
  const rpc = new RpcClient(rpcUrl(dataset))

  // The lake's state first, then the registry: a contract is saved before any block that needs it is
  // committed, so a registry read after the manifest holds every contract up to the manifest's block.
  // The writer starts from exactly that manifest, or fails if another process moved it meanwhile.
  const pinned = await store.read<Manifest>(MANIFEST_FILE)
  if (!pinned) throw new Error(`${dataset} has no lake to continue: run the backfill first`)
  const { registry, save } = await openRegistry(store)
  const tail = new Tail({ height: pinned.value.height, hash: pinned.value.hash })
  const lake = await LakeDataset.open(lakeDest, dataset)
  // A lake the portal cannot read would otherwise be served as a 503 on every request.
  await lake.check()
  // Until the RPC is checked, the view serves only what the lake's files hold: a header they lack is a
  // retryable 503, not a header from what may be another chain.
  lake.holdRpcHeaders()
  serve(new LiveView(lake, new Tail({ height: pinned.value.height, hash: pinned.value.hash })))
  // The check alone releases the headers; naming the lake for its receipts runs beside it, and both
  // come before the writer. An RPC serving another chain stops it here.
  const checked = (options.checkSource ?? (() => checkRpcChain(rpc, config, log)))().then(() => lake.allowRpcHeaders())
  // A refused RPC ends the start at once, whether or not the lake is named yet.
  const [identity] = await Promise.all([identityOf(store, config), checked])

  const scans = ScanLog.reading()
  const source = new RpcSource(rpc, config, registry, save, {
    maxRange: Number(process.env.MAX_RANGE || 2000),
    addressesPerCall: Number(process.env.ADDRESSES_PER_CALL || 500),
    pollMs: Number(process.env.POLL_MS || 5000),
    scans,
  })

  // Each commit is told to the portal: the lake learns it first, so the blocks the tail lets go of
  // are already served from the new chunk.
  const db = openWriter(lakeDest, dataset, store, {
    chunkSizeMb: Number(process.env.CHUNK_SIZE_MB || 64),
    startAt: pinned.version,
    receipts: { identity, source: 'rpc', lowerBound: pinned.value.height + 1, scans, filters: new Filters(store), log },
    onCommit(committed) {
      lake.setWritten(committed)
      tail.written({ height: committed.height, hash: committed.hash })
    },
  })

  // Write a chunk at least this often, so the files in the lake never fall far behind and the
  // tail held in memory stays small.
  const flushEveryMs = Number(process.env.FLUSH_INTERVAL_MS || 30 * 60 * 1000)
  let lastFlush = Date.now()
  run(source, db, async (ctx) => {
    const blocks = ctx.blocks
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
