import { randomBytes } from 'crypto'
import { Database, Dest } from '@subsquid/file-store'
import { Logger } from '@subsquid/logger'
import { openDest } from './dest'
import { Chunk, Manifest, MANIFEST_FILE } from './manifest'
import { appendReceipt, Filters, Identity, ScanLog, ScanSource, segmentsOf } from './receipts'
import { Conflict, Store, Versioned } from './store'
import { tables } from './tables'

const CHUNK_FOLDER = /^(\d+)-(\d+)$/

/**
 * file-store's Database over a dataset's lake, safe whatever the number of processes writing it: one
 * of them extends the lake, and the others fail before they publish anything.
 *
 * - Each process writes its chunks to folders of its own (`<from>-<to>-<writer>`), so it never writes
 *   or deletes another's files.
 * - A chunk joins the lake when the manifest that lists it is written, by compare-and-swap from the
 *   version this process last read or wrote. A process that finds the manifest changed stops.
 * - Readers serve only the chunks the manifest lists, so a chunk left behind by a writer that failed,
 *   or that lost the manifest to another, is never read.
 */
export function openWriter(
  lakeDest: string,
  dataset: string,
  store: Store,
  options: {
    chunkSizeMb: number
    /**
     * The manifest version this process started from, read before anything that must be at least
     * as recent (the registry of contracts). Connecting to any other version fails with a Conflict,
     * so the process never continues a lake from a state newer than what it read alongside it.
     */
    startAt?: string
    onCommit?: (manifest: Manifest) => void
    /** Writes a receipt of what each commit read; see `receipts.ts`. */
    receipts?: CommitReceipts
  }
) {
  const writer = randomBytes(16).toString('hex')
  const chunks = openDest(lakeDest, dataset, 'chunks')
  let manifest: Versioned<Manifest> | undefined
  let flushed: Chunk | undefined

  const dest: Dest = {
    readFile: (file) => chunks.readFile(file),
    writeFile: (file, data) => chunks.writeFile(file, data),
    exists: (path) => chunks.exists(path),
    mkdir: (path) => chunks.mkdir(path),
    // file-store lists the folder only to delete unfinished chunks, and nothing is deleted here: a
    // folder the manifest does not list is never read, and it may be in use by the process writing it.
    // (On S3, listing a lake that has no chunk yet would fail.)
    readdir: async () => [],
    rm: async () => {},
    async transact(folder, cb) {
      const range = folder.match(CHUNK_FOLDER)
      if (!range) throw new Error(`${dataset}: unexpected chunk folder ${folder}`)
      const dir = `${folder}-${writer}`
      await chunks.transact(dir, cb)
      flushed = { dir, from: Number(range[1]), to: Number(range[2]) }
    },
    path: (...paths) => chunks.path(...paths),
  }

  return new Database({
    tables,
    dest,
    chunkSizeMb: options.chunkSizeMb,
    hooks: {
      async onStateRead() {
        if (manifest) {
          // file-store reads the state before every batch: a manifest that changed since this process
          // wrote it means another process extends the lake, and this one stops before it writes more.
          // Its version is enough to tell.
          if ((await store.version(MANIFEST_FILE)) !== manifest.version) throw new Conflict(`${dataset}: another process extended the lake`)
          return { height: manifest.value.height, hash: manifest.value.hash }
        }
        const read = await store.read<Manifest>(MANIFEST_FILE)
        if (options.startAt !== undefined && read?.version !== options.startAt) {
          throw new Conflict(`${dataset}: another process extended the lake while this one started`)
        }
        manifest = read
        return read && { height: read.value.height, hash: read.value.hash }
      },
      async onStateUpdate(_dest: Dest, state: { height: number; hash: string }, prev?: { height: number; hash: string }) {
        // The chunk this commit closes, when the batch had rows to write.
        const closed = flushed && prev && flushed.from === prev.height + 1 && flushed.to === state.height ? [flushed] : []
        flushed = undefined
        const next: Manifest = { height: state.height, hash: state.hash, chunks: [...(manifest?.value.chunks ?? []), ...closed] }
        const version = await store.write(MANIFEST_FILE, next, manifest?.version ?? null)
        manifest = { value: next, version }
        options.onCommit?.(next)
        // The commit is published either way: a receipt that cannot be written leaves its blocks unproven,
        // and never stops the writer.
        const receipts = options.receipts
        if (receipts) await recordCommit(store, receipts, prev, state, closed).catch((e) => receipts.log.error({ err: e }, `blocks to ${state.height} got no receipt`))
      },
    },
  })
}

/** What a writer needs to write the receipt of each commit. */
export interface CommitReceipts {
  /** The lake's identity, or how to get it once the lake has a manifest (see `lazyIdentity`). */
  identity: Identity | (() => Promise<Identity>)
  source: ScanSource
  /** The first block the source reads: no commit claims a block below it, even its first one. */
  lowerBound: number
  scans: ScanLog
  filters: Filters
  log: Logger
}

/**
 * The receipt of a commit this writer just published: the blocks it moved the lake over, from the
 * source's lower bound at most, each with the filter it was read with. A commit whose blocks were not
 * all read gets none, and the blocks it covers stay unproven.
 */
async function recordCommit(
  store: Store,
  receipts: CommitReceipts,
  prev: { height: number; hash: string } | undefined,
  state: { height: number; hash: string },
  closed: Chunk[]
): Promise<void> {
  const from = Math.max(prev ? prev.height + 1 : receipts.lowerBound, receipts.lowerBound)
  const reads = receipts.scans.take(from, state.height)
  if (from > state.height) return
  if (!reads) {
    receipts.log.warn(`blocks ${from}-${state.height} were committed without a read of each: they get no receipt`)
    return
  }
  const segments = await segmentsOf(receipts.filters, reads)
  const identity = typeof receipts.identity === 'function' ? await receipts.identity() : receipts.identity
  await appendReceipt(store, identity, {
    kind: 'commit',
    source: receipts.source,
    segments,
    to: state.height,
    toHash: state.hash,
    wrote: closed.map((c) => c.dir),
    replaced: {},
  })
}
