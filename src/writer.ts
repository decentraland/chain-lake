import { randomBytes } from 'crypto'
import { Database, Dest } from '@subsquid/file-store'
import { openDest } from './dest'
import { Chunk, Manifest, MANIFEST_FILE } from './manifest'
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
export function openWriter(lakeDest: string, dataset: string, store: Store, options: { chunkSizeMb: number; onCommit?: (manifest: Manifest) => void }) {
  const writer = randomBytes(4).toString('hex')
  const chunks = openDest(lakeDest, dataset, 'chunks')
  let manifest: Versioned<Manifest> | undefined
  let flushed: Chunk | undefined

  const dest: Dest = {
    readFile: (file) => chunks.readFile(file),
    writeFile: (file, data) => chunks.writeFile(file, data),
    exists: (path) => chunks.exists(path),
    mkdir: (path) => chunks.mkdir(path),
    readdir: (path) => chunks.readdir(path),
    // Nothing is deleted: a folder the manifest does not list is never read, and it may be in use by
    // the process writing it.
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
        const read = await store.read<Manifest>(MANIFEST_FILE)
        // file-store reads the state before every batch: a manifest that changed since this process
        // wrote it means another process extends the lake, and this one stops before it writes more.
        if (manifest && read?.version !== manifest.version) throw new Conflict(`${dataset}: another process extended the lake`)
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
      },
    },
  })
}
