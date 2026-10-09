/** The file that says what a dataset's lake holds. */
export const MANIFEST_FILE = 'manifest.json'

export interface Chunk {
  /** The folder under `chunks/`, `<from>-<to>-<writer>`. */
  dir: string
  from: number
  to: number
}

/** What the lake holds: every block up to `height`, in the chunks listed. Nothing else is part of it. */
export interface Manifest {
  height: number
  hash: string
  chunks: Chunk[]
}
