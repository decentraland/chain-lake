import { createHash } from 'crypto'
import { mkdir, readFile, rename, writeFile } from 'fs/promises'
import { join } from 'path'
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'

/** A file's content and the version it was read at. */
export interface Versioned<T> {
  value: T
  version: string
}

/** The file changed since this process read it: another process writes the dataset, and this one must stop. */
export class Conflict extends Error {}

/**
 * The few files that say what a dataset's lake holds: its manifest, its registry of contracts and its
 * backfill's coverage. Every write is a compare-and-swap against the version this process last read or
 * wrote, so of two processes writing the same dataset, only one succeeds; the other gets a Conflict.
 */
export interface Store {
  read<T>(name: string): Promise<Versioned<T> | undefined>
  /** Writes `value` if the file is still at `version` (null: if it does not exist yet), and returns its new version. */
  write<T>(name: string, value: T, version: string | null): Promise<string>
}

export function openStore(lakeDest: string, dataset: string): Store {
  const location = `${lakeDest.replace(/\/$/, '')}/${dataset}`
  return location.startsWith('s3://') ? new S3Store(location) : new LocalStore(location)
}

const serialize = (value: unknown) => JSON.stringify(value, null, 2) + '\n'

/** S3's conditional writes: If-Match on the ETag read, or If-None-Match for a file that must not exist yet. */
class S3Store implements Store {
  private readonly client = new S3Client({})
  private readonly bucket: string
  private readonly prefix: string

  constructor(location: string) {
    const url = new URL(location)
    this.bucket = url.hostname
    this.prefix = url.pathname.replace(/^\/+|\/+$/g, '')
  }

  private key(name: string): string {
    return this.prefix ? `${this.prefix}/${name}` : name
  }

  async read<T>(name: string): Promise<Versioned<T> | undefined> {
    try {
      const res = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: this.key(name) }))
      return { value: JSON.parse(await res.Body!.transformToString()) as T, version: res.ETag! }
    } catch (e) {
      if ((e as { name?: string }).name === 'NoSuchKey') return undefined
      throw e
    }
  }

  async write<T>(name: string, value: T, version: string | null): Promise<string> {
    try {
      const res = await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: this.key(name),
          Body: serialize(value),
          ContentType: 'application/json',
          ...(version === null ? { IfNoneMatch: '*' } : { IfMatch: version }),
        })
      )
      return res.ETag!
    } catch (e) {
      const status = (e as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode
      // 412: the file changed, or exists; 409: another conditional write of it is in flight.
      if (status === 412 || status === 409) throw new Conflict(`${name} was written by another process`)
      throw e
    }
  }
}

const digest = (text: string) => createHash('sha256').update(text).digest('hex')

/**
 * A local directory, for development. The check and the write are not one atomic step, which is fine
 * for the single process a local lake has.
 */
class LocalStore implements Store {
  constructor(private readonly dir: string) {}

  async read<T>(name: string): Promise<Versioned<T> | undefined> {
    let text: string
    try {
      text = await readFile(join(this.dir, name), 'utf8')
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return undefined
      throw e
    }
    return { value: JSON.parse(text) as T, version: digest(text) }
  }

  async write<T>(name: string, value: T, version: string | null): Promise<string> {
    const current = await this.read(name)
    if ((current?.version ?? null) !== version) throw new Conflict(`${name} was written by another process`)
    const text = serialize(value)
    await mkdir(this.dir, { recursive: true })
    const temp = join(this.dir, `.${name}.${process.pid}.tmp`)
    await writeFile(temp, text)
    await rename(temp, join(this.dir, name))
    return digest(text)
  }
}
