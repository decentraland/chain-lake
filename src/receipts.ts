import { createHash, randomBytes } from 'crypto'
import { DatasetConfig } from './config'
import { Coverage, COVERAGE_FILE, readCoverage } from './coverage'
import { Manifest } from './manifest'
import { Conflict, Store } from './store'

/**
 * What each publication of the lake read, and with which filter: the evidence a reader needs before it
 * may say that the lake holds every log of an address over a range of blocks.
 *
 * - A **segment** is one interval read with one filter, every block of it.
 * - A **filter** is an immutable object named by its digest (`filter-<sha256>.json`): the addresses a
 *   read asked for. One that only grew is written as the addresses it added to the one before.
 * - A **receipt** is written once the publication carrying its data has won the manifest. It lists its
 *   segments, the block it ends at with the hash the read observed there, and the chunk folders it
 *   added or replaced.
 *
 * The receipts say nothing about the configured contracts or the registry: an address is held over a
 * segment only when that segment's own filter asked for it.
 */
export const SCANS_SCHEMA = 1

/** Each lake's receipts in a file of its own, so a writer of a lake rebuilt at the same location never touches the new one's. */
export const scansFile = (lakeId: string) => `scans-${lakeId}.json`

export const filterFile = (digest: string) => `filter-${digest}.json`

export type ScanSource = 'rpc' | 'sqd-portal'

export interface Segment {
  from: number
  to: number
  /** The digest of the filter the interval was read with. */
  filter: string
}

export interface Receipt {
  /** `commit`: a writer extended the lake; `absorb`: history merged into it at an unchanged height. */
  kind: 'commit' | 'absorb'
  source: ScanSource
  segments: Segment[]
  /** The block the publication ends at, and its hash as the read observed it. */
  to: number
  toHash: string
  /** The chunk folders the publication added. */
  wrote: string[]
  /** The folders it replaced, by the folder replacing each. */
  replaced: Record<string, string>
}

/** Which lake a receipt belongs to. */
export interface Identity {
  dataset: string
  chainId: number
  lakeId: string
}

export interface Ledger extends Identity {
  schemaVersion: number
  receipts: (Receipt & { id: string })[]
}

export interface FilterObject {
  parent?: string
  add: string[]
}

/** JSON with every object's keys sorted, so equal values have one text and one digest. */
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`
  }
  return JSON.stringify(value)
}

const sha256 = (text: string) => createHash('sha256').update(text).digest('hex')
export const digestOf = (value: unknown) => sha256(canonical(value))

/** A receipt's id: the digest of the receipt with the lake it belongs to, so distinct receipts never share one. */
export const receiptId = (identity: Identity, receipt: Receipt) => digestOf({ ...identity, ...receipt })

const sameLake = (a: Identity, b: Identity) => a.dataset === b.dataset && a.chainId === b.chainId && a.lakeId === b.lakeId

/** Writes a file that never changes once written; another process writing the same content is not a conflict. */
async function putImmutable(store: Store, name: string, value: unknown): Promise<void> {
  const present = async () => {
    const current = await store.read(name)
    if (!current) return false
    if (canonical(current.value) !== canonical(value)) throw new Error(`${name} holds other content than its name says`)
    return true
  }
  if (await present()) return
  try {
    await store.write(name, value, null)
  } catch (e) {
    if (!(e instanceof Conflict) || !(await present())) throw e
  }
}

/** The lake's identity for its receipts, giving it a `lakeId` the first time. */
export async function identityOf(store: Store, config: DatasetConfig, attempts = 10): Promise<Identity> {
  for (let attempt = 0; attempt < attempts; attempt++) {
    const current = await readCoverage(store)
    if (!current) throw new Error(`${config.dataset} has no ${COVERAGE_FILE}: its receipts would belong to no lake`)
    if (current.value.lakeId) return { dataset: config.dataset, chainId: config.chainId, lakeId: current.value.lakeId }
    try {
      await store.write<Coverage>(COVERAGE_FILE, { ...current.value, lakeId: randomBytes(16).toString('hex') }, current.version)
    } catch (e) {
      if (!(e instanceof Conflict)) throw e
    }
  }
  throw new Error(`${config.dataset}: ${COVERAGE_FILE} kept changing while naming the lake`)
}

/** Writes each filter a read used, once, and answers its digest. */
export class Filters {
  private last?: { digest: string; addresses: Set<string> }
  private readonly known = new Map<string, string>()

  constructor(private readonly store: Store) {}

  async digest(addresses: Iterable<string>): Promise<string> {
    const sorted = [...new Set([...addresses].map((a) => a.toLowerCase()))].sort()
    const key = sha256(sorted.join(','))
    const cached = this.known.get(key)
    if (cached) return cached
    const all = new Set(sorted)
    const last = this.last
    const grew = last !== undefined && [...last.addresses].every((a) => all.has(a))
    const object: FilterObject = grew ? { parent: last.digest, add: sorted.filter((a) => !last.addresses.has(a)) } : { add: sorted }
    const digest = digestOf(object)
    await putImmutable(this.store, filterFile(digest), object)
    this.last = { digest, addresses: all }
    this.known.set(key, digest)
    return digest
  }
}

/** One interval a source read, with the addresses it asked for. */
export interface Read {
  from: number
  to: number
  addresses: string[]
}

/**
 * The intervals a source read since the lake's last commit, each with the addresses it asked for, so the
 * commit's receipt can say what was read. Equal address sets are kept once.
 */
export class ScanLog {
  private reads: { from: number; to: number; key: string }[] = []
  private readonly sets = new Map<string, string[]>()

  private constructor(private readonly constant?: string[]) {}

  /** A log a source fills as it reads, as the follower does. */
  static reading(): ScanLog {
    return new ScanLog()
  }

  /** A source that reads every block of its range with one filter, as the backfill does. */
  static constant(addresses: string[]): ScanLog {
    return new ScanLog([...new Set(addresses.map((a) => a.toLowerCase()))].sort())
  }

  add(from: number, to: number, addresses: Iterable<string>): void {
    if (this.constant) throw new Error('a constant scan log takes no reads')
    const sorted = [...new Set([...addresses].map((a) => a.toLowerCase()))].sort()
    const key = sha256(sorted.join(','))
    if (!this.sets.has(key)) this.sets.set(key, sorted)
    this.reads.push({ from, to, key })
  }

  /**
   * The reads that cover [from, to], clipped to it, or undefined when a block of it was not read. The
   * reads that end by `to` are let go either way.
   */
  take(from: number, to: number): Read[] | undefined {
    if (this.constant) return [{ from, to, addresses: this.constant }]
    const out: Read[] = []
    let next = from
    for (const r of [...this.reads].sort((a, b) => a.from - b.from)) {
      if (next > to) break
      if (r.to < next) continue
      if (r.from > next) break
      const end = Math.min(r.to, to)
      out.push({ from: next, to: end, addresses: this.sets.get(r.key)! })
      next = end + 1
    }
    this.reads = this.reads.filter((r) => r.to > to)
    const kept = new Set(this.reads.map((r) => r.key))
    for (const key of [...this.sets.keys()]) if (!kept.has(key)) this.sets.delete(key)
    return next > to ? out : undefined
  }
}

/** Adjacent segments with the same filter, as one. */
export function mergeSegments(segments: Segment[]): Segment[] {
  const out: Segment[] = []
  for (const s of [...segments].sort((a, b) => a.from - b.from)) {
    const last = out[out.length - 1]
    if (last && last.filter === s.filter && last.to + 1 === s.from) last.to = s.to
    else out.push({ ...s })
  }
  return out
}

/** The segments of `reads`, with each filter written. */
export async function segmentsOf(filters: Filters, reads: Read[]): Promise<Segment[]> {
  const segments: Segment[] = []
  for (const r of reads) segments.push({ from: r.from, to: r.to, filter: await filters.digest(r.addresses) })
  return mergeSegments(segments)
}

/**
 * Adds a receipt to its lake's ledger, once: a ledger another process changed meanwhile is read again and
 * the receipt added to it. A ledger that names another dataset or chain, or a schema this build does not
 * know, is left alone and the receipt is not written.
 */
export async function appendReceipt(store: Store, identity: Identity, receipt: Receipt, attempts = 10): Promise<void> {
  const file = scansFile(identity.lakeId)
  const id = receiptId(identity, receipt)
  for (let attempt = 0; attempt < attempts; attempt++) {
    const current = await store.read<Ledger>(file)
    if (current && current.value.schemaVersion !== SCANS_SCHEMA) {
      throw new Error(`${identity.dataset}: ${file} has schema ${current.value.schemaVersion}, which this build does not write`)
    }
    if (current && !sameLake(current.value, identity)) {
      throw new Error(`${identity.dataset}: ${file} belongs to ${current.value.dataset} on chain ${current.value.chainId}, not to chain ${identity.chainId}`)
    }
    if (current && !Array.isArray(current.value.receipts)) throw new Error(`${identity.dataset}: ${file} holds no list of receipts`)
    const base: Ledger = current?.value ?? { schemaVersion: SCANS_SCHEMA, ...identity, receipts: [] }
    // Only an entry that verifies under this id is this receipt; one that does not parse is passed over.
    const present = base.receipts.some((raw) => {
      const parsed = parseReceipt(raw)
      return parsed !== undefined && isRecord(raw) && raw.id === id && receiptId(identity, parsed) === id
    })
    if (present) return
    try {
      await store.write<Ledger>(file, { ...base, receipts: [...base.receipts, { id, ...receipt }] }, current?.version ?? null)
      return
    } catch (e) {
      if (!(e instanceof Conflict)) throw e
    }
  }
  throw new Error(`${identity.dataset}: ${file} kept changing while recording a receipt`)
}

/** What a snapshot of the lake can prove: up to which block, and from which block for each address. */
export interface Claims {
  /** The highest block the claims reach, with its hash; undefined when nothing is proven. */
  finalized?: { number: number; hash: string }
  /** For each address proven, the first block from which the lake holds every log of it up to `finalized`. */
  from: Map<string, number>
}

const NONE: Claims = { from: new Map() }

const isRecord = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value)
const isBlock = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) >= 0

/** A stored receipt as it was written, or undefined when its JSON is not a receipt. */
function parseReceipt(raw: unknown): Receipt | undefined {
  if (!isRecord(raw)) return undefined
  const { kind, source, segments, to, toHash, wrote, replaced } = raw
  if ((kind !== 'commit' && kind !== 'absorb') || (source !== 'rpc' && source !== 'sqd-portal')) return undefined
  if (!isBlock(to) || typeof toHash !== 'string') return undefined
  if (!Array.isArray(wrote) || !wrote.every((dir) => typeof dir === 'string')) return undefined
  if (!isRecord(replaced) || !Object.values(replaced).every((dir) => typeof dir === 'string')) return undefined
  if (!Array.isArray(segments) || segments.length === 0) return undefined
  const parsed: Segment[] = []
  for (const s of segments) {
    if (!isRecord(s) || !isBlock(s.from) || !isBlock(s.to) || typeof s.filter !== 'string' || s.from > s.to || s.to > to) return undefined
    parsed.push({ from: s.from, to: s.to, filter: s.filter })
  }
  return { kind, source, segments: parsed, to, toHash, wrote: [...wrote] as string[], replaced: { ...(replaced as Record<string, string>) } }
}

/** The ledger's receipts whose JSON parses and whose id is the digest of what they say, for the lake expected. */
function verifiedReceipts(ledger: Ledger, expected: Identity): Receipt[] {
  return ledger.receipts.flatMap((raw) => {
    const receipt = parseReceipt(raw)
    return receipt && isRecord(raw) && raw.id === receiptId(expected, receipt) ? [receipt] : []
  })
}

/**
 * What `snapshot` proves from `ledger`. A receipt counts for the snapshot only if the snapshot holds its
 * publication:
 *
 * - a commit, if the snapshot reached its end: the manifest is one chain of compare-and-swap writes and a
 *   commit moves its height from the block before its first segment to its end at once, so a snapshot at
 *   or above that end descends from it, and a lake only ever gains data;
 * - an absorb that wrote chunks, if every chunk it wrote is in the snapshot, or replaced by one that is
 *   (as a later absorb records); one that wrote none, if the snapshot reached its end, since its read
 *   found nothing to add on the branch the lake holds.
 *
 * An address is proven from the first block of the longest run of blocks ending at the highest proven
 * block that the segments asking for it cover without a gap. A ledger of another lake or schema, a
 * receipt that does not parse or whose id is not the digest of what it says, and a filter that is
 * missing or does not match its digest prove nothing.
 */
export async function claims(
  snapshot: Manifest,
  ledger: Ledger | undefined,
  expected: Identity,
  loadFilter: (digest: string) => Promise<FilterObject | undefined>
): Promise<Claims> {
  if (!isRecord(ledger) || ledger.schemaVersion !== SCANS_SCHEMA || !sameLake(ledger, expected) || !Array.isArray(ledger.receipts)) return NONE
  // Only a receipt whose id matches what it says feeds the replacements and the claims.
  const receipts = verifiedReceipts(ledger, expected)

  const listed = new Set(snapshot.chunks.map((c) => c.dir))
  const replacedBy = new Map<string, string[]>()
  for (const r of receipts) {
    if (r.kind !== 'absorb') continue
    for (const [old, next] of Object.entries(r.replaced)) replacedBy.set(old, [...(replacedBy.get(old) ?? []), next])
  }
  const presence = new Map<string, boolean>()
  const present = (dir: string, seen = new Set<string>()): boolean => {
    const known = presence.get(dir)
    if (known !== undefined) return known
    if (seen.has(dir)) return false
    seen.add(dir)
    const result = listed.has(dir) || (replacedBy.get(dir) ?? []).some((next) => present(next, seen))
    presence.set(dir, result)
    return result
  }
  const holds = (r: Receipt) => {
    if (r.to > snapshot.height) return false
    if (r.to === snapshot.height && r.toHash !== snapshot.hash) return false
    if (r.kind === 'commit') return true
    if (r.kind === 'absorb') return r.wrote.every((dir) => present(dir))
    return false
  }
  const held = receipts.filter(holds)
  if (!held.length) return NONE
  const top = Math.max(...held.map((r) => r.to))
  const finalized = { number: top, hash: top === snapshot.height ? snapshot.hash : held.find((r) => r.to === top)!.toHash }

  // The filters, with their ancestors; a filter that cannot be read drops the segments that name it.
  const objects = new Map<string, FilterObject | null>()
  const load = async (digest: string): Promise<FilterObject | null> => {
    if (objects.has(digest)) return objects.get(digest)!
    objects.set(digest, null)
    const object = await loadFilter(digest).catch(() => undefined)
    if (!object || !Array.isArray(object.add) || digestOf(object) !== digest) return null
    if (object.parent !== undefined && !(await load(object.parent))) return null
    objects.set(digest, object)
    return object
  }
  const intervals = new Map<string, [number, number][]>()
  for (const r of held) {
    for (const s of r.segments) {
      if (!(await load(s.filter))) continue
      intervals.set(s.filter, [...(intervals.get(s.filter) ?? []), [s.from, s.to]])
    }
  }

  // Each filter's segments together with those of the filters that descend from it, which ask for
  // every address it asks for.
  const children = new Map<string, string[]>()
  for (const [digest, object] of objects) {
    if (object?.parent !== undefined) children.set(object.parent, [...(children.get(object.parent) ?? []), digest])
  }
  const reach = new Map<string, [number, number][]>()
  const reachOf = (digest: string): [number, number][] => {
    const known = reach.get(digest)
    if (known) return known
    const all = [...(intervals.get(digest) ?? [])]
    for (const child of children.get(digest) ?? []) all.push(...reachOf(child))
    const merged = mergeIntervals(all)
    reach.set(digest, merged)
    return merged
  }

  // An address is asked for by every filter that adds it and by their descendants.
  const addedIn = new Map<string, string[]>()
  for (const [digest, object] of objects) {
    if (!object) continue
    for (const address of object.add) addedIn.set(address, [...(addedIn.get(address) ?? []), digest])
  }
  const startOf = new Map<string, number | undefined>()
  const from = new Map<string, number>()
  for (const [address, filters] of addedIn) {
    const key = filters.sort().join(',')
    if (!startOf.has(key)) startOf.set(key, suffixStart(mergeIntervals(filters.flatMap(reachOf)), top))
    const start = startOf.get(key)
    if (start !== undefined) from.set(address, start)
  }
  return { finalized, from }
}

function mergeIntervals(intervals: [number, number][]): [number, number][] {
  const out: [number, number][] = []
  for (const [a, b] of [...intervals].sort((x, y) => x[0] - y[0])) {
    const last = out[out.length - 1]
    if (last && a <= last[1] + 1) last[1] = Math.max(last[1], b)
    else out.push([a, b])
  }
  return out
}

/** The first block of the merged interval that ends at `top`, if one does. */
function suffixStart(merged: [number, number][], top: number): number | undefined {
  const last = merged[merged.length - 1]
  return last && last[1] === top ? last[0] : undefined
}
