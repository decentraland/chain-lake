import { readCoverage } from '../coverage'
import { Manifest, MANIFEST_FILE } from '../manifest'
import { claims, digestOf, FilterObject, filterFile, Identity, Ledger, scansFile } from '../receipts'
import { NotJson, Store } from '../store'
import { LakeDataset, revisionOf } from './lake'
import { NotReady } from './server'

/** `GET /datasets/<dataset>/contracts`: from which block the lake holds every log of each address. */
export interface ContractsAnswer {
  schemaVersion: 1
  dataset: string
  chainId: number
  /** The revision of the manifest the answer was computed from: pages read from it carry the same one. */
  coverageRevision: string
  /** The highest block the answer reaches, inclusive; null when no receipt counts for the manifest. */
  finalized: { number: number; hash: string } | null
  /** Each address proven, with the first block (inclusive) from which every log of it up to `finalized` is held. */
  contracts: { address: string; from: number }[]
}

/** The lake at the location is not the one the provider was bound to: whatever serves it is obsolete. */
export class LakeReplaced extends NotReady {}

/**
 * The coverage answer of one lake, bound to its identity. Every answer is computed from one manifest,
 * after the coverage record shows that same lake complete, and returned only if it still does once
 * computed: a lake rebuilt or marked partial meanwhile gets a 503, never an answer from the other one.
 * Its caches (the ledger by the version it was read at, the filters once verified, the last answer) all
 * belong to that one lake.
 */
export class CoverageProvider {
  private ledger?: { version: string; value: Ledger }
  private readonly filters = new Map<string, FilterObject>()
  private last?: { key: string; answer: ContractsAnswer }

  /**
   * `live`: the manifest its view serves (`lake.snapshot()`, the one its writer installs) and the block
   * it serves the files up to, which an answer waits for, so it never reaches past them. `static`: the
   * manifest as stored, read afresh with its version for every answer and confirmed unchanged once it is
   * computed, whatever the view caches for its pages.
   */
  constructor(
    private readonly store: Store,
    readonly identity: Identity,
    private readonly mode: { live: LakeDataset; files: () => { height: number; hash: string } } | { static: true }
  ) {}

  private async manifest(): Promise<{ snapshot: Manifest; version?: string }> {
    if ('live' in this.mode) return { snapshot: await this.mode.live.snapshot() }
    const read = await this.store.read<Manifest>(MANIFEST_FILE)
    if (!read) throw new NotReady(`${this.identity.dataset}: the lake has no manifest`)
    return { snapshot: read.value, version: read.version }
  }

  async contracts(): Promise<ContractsAnswer> {
    await this.gate()
    const { snapshot, version } = await this.manifest()
    this.checkFiles(snapshot)
    const ledger = await this.readLedger()
    const key = `${revisionOf(snapshot)}:${ledger?.version ?? ''}`
    let answer = this.last?.key === key ? this.last.answer : undefined
    if (!answer) {
      const proven = await claims(snapshot, ledger?.value, this.identity, (digest) => this.filter(digest))
      if (proven.unavailable) throw new NotReady(`${this.identity.dataset}: a filter could not be read`)
      answer = {
        schemaVersion: 1,
        dataset: this.identity.dataset,
        chainId: this.identity.chainId,
        coverageRevision: revisionOf(snapshot),
        finalized: proven.finalized ?? null,
        contracts: [...proven.from].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([address, from]) => ({ address, from })),
      }
      this.last = { key, answer }
    }
    // The lake may have changed while the answer was computed: it holds only if nothing did.
    await this.gate()
    this.checkFiles(snapshot)
    if (version !== undefined && (await this.store.version(MANIFEST_FILE)) !== version) {
      throw new NotReady(`${this.identity.dataset}: the manifest changed while the answer was computed`)
    }
    return answer
  }

  private async gate(): Promise<void> {
    const coverage = (await readCoverage(this.store))?.value
    if (!coverage || coverage.lakeId !== this.identity.lakeId) throw new LakeReplaced(`${this.identity.dataset}: the lake at this location is not the one this view serves`)
    if (!coverage.complete || coverage.partial) throw new NotReady(`${this.identity.dataset}: the lake's history is not complete`)
  }

  private checkFiles(snapshot: Manifest): void {
    if (!('live' in this.mode)) return
    const files = this.mode.files()
    if (files.height !== snapshot.height || files.hash !== snapshot.hash) {
      throw new NotReady(`${this.identity.dataset}: the manifest is at block ${snapshot.height}, and this replica serves the lake's files up to ${files.height}`)
    }
  }

  /** The ledger, read again only when its version changed, and kept under the version it was read at. */
  private async readLedger(): Promise<{ version: string; value: Ledger } | undefined> {
    const name = scansFile(this.identity.lakeId)
    const version = await this.store.version(name)
    if (version === undefined) return (this.ledger = undefined)
    if (this.ledger?.version === version) return this.ledger
    const read = await this.store.read<Ledger>(name)
    return (this.ledger = read && { version: read.version, value: read.value })
  }

  /**
   * A filter object, kept once it matches its name; a read that fails is not kept, and is tried again.
   * A stored object that is not JSON proves nothing, as one that does not match its name: it is written
   * whole before any receipt names it, so reading it again would not change it.
   */
  private async filter(digest: string): Promise<FilterObject | undefined> {
    const known = this.filters.get(digest)
    if (known) return known
    let object: FilterObject | undefined
    try {
      object = (await this.store.read<FilterObject>(filterFile(digest)))?.value
    } catch (e) {
      if (e instanceof NotJson) return undefined
      throw e
    }
    if (object && Array.isArray(object.add) && digestOf(object) === digest) this.filters.set(digest, object)
    return object
  }
}
