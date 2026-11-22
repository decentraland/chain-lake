import { existsSync } from 'fs'
import { join } from 'path'
import { loadConfig } from '../config'
import { readCoverage } from '../coverage'
import { MANIFEST_FILE } from '../manifest'
import { openStore } from '../store'
import { CoverageProvider, LakeReplaced } from './contracts'
import { LakeDataset } from './lake'
import { ChainView, NotReady } from './server'

/**
 * A lake served as it is, from its files, by `serve`. Its coverage is stated for the lake its coverage
 * record named when first asked, with that dataset's configured chain; asked again once the location
 * holds another lake, it answers 503 and calls `obsolete`, so the next request opens the location anew.
 */
export async function openStaticView(lakeDir: string, dataset: string, obsolete: () => void, options: StaticOptions = {}): Promise<ChainView> {
  const lake = await LakeDataset.open(lakeDir, dataset)
  // Nothing writes this lake in-process: a lake restored from an older copy is served at its height.
  lake.monotonic = false
  if (options.manifestTtlMs !== undefined) lake.manifestTtlMs = options.manifestTtlMs
  const store = openStore(lakeDir, dataset)
  let provider: CoverageProvider | undefined
  return {
    head: () => lake.head(),
    page: (query, from, to) => lake.page(query, from, to),
    async contracts() {
      if (!provider) {
        const lakeId = (await readCoverage(store))?.value.lakeId
        if (!lakeId) throw new NotReady(`${dataset}: the lake has no receipts to state its coverage by`)
        // Its answers are read from the manifest as stored, never from what the view caches for pages.
        provider = new CoverageProvider(store, { dataset, chainId: loadConfig(dataset).chainId, lakeId }, { static: true })
      }
      try {
        return await provider.contracts()
      } catch (e) {
        if (e instanceof LakeReplaced) obsolete()
        throw e
      }
    },
  }
}

/**
 * The views `serve` answers with, one per dataset the lake directory holds, opened on first request. A
 * view that failed to open, or that found its location holding another lake, is opened anew by the next
 * request; it drops only itself, so a late request of a view already replaced leaves the new one alone.
 */
export interface StaticOptions {
  /** How long a view reuses a manifest read for its pages; five seconds by default. */
  manifestTtlMs?: number
}

export function staticViews(lakeDir: string, options: StaticOptions = {}): (dataset: string) => Promise<ChainView> | undefined {
  const views = new Map<string, Promise<ChainView>>()
  return (dataset) => {
    if (!views.has(dataset)) {
      // Only the datasets the lake holds, so a name that matches nothing opens nothing.
      if (!existsSync(join(lakeDir, dataset, MANIFEST_FILE))) return undefined
      const forget = () => {
        if (views.get(dataset) === opened) views.delete(dataset)
      }
      const opened = openStaticView(lakeDir, dataset, forget, options)
      opened.catch(forget)
      views.set(dataset, opened)
    }
    return views.get(dataset)
  }
}
