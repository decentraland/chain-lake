import { Logger } from '@subsquid/logger'
import { DatasetConfig } from './config'
import { missingContracts } from './absorb'
import { checkCoverage, COVERAGE_FILE, readCoverage } from './coverage'
import { Registry, REGISTRY_FILE } from './discover'
import { Manifest, MANIFEST_FILE } from './manifest'
import { Store } from './store'

/**
 * Makes a dataset ready to follow. One whose backfill is not complete gets it first, from the SQD
 * portal: `discover` when it has no contracts yet, then `backfill` up to the height they were
 * discovered at. An interrupted backfill resumes from the lake's manifest. Once complete, the coverage
 * record says so, and from then on the dataset never needs the SQD portal again.
 *
 * A complete lake whose config gained contracts takes them with their whole history first, over RPC
 * (`absorb`), so the SQD portal is not needed for them either.
 *
 * Only lakes these commands started are adopted: one with no coverage record holds history nobody
 * can vouch for, and is refused, as is one that misses history (see `checkCoverage`).
 */
export async function bootstrap(
  store: Store,
  config: DatasetConfig,
  runScript: (script: 'discover.js' | 'backfill.js' | 'absorb.js') => Promise<void>,
  log?: Logger
): Promise<void> {
  let coverage = await readCoverage(store)
  if (!coverage && ((await store.read(REGISTRY_FILE)) || (await store.read(MANIFEST_FILE)))) {
    throw new Error(`${config.dataset}: the lake has no ${COVERAGE_FILE}, so what it holds is unknown; start it over in an empty location`)
  }
  if (coverage?.value.complete && !coverage.value.partial) {
    const missing = missingContracts(config, coverage.value)
    if (missing.length) {
      log?.info(`the config gained ${missing.map((c) => c.name).join(', ')}: absorbing their history over RPC`)
      await runScript('absorb.js')
      coverage = await readCoverage(store)
    }
  }
  if (coverage) checkCoverage(config, coverage.value)
  if (coverage?.value.complete) return

  if (!coverage || !(await store.read(REGISTRY_FILE))) {
    log?.info('no lake yet: discovering the contracts the factories created, from the SQD portal')
    await runScript('discover.js')
    coverage = await readCoverage(store)
    if (!coverage) throw new Error(`${config.dataset}: discover recorded no coverage`)
  }

  const registry = (await store.read<Registry>(REGISTRY_FILE))!.value
  const written = async () => (await store.read<Manifest>(MANIFEST_FILE))?.value.height ?? -1
  if ((await written()) < registry.height) {
    log?.info(`backfilling from the SQD portal: the lake is at block ${await written()}, contracts are known up to ${registry.height}`)
    await runScript('backfill.js')
  }
  const reached = await written()
  if (reached < registry.height) throw new Error(`${config.dataset}: the backfill ended at block ${reached}, short of ${registry.height}`)
  await store.write(COVERAGE_FILE, { ...coverage.value, complete: { height: registry.height } }, coverage.version)
  log?.info(`backfill complete up to block ${registry.height}`)
}
