import { DatasetConfig } from './config'
import { configuredContracts } from './discover'
import { Store, Versioned } from './store'

/** What a dataset's lake holds the history of, and how it came to hold it. */
export const COVERAGE_FILE = 'coverage.json'

export interface Coverage {
  /** The configured contracts and factories whose whole history the lake holds, or will once backfilled. */
  contracts: string[]
  /** Set once the backfill reached `height`; the follower took over from there. */
  complete?: { height: number }
  /** Set by a development run that left history out. Such a lake is never served. */
  partial?: string
}

export function coverageOf(config: DatasetConfig): Coverage {
  return { contracts: configuredContracts(config).map((c) => c.address).sort() }
}

export function readCoverage(store: Store): Promise<Versioned<Coverage> | undefined> {
  return store.read<Coverage>(COVERAGE_FILE)
}

/**
 * Throws unless the lake holds, or is being backfilled with, the whole history of every contract the
 * config follows. A contract added to the config afterwards would be served as if it had no history.
 */
export function checkCoverage(config: DatasetConfig, coverage: Coverage): void {
  if (coverage.partial) throw new Error(`${config.dataset}: ${coverage.partial}; a lake missing history is never served`)
  const covered = new Set(coverage.contracts)
  const missing = configuredContracts(config).filter((c) => !covered.has(c.address))
  if (missing.length) {
    throw new Error(`${config.dataset}: ${missing.map((c) => `${c.name} (${c.address})`).join(', ')} joined the config after the backfill started, and the lake holds none of their earlier history`)
  }
}

/** Records that a development run left history out of the dataset. */
export async function markPartial(store: Store, config: DatasetConfig, why: string): Promise<void> {
  const current = await readCoverage(store)
  await store.write<Coverage>(COVERAGE_FILE, { ...(current?.value ?? coverageOf(config)), partial: why }, current?.version ?? null)
}
