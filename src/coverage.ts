import { Dest } from '@subsquid/file-store'
import { DatasetConfig } from './config'
import { configuredContracts } from './discover'

/** Marks a dataset that a development run wrote with history left out. `live` never serves it. */
export const PARTIAL_FILE = 'partial.txt'
/** Written once a dataset's backfill is complete: what it covered. */
export const BACKFILL_FILE = 'backfill.json'

export interface Coverage {
  /** The block the backfill reached; the follower continues from there. */
  height: number
  /** The configured contracts and factories whose history the backfill holds. */
  contracts: string[]
}

export function markPartial(root: Dest, why: string): Promise<void> {
  return root.writeFile(PARTIAL_FILE, why + '\n')
}

export function coverageOf(config: DatasetConfig, height: number): Coverage {
  return { height, contracts: configuredContracts(config).map((c) => c.address).sort() }
}

/**
 * Throws unless the lake holds the history of every contract the config follows. A contract added
 * to the config after the backfill would otherwise be served with no history, as if it had none.
 */
export function checkCoverage(config: DatasetConfig, coverage: Coverage): void {
  const covered = new Set(coverage.contracts)
  const missing = configuredContracts(config).filter((c) => !covered.has(c.address))
  if (missing.length) {
    throw new Error(`${config.dataset}: ${missing.map((c) => `${c.name} (${c.address})`).join(', ')} joined the config after the backfill, and the lake holds none of their history`)
  }
}
