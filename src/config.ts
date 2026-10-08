import { readFileSync } from 'fs'
import { join } from 'path'

export interface Contract {
  name: string
  address: string
}

/** A contract that creates others; the address of each new one is read from a log topic. */
export interface Factory extends Contract {
  fromBlock: number
  topic0: string
  addressTopic: number
}

export interface DatasetConfig {
  /** The SQD portal dataset, e.g. `ethereum-mainnet`. */
  dataset: string
  fromBlock: number
  contracts: Contract[]
  factories: Factory[]
}

/** The configuration of the dataset named by `DATASET`, from `config/<dataset>.json`. */
export function loadConfig(dataset = required('DATASET')): DatasetConfig {
  const file = join(__dirname, '..', 'config', `${dataset}.json`)
  const config = JSON.parse(readFileSync(file, 'utf8')) as DatasetConfig
  for (const c of [...config.contracts, ...config.factories]) {
    if (!/^0x[0-9a-f]{40}$/.test(c.address)) throw new Error(`${c.name}: ${c.address} is not a lowercase address`)
  }
  return config
}

/** The datasets named by `DATASETS`, comma-separated, or the one named by `DATASET`. */
export function datasetsFromEnv(): string[] {
  const value = process.env.DATASETS || process.env.DATASET
  if (!value) throw new Error('DATASETS is required')
  const datasets = value.split(',').map((d) => d.trim()).filter(Boolean)
  if (!datasets.length) throw new Error(`DATASETS names no dataset: "${value}"`)
  if (new Set(datasets).size !== datasets.length) throw new Error(`DATASETS names a dataset twice: "${value}"`)
  return datasets
}

export function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

/** A block number from the environment, or undefined. */
export function blockFromEnv(name: string): number | undefined {
  const value = process.env[name]
  if (!value) return undefined
  const block = Number(value)
  if (!Number.isSafeInteger(block) || block < 0) throw new Error(`${name} must be a block number, got "${value}"`)
  return block
}
