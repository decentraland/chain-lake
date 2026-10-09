import assert from 'node:assert/strict'
import { test } from 'node:test'
import { DatasetConfig } from './config'
import { checkCoverage, coverageOf } from './coverage'

const A = '0x' + 'aa'.repeat(20)
const B = '0x' + 'bb'.repeat(20)
const F = '0x' + 'fa'.repeat(20)

const config = (addresses: string[]): DatasetConfig => ({
  dataset: 'polygon-mainnet',
  fromBlock: 0,
  contracts: addresses.map((address, i) => ({ name: `Contract${i}`, address })),
  factories: [{ name: 'Factory', address: F, fromBlock: 0, topic0: '0x' + 'cf'.repeat(32), addressTopic: 1 }],
})

test('a dataset records the configured contracts and factories whose history it holds', () => {
  assert.deepEqual(coverageOf(config([B, A])), { contracts: [A, B, F] })
})

test('a contract added to the config after the backfill started stops the dataset from being served', () => {
  const covered = coverageOf(config([A]))
  assert.doesNotThrow(() => checkCoverage(config([A]), covered))
  assert.throws(() => checkCoverage(config([A, B]), covered), /Contract1 .* joined the config after the backfill started/)
})

test('a lake a development run left history out of is never served', () => {
  assert.throws(() => checkCoverage(config([A]), { ...coverageOf(config([A])), partial: 'a development backfill left history out' }), /never served/)
})
