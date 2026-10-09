import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import { datasetsFromEnv } from './config'

afterEach(() => {
  delete process.env.DATASETS
  delete process.env.DATASET
})

test('DATASETS names several datasets', () => {
  process.env.DATASETS = 'ethereum-mainnet, polygon-mainnet'
  assert.deepEqual(datasetsFromEnv(), ['ethereum-mainnet', 'polygon-mainnet'])
})

test('DATASET names one', () => {
  process.env.DATASET = 'ethereum-sepolia'
  assert.deepEqual(datasetsFromEnv(), ['ethereum-sepolia'])
})

test('a dataset named twice, or none, is refused', () => {
  process.env.DATASETS = 'polygon-mainnet,polygon-mainnet'
  assert.throws(() => datasetsFromEnv(), /twice/)
  process.env.DATASETS = ' , '
  assert.throws(() => datasetsFromEnv(), /names no dataset/)
  delete process.env.DATASETS
  assert.throws(() => datasetsFromEnv(), /DATASETS is required/)
})
