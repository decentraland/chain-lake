import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createdAddress } from './discover'
import { loadConfig } from './config'

test('the created address is the low 20 bytes of its topic, lowercase', () => {
  const topic = '0x000000000000000000000000' + 'AbCdEf0123456789aBcDeF0123456789AbCdEf01'
  assert.equal(createdAddress(['0xtopic0', topic], 1), '0xabcdef0123456789abcdef0123456789abcdef01')
})

test('a missing or malformed topic is an error', () => {
  assert.throws(() => createdAddress(['0xtopic0'], 1))
  assert.throws(() => createdAddress(['0xtopic0', '0x1234'], 1))
})

test('every configured address is a lowercase address, and none repeats', () => {
  for (const dataset of ['ethereum-mainnet', 'polygon-mainnet', 'ethereum-sepolia', 'polygon-amoy-testnet']) {
    const config = loadConfig(dataset)
    const addresses = [...config.contracts, ...config.factories].map((c) => c.address)
    assert.equal(new Set(addresses).size, addresses.length, `${dataset} repeats an address`)
  }
})
