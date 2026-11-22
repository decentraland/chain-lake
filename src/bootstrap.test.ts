import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { test, TestContext } from 'node:test'
import { bootstrap } from './bootstrap'
import { DatasetConfig } from './config'
import { Coverage, COVERAGE_FILE, coverageOf } from './coverage'
import { REGISTRY_FILE } from './discover'
import { MANIFEST_FILE } from './manifest'
import { openStore, Store } from './store'

const config: DatasetConfig = { dataset: 'polygon-mainnet', chainId: 137, fromBlock: 0, contracts: [{ name: 'Static', address: '0x' + 'aa'.repeat(20) }], factories: [] }

/** A store in a fresh directory, and stand-ins for discover and backfill that record their runs. */
async function setup(t: TestContext, prepare: (store: Store) => Promise<void>, backfillTo = 100, duringBackfill?: (store: Store) => Promise<void>) {
  const lakeDir = mkdtempSync(join(tmpdir(), 'chain-lake-'))
  t.after(() => rmSync(lakeDir, { recursive: true, force: true }))
  const store = openStore(lakeDir, config.dataset)
  const ran: string[] = []
  const scripts = async (script: string) => {
    ran.push(script)
    if (script === 'discover.js') {
      if (!(await store.read(COVERAGE_FILE))) await store.write(COVERAGE_FILE, coverageOf(config), null)
      await store.write(REGISTRY_FILE, { dataset: config.dataset, height: 100, contracts: [] }, null)
    } else {
      const current = await store.read(MANIFEST_FILE)
      await store.write(MANIFEST_FILE, { height: backfillTo, hash: '0x', chunks: [] }, current?.version ?? null)
      // The backfill names the lake for its receipts, as `identityOf` does.
      const coverage = (await store.read<Coverage>(COVERAGE_FILE))!
      if (!coverage.value.lakeId) await store.write<Coverage>(COVERAGE_FILE, { ...coverage.value, lakeId: 'named by the backfill' }, coverage.version)
      await duringBackfill?.(store)
    }
  }
  await prepare(store)
  return { store, ran, go: () => bootstrap(store, config, scripts as never) }
}

test('a new dataset is discovered, backfilled and recorded as complete', async (t) => {
  const { store, ran, go } = await setup(t, async () => {})
  await go()
  assert.deepEqual(ran, ['discover.js', 'backfill.js'])
  const coverage = (await store.read<Coverage>(COVERAGE_FILE))!.value
  assert.deepEqual(coverage.complete, { height: 100 })
  assert.equal(coverage.lakeId, 'named by the backfill', 'what the backfill wrote is kept')

  // From then on, nothing runs.
  ran.length = 0
  await go()
  assert.deepEqual(ran, [])
})

test('an interrupted backfill resumes, without discovering again', async (t) => {
  const { ran, go } = await setup(t, async (store) => {
    await store.write(COVERAGE_FILE, coverageOf(config), null)
    await store.write(REGISTRY_FILE, { dataset: config.dataset, height: 100, contracts: [] }, null)
    await store.write(MANIFEST_FILE, { height: 50, hash: '0x', chunks: [] }, null)
  })
  await go()
  assert.deepEqual(ran, ['backfill.js'])
})

test('a lake with no coverage record is refused, whatever it holds', async (t) => {
  const { ran, go } = await setup(t, async (store) => {
    await store.write(REGISTRY_FILE, { dataset: config.dataset, height: 100, contracts: [] }, null)
    await store.write(MANIFEST_FILE, { height: 100, hash: '0x', chunks: [] }, null)
  })
  await assert.rejects(go(), /no coverage\.json, so what it holds is unknown/)
  assert.deepEqual(ran, [])
})

test('a backfill that stops short is not recorded as complete', async (t) => {
  const { store, go } = await setup(t, async () => {}, 80)
  await assert.rejects(go(), /ended at block 80, short of 100/)
  assert.equal((await store.read<Coverage>(COVERAGE_FILE))!.value.complete, undefined)
})

test('a lake missing history is refused', async (t) => {
  const { go } = await setup(t, async (store) => {
    await store.write(COVERAGE_FILE, { ...coverageOf(config), partial: 'a development backfill left history out' }, null)
  })
  await assert.rejects(go(), /never served/)
})

test('a lake the backfill marked partial is refused, with its marker and name kept and nothing recorded complete', async (t) => {
  const { store, go } = await setup(t, async () => {}, 100, async (store) => {
    const coverage = (await store.read<Coverage>(COVERAGE_FILE))!
    await store.write<Coverage>(COVERAGE_FILE, { ...coverage.value, partial: 'a development backfill left history out' }, coverage.version)
  })
  await assert.rejects(go(), /never served/)
  const coverage = (await store.read<Coverage>(COVERAGE_FILE))!.value
  assert.equal(coverage.partial, 'a development backfill left history out')
  assert.equal(coverage.lakeId, 'named by the backfill')
  assert.equal(coverage.complete, undefined)
})
