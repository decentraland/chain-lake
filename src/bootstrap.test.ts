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

const config: DatasetConfig = { dataset: 'polygon-mainnet', fromBlock: 0, contracts: [{ name: 'Static', address: '0x' + 'aa'.repeat(20) }], factories: [] }

/** A store in a fresh directory, and stand-ins for discover and backfill that record their runs. */
async function setup(t: TestContext, prepare: (store: Store) => Promise<void>, backfillTo = 100) {
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
    }
  }
  await prepare(store)
  return { store, ran, go: () => bootstrap(store, config, scripts as never) }
}

test('a new dataset is discovered, backfilled and recorded as complete', async (t) => {
  const { store, ran, go } = await setup(t, async () => {})
  await go()
  assert.deepEqual(ran, ['discover.js', 'backfill.js'])
  assert.deepEqual((await store.read<Coverage>(COVERAGE_FILE))!.value.complete, { height: 100 })

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
