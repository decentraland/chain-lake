import assert from 'node:assert/strict'
import { test } from 'node:test'
import { oneAtATime, scriptArgs } from './scripts'

test('absorb runs with a bounded heap, and the other commands as they are', () => {
  assert.deepEqual(scriptArgs('absorb.js', {}), ['--max-old-space-size=768'])
  assert.deepEqual(scriptArgs('absorb.js', { ABSORB_HEAP_MB: '512' }), ['--max-old-space-size=512'])
  assert.deepEqual(scriptArgs('backfill.js', {}), [])
})

test('runs of one queue never overlap, and a failed run does not stop the next', async () => {
  const queue = oneAtATime()
  const events: string[] = []
  const run = (name: string, fail = false) => async () => {
    events.push(`${name} start`)
    await new Promise((r) => setTimeout(r, 5))
    events.push(`${name} end`)
    if (fail) throw new Error(name)
  }
  const results = await Promise.allSettled([queue(run('a', true)), queue(run('b'))])
  assert.deepEqual(events, ['a start', 'a end', 'b start', 'b end'])
  assert.deepEqual(results.map((r) => r.status), ['rejected', 'fulfilled'])
})
