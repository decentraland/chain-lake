/**
 * The Node options a command runs with. `absorb` runs beside the portal, in the same task, so its heap is
 * bounded: it stages the history it reads on disk, and holds only the ranges being read.
 */
export function scriptArgs(script: string, env: NodeJS.ProcessEnv): string[] {
  return script === 'absorb.js' ? [`--max-old-space-size=${Number(env.ABSORB_HEAP_MB || 768)}`] : []
}

/**
 * Runs `run` once every earlier run of the same queue has ended, whether it succeeded or failed: the
 * absorbs of the task's datasets run one at a time, each with a heap and a DuckDB of its own.
 */
export function oneAtATime(): <T>(run: () => Promise<T>) => Promise<T> {
  let last: Promise<unknown> = Promise.resolve()
  return (run) => {
    const next = last.then(run)
    last = next.catch(() => undefined)
    return next
  }
}
