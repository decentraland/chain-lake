/**
 * Trouble with the node rather than with this program: errors it returns, failures to reach it, and
 * answers that do not fit together. A follower waits and asks again; anything else is a bug.
 */
export class RpcTrouble extends Error {}

/** A JSON-RPC error the node returned for one call (as opposed to a transport failure). */
export class RpcError extends RpcTrouble {
  constructor(readonly code: number, message: string) {
    super(message)
  }
}

/** The node could not be reached, or kept failing, through every retry of the client. */
export class RpcUnavailable extends RpcTrouble {}

/**
 * The node refused the request itself: a bad key, a forbidden method, a batch too large. Asking
 * again changes nothing, so it is not RPC trouble to wait out; it ends the run, with this message.
 */
export class RpcRejected extends Error {}

export interface RpcCall {
  method: string
  params: unknown[]
}

const RETRIES = 8
/** Longer than any answer of a healthy node; a hung connection is dropped and retried. */
const TIMEOUT_MS = 60_000

/**
 * The statuses that say the request itself is wrong: a bad key, a wrong URL or method, a batch too
 * large. Any other failing status is the node, or a proxy in front of it, having trouble (a 409 or 408
 * page from a CDN, a 429, a 5xx), and is asked again.
 */
const REFUSALS = new Set([400, 401, 403, 404, 405, 413, 414, 431])

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * A JSON-RPC client that sends calls in batches. Transport failures and failing statuses other than
 * a refusal (see `REFUSALS`) are retried with backoff; an error the node returns for a call is handed
 * back to the caller, which decides (a log range too wide, for instance, is split and asked again).
 */
export class RpcClient {
  constructor(readonly url: string, private readonly batchSize = 50) {}

  async call<T>(method: string, params: unknown[]): Promise<T> {
    const [result] = await this.batch<T>([{ method, params }])
    if (result instanceof RpcError) throw result
    return result
  }

  /** Results in call order; an RpcError in place of each call the node rejected. */
  async batch<T>(calls: RpcCall[]): Promise<(T | RpcError)[]> {
    const results: (T | RpcError)[] = []
    for (let i = 0; i < calls.length; i += this.batchSize) {
      results.push(...(await this.send<T>(calls.slice(i, i + this.batchSize))))
    }
    return results
  }

  private async send<T>(calls: RpcCall[]): Promise<(T | RpcError)[]> {
    const body = JSON.stringify(calls.map((c, id) => ({ jsonrpc: '2.0', id, method: c.method, params: c.params })))
    for (let attempt = 0; ; attempt++) {
      try {
        const res = await fetch(this.url, { method: 'POST', headers: { 'content-type': 'application/json' }, body, signal: AbortSignal.timeout(TIMEOUT_MS) })
        if (!res.ok) {
          const text = (await res.text().catch(() => '')).slice(0, 200)
          if (REFUSALS.has(res.status)) throw new RpcRejected(`HTTP ${res.status}: ${text}`)
          throw new Error(`HTTP ${res.status}: ${text}`)
        }
        const answers = (await res.json()) as { id: number; result?: T; error?: { code: number; message: string } }[] | { error?: { message: string } }
        if (!Array.isArray(answers)) {
          if (answers?.error) throw new RpcRejected(`the node rejected a batch of ${calls.length} calls: ${answers.error.message}`)
          throw new Error(`unexpected answer: ${JSON.stringify(answers).slice(0, 200)}`)
        }
        const byId = new Map(answers.map((a) => [a.id, a]))
        return calls.map((_, id) => {
          const a = byId.get(id)
          if (!a) return new RpcError(-1, 'no answer for this call')
          return a.error ? new RpcError(a.error.code, a.error.message) : (a.result as T)
        })
      } catch (e) {
        if (e instanceof RpcRejected) throw e
        if (attempt >= RETRIES) throw new RpcUnavailable(`no answer after ${attempt + 1} attempts: ${e instanceof Error ? e.message : e}`)
        await wait(Math.min(30_000, 500 * 2 ** attempt))
      }
    }
  }
}

/**
 * Runs `f` until the node answers. RPC trouble is waited out, with a backoff of up to a minute, for as
 * long as it lasts: an outage only stalls the caller. Anything else is thrown.
 */
export async function untilAnswered<T>(what: string, f: () => Promise<T>, log: { warn(fields: object, message: string): void }): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await f()
    } catch (e) {
      if (!(e instanceof RpcTrouble)) throw e
      const delay = Math.min(60_000, 1000 * 2 ** Math.min(attempt, 6))
      log.warn({ err: e }, `${what} failed (attempt ${attempt}); retrying in ${delay / 1000} s`)
      await wait(delay)
    }
  }
}
