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

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * A JSON-RPC client that sends calls in batches. Transport failures, 429s and 5xx answers are
 * retried with backoff; an error the node returns for a call is handed back to the caller, which
 * decides (a log range too wide, for instance, is split and asked again).
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
        if (res.status === 429 || res.status >= 500) {
          await res.body?.cancel()
          throw new Error(`HTTP ${res.status}`)
        }
        if (!res.ok) throw new RpcRejected(`HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`)
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
