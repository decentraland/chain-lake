import assert from 'node:assert/strict'
import { createServer, Server } from 'http'
import { AddressInfo } from 'net'
import { test } from 'node:test'
import { RpcClient, RpcRejected } from './rpc'

/** A node that answers each request with the next of `answers`: a status and a body. */
async function node(answers: [number, unknown][]): Promise<{ url: string; server: Server; requests: () => number }> {
  let n = 0
  const server = createServer((req, res) => {
    req.resume()
    req.on('end', () => {
      const [status, body] = answers[Math.min(n++, answers.length - 1)]
      res.writeHead(status, { 'content-type': 'application/json' })
      res.end(typeof body === 'string' ? body : JSON.stringify(body))
    })
  })
  await new Promise<void>((resolve) => server.listen(0, resolve))
  return { url: `http://localhost:${(server.address() as AddressInfo).port}`, server, requests: () => n }
}

test('a rate limit is waited out and asked again', async () => {
  const { url, server, requests } = await node([
    [429, 'slow down'],
    [200, [{ id: 0, result: '0x10' }]],
  ])
  try {
    assert.equal(await new RpcClient(url).call('eth_blockNumber', []), '0x10')
    assert.equal(requests(), 2)
  } finally {
    server.close()
  }
})

test('a refusal is final: a bad key, or a batch rejected as a whole', async () => {
  for (const [status, body] of [
    [401, '<html>unauthorized</html>'],
    [200, { jsonrpc: '2.0', id: null, error: { code: -32600, message: 'batch too large' } }],
  ] as [number, unknown][]) {
    const { url, server, requests } = await node([[status, body]])
    try {
      await assert.rejects(new RpcClient(url).call('eth_blockNumber', []), RpcRejected)
      assert.equal(requests(), 1, 'not asked again')
    } finally {
      server.close()
    }
  }
})
