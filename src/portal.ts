const PUBLIC_PORTAL = 'https://portal.sqd.dev'

export interface PortalSource {
  url: string
  http: { retryAttempts: number; headers: Record<string, string> }
}

/**
 * The SQD portal stream for a dataset. `SQD_PORTAL_URL` selects another portal (the shared one for
 * long backfills); `SQD_PORTAL_API_KEY` is sent only when set.
 */
export function portalSource(dataset: string): PortalSource {
  const host = (process.env.SQD_PORTAL_URL || PUBLIC_PORTAL).replace(/\/$/, '')
  const headers: Record<string, string> = {}
  if (process.env.SQD_PORTAL_API_KEY) headers['x-api-key'] = process.env.SQD_PORTAL_API_KEY
  // Portal errors are transient (a 503 while a chunk replicates, a 529 while the public portal
  // throttles); retrying rides them out.
  return { url: `${host}/datasets/${dataset}`, http: { retryAttempts: Infinity, headers } }
}
