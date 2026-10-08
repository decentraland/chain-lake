import { existsSync } from 'fs'
import { join } from 'path'
import { createLogger } from '@subsquid/logger'
import { required } from './config'
import { LakeDataset } from './serve/lake'
import { ChainView, createPortal } from './serve/server'

const logger = createLogger('lake:portal')
const port = Number(process.env.PORT || 8100)

// LAKE_DIR is the local directory a lake was written to (LAKE_DEST of the writer). This serves the
// lake as it is; `live` also follows the chain and serves what it has not written yet.
const lakeDir = required('LAKE_DIR')
const views = new Map<string, Promise<ChainView>>()
createPortal((dataset) => {
  if (!views.has(dataset)) {
    // Only the datasets the lake holds, so a name that matches nothing opens nothing.
    if (!existsSync(join(lakeDir, dataset, 'chunks', 'status.txt'))) return undefined
    views.set(dataset, LakeDataset.open(lakeDir, dataset))
  }
  return views.get(dataset)
}).listen(port, () => logger.info(`portal listening on ${port}`))
