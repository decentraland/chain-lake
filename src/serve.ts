import { createLogger } from '@subsquid/logger'
import { required } from './config'
import { createPortal } from './serve/server'
import { staticViews } from './serve/static'

const logger = createLogger('lake:portal')
const port = Number(process.env.PORT || 8100)

// LAKE_DIR is the local directory a lake was written to (LAKE_DEST of the writer). This serves the
// lake as it is; `live` also follows the chain and serves what it has not written yet.
const lakeDir = required('LAKE_DIR')
createPortal(staticViews(lakeDir)).listen(port, () => logger.info(`portal listening on ${port}`))
