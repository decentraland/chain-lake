import { Dest, LocalDest } from '@subsquid/file-store'
import { S3Dest } from '@subsquid/file-store-s3'

/**
 * Where a dataset is written: `<LAKE_DEST>/<dataset>`. LAKE_DEST is a local directory for
 * development or an `s3://bucket/prefix` URL.
 */
export function openDest(lakeDest: string, dataset: string, ...path: string[]): Dest {
  const location = [lakeDest.replace(/\/$/, ''), dataset, ...path].join('/')
  // Options of our own, even empty ones, make the client take credentials and region from the usual
  // AWS chain (the task role on ECS). Without them, file-store-s3 requires S3_ACCESS_KEY_ID and
  // S3_SECRET_ACCESS_KEY.
  return location.startsWith('s3://') ? new S3Dest(location, {}) : new LocalDest(location)
}
