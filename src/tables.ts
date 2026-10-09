import { Column, Table, Types } from '@subsquid/file-store-parquet'

// Addresses and hashes are lowercase 0x-prefixed hex strings, as the portal returns them, so the
// files read the same in DuckDB, Athena or a portal serving them back.
const OPTIONS = { compression: 'GZIP' as const }

export const Blocks = new Table(
  'blocks.parquet',
  {
    number: Column(Types.Int64()),
    hash: Column(Types.String()),
    parent_hash: Column(Types.String()),
    /** Seconds since the epoch, as on chain. */
    timestamp: Column(Types.Int64()),
  },
  OPTIONS
)

export const Logs = new Table(
  'logs.parquet',
  {
    block_number: Column(Types.Int64()),
    log_index: Column(Types.Int32()),
    transaction_index: Column(Types.Int32()),
    transaction_hash: Column(Types.String()),
    address: Column(Types.String()),
    topic0: Column(Types.String(), { nullable: true }),
    topic1: Column(Types.String(), { nullable: true }),
    topic2: Column(Types.String(), { nullable: true }),
    topic3: Column(Types.String(), { nullable: true }),
    data: Column(Types.String()),
  },
  OPTIONS
)

export const Transactions = new Table(
  'transactions.parquet',
  {
    block_number: Column(Types.Int64()),
    transaction_index: Column(Types.Int32()),
    hash: Column(Types.String()),
    from: Column(Types.String()),
    to: Column(Types.String(), { nullable: true }),
    input: Column(Types.String()),
  },
  OPTIONS
)

export const tables = { blocks: Blocks, logs: Logs, transactions: Transactions }
