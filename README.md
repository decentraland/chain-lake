# chain-lake

Decentraland's own copy of the chain data its indexers read, and a portal that serves it.

The [squids](https://docs.sqd.dev/) that index Decentraland's contracts read the chain through the SQD
portal. This project keeps that data in our own storage and serves it over the same protocol, so a
squid switches portals by changing one URL and nothing else.

```
RPC ──► follower ──► lake (Parquet, local or S3)   finalized blocks
            │
            └──► memory                             recent and unfinalized blocks
                    │
       portal ◄─────┘   reads the lake and the follower's memory
         ▲
       squids           SQD_PORTAL_URL=<this portal>
```

## What the lake holds

One dataset per chain (`ethereum-mainnet`, `polygon-mainnet`, `ethereum-sepolia`,
`polygon-amoy-testnet`), for the contracts in `config/<dataset>.json`:

- **Every log** of every followed contract, not only the events the squids read today.
- **Every transaction** that emitted one of those logs.
- **The header of every block** holding one of those logs.

The followed contracts are the configured ones, the factories, and every contract the factories create
(the CollectionV2 collections). The latter are found from the factories' `ProxyCreated` events, and
`contracts.json` records them.

```
<lake>/<dataset>/manifest.json                            the last block the lake holds, and the chunks that hold it
<lake>/<dataset>/contracts.json                           the contracts followed
<lake>/<dataset>/coverage.json                            whose history the lake holds, and whether its backfill is complete
<lake>/<dataset>/chunks/<from>-<to>-<writer>/blocks.parquet
<lake>/<dataset>/chunks/<from>-<to>-<writer>/logs.parquet
<lake>/<dataset>/chunks/<from>-<to>-<writer>/transactions.parquet
```

The lake is what `manifest.json` lists, and nothing else: a chunk folder it does not list is never
read.

Addresses and hashes are lowercase hex strings, so the files read the same in DuckDB, Athena or
Spark.

## Commands

| Command | What it does |
| --- | --- |
| `discover` | Finds the contracts the factories created, up to a block, and writes `contracts.json` |
| `backfill` | Writes the history of the followed contracts from the SQD portal, up to the height of `contracts.json` |
| `live` | Follows each chain in `DATASETS` over RPC from where its lake ends, and serves them all from one portal |
| `follow` | Follows one chain over RPC without serving |
| `serve` | Serves a local lake as it is |

A dataset starts with `discover` and `backfill`. From then on `live` keeps it up to date and serves
it. `live` is the image's default command, and runs the first two on its own: a dataset whose
backfill is not complete is discovered and backfilled from the SQD portal before it is followed, and
an interrupted backfill resumes where it stopped. Once complete, `coverage.json` says so, and `live`
never reads the SQD portal for that dataset again.

`live` refuses a lake whose history it cannot vouch for:

- one with no `coverage.json`, which these commands did not start;
- one that a development run wrote with `LAKE_ADDRESSES` or `FROM_BLOCK`, which `coverage.json` marks as partial;
- one whose config gained a contract after its backfill started, since the lake holds none of that contract's earlier history.

One `live` process runs a follower per dataset and a single portal, so one service covers both
chains of an environment: `DATASETS=ethereum-mainnet,polygon-mainnet`. The portal listens as soon as
the process starts, serves each dataset once it is ready, and answers `503` for one still starting;
`GET /health` reports which.

Any number of processes may write the same lakes, during a deployment for instance, and the lakes
stay consistent. Each process writes its chunks to folders of its own, and a chunk becomes part of
the lake only when the process writes `manifest.json` by compare-and-swap (S3 conditional writes)
from the version it last read. One process extends each lake; any other finds the manifest changed
and stops before it publishes anything. `contracts.json` and `coverage.json` are written the same
way.

When a chain's RPC node fails or falls behind, only that dataset's follower stops, and it retries
until the node answers again; the portal keeps serving everything it has. Any other failure ends the
process, and the service restarts as a whole.

## The portal

`live` and `serve` speak the SQD portal protocol: `POST /datasets/<dataset>/stream`, `POST /datasets/<dataset>/finalized-stream`, `GET /datasets/<dataset>/head` and `GET /datasets/<dataset>/finalized-head`.

- **`finalized-stream`** serves finalized blocks.
- **`stream`** also serves the blocks above the last finalized one. When a client's parent block was reorged away, it answers `409` with the canonical blocks, so the squid rolls back.
- **What it serves:** log requests, optionally with each log's transaction, over the fields the lake stores. Anything else gets an explicit `400` instead of less data than was asked for: traces, state diffs, transaction requests, or fields the lake does not hold.
- **How it reads:** a query reads only the chunks whose block range it touches.

## Running

```bash
npm ci
npm run build
export DATASET=polygon-mainnet LAKE_DEST=./data
npm run discover                      # STOP_BLOCK=<block> to stop early
npm run backfill                      # SQD_PORTAL_URL / SQD_PORTAL_API_KEY for another portal
npm run live                          # then point a squid at http://localhost:8100
```

| Variable | Used by | Meaning |
| --- | --- | --- |
| `DATASET` | all | `ethereum-mainnet`, `polygon-mainnet`, `ethereum-sepolia` or `polygon-amoy-testnet` |
| `DATASETS` | `live` | Several datasets, comma-separated; `live` takes `DATASET` when this is not set |
| `LAKE_DEST` | all but `serve` | A local directory or `s3://bucket/prefix`; S3 credentials come from the AWS environment |
| `LAKE_DIR` | `serve` | The local directory a lake was written to |
| `STOP_BLOCK`, `FROM_BLOCK` | `discover`, `backfill`, `follow` | Bounds a run, for development and comparisons |
| `SQD_PORTAL_URL`, `SQD_PORTAL_API_KEY` | `discover`, `backfill`, `live` | The SQD portal to read history from; the public one by default |
| `RPC_URL_<DATASET>` | `live`, `follow`, the portal | JSON-RPC endpoint of a dataset, e.g. `RPC_URL_POLYGON_MAINNET`; `rpc.decentraland.org` by default |
| `PORT` | `live`, `serve` | Portal port, 8100 by default |
| `MAX_RANGE`, `ADDRESSES_PER_CALL` | `live`, `follow` | Blocks and addresses per `eth_getLogs` (2000 and 500) |
| `POLL_MS` | `live`, `follow` | How often a follower that is caught up looks for newly finalized blocks (5000) |
| `CHUNK_SIZE_MB` | `backfill`, `live`, `follow` | Size of the data a chunk holds before it is written (64) |
| `FLUSH_INTERVAL_MS` | `live` | Writes a chunk at least this often (30 minutes) |
| `HOT_BLOCKS`, `HOT_POLL_MS` | `live` | `false` serves finalized blocks only; how often the chain head is polled (2000) |
| `HOT_MAX_BLOCKS` | `live` | Unfinalized blocks held at most; a follower further behind serves finalized blocks only (1000) |
| `DUCKDB_MEMORY_LIMIT` | `live`, `serve` | Memory the portal's queries may use, shared by every dataset (`1GB`) |

## Tests

```bash
npm test
```

## License

[MIT](LICENSE)
