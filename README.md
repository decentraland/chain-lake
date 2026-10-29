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
(CollectionV2 collections, vesting contracts). Each factory names the event that announces a new
contract and the topic that holds its address, and `contracts.json` records every contract found.

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
| `absorb` | Adds the contracts a complete lake's config gained, with their whole history read over RPC, by rewriting the chunks that cover it |
| `live` | Follows each chain in `DATASETS` over RPC from where its lake ends, and serves them all from one portal |
| `follow` | Follows one chain over RPC without serving |
| `serve` | Serves a local lake as it is |
| `parity` | Compares what a lake serves with what the SQD portal serves for the same query, block by block, and reports every difference; it only reads |

A dataset starts with `discover` and `backfill`. From then on `live` keeps it up to date and serves
it. `live` is the image's default command, and runs the first two on its own: a dataset whose
backfill is not complete is discovered and backfilled from the SQD portal before it is followed, and
an interrupted backfill resumes where it stopped. Once complete, `coverage.json` says so, and `live`
never reads the SQD portal for that dataset again.

A contract or factory added to the config of a complete lake is absorbed before the lake is followed
again, and so is a factory whose `fromBlock` moved earlier: `coverage.json` records the block each
factory is held from. `absorb` reads the history over RPC, from the first block that can hold it up
to the lake's height: first the contracts the new factories created, then the logs of all of them,
several ranges at once. A contract with a `fromBlock` (the block it was deployed at) is read from
there; one without it, from the dataset's `fromBlock`. RPC trouble is waited out for as long as it
lasts, as the follower does. It merges each block into the chunk that covers it (blocks in a gap
between chunks get a chunk of their own). The history is staged on disk as it is read, and each
changed chunk is written to a new folder as soon as all its blocks are read, so memory holds only the
ranges being read, whatever the history's size. Absorbs run one at a time in a task, beside the portal: an absorb waiting out an RPC outage holds the
other datasets that gained contracts in `absorbing`, without new blocks, until it ends. It moves the
lake to the new folders with one manifest write, so the portal serves the lake before or after, never
in between. A run that stops halfway is run
again: merging the same history twice changes nothing. Absorbing is meant for contracts with little
history next to the lake's: the history goes into the chunks it falls in, whatever their size.

It can outlast a health check, so it does not hold the task back. Meanwhile `live` serves that
dataset as its lake stood, without new blocks: once the task a deployment replaces stops, the
dataset's squids wait for new blocks instead of getting 503s, and the new contracts' history is
served once absorbed. Read the new contracts only once `GET /health` no longer reports the dataset
as `absorbing`. If the task a deployment replaces extends the lake meanwhile, `absorb` reads those
blocks too and publishes again, and that task stops at its next commit.

A factory removed from the config and added back (a deployment rolled back and forward again) is not
absorbed again: `coverage.json` still lists it, and the contracts it created in between are missed.
To absorb them, take the factory out of `contracts` in `coverage.json`: it is then absorbed again
from its `fromBlock`, and the history the lake already holds merges without change.

`live` refuses a lake whose history it cannot vouch for:

- one with no `coverage.json`, which these commands did not start;
- one that a development run wrote with `LAKE_ADDRESSES` or `FROM_BLOCK`, which `coverage.json` marks as partial;
- one whose config gained a contract while its backfill was still running, since the lake holds none of that contract's earlier history (once complete, the contract is absorbed instead).

One `live` process runs a follower per dataset and a single portal, so one service covers both
chains of an environment: `DATASETS=ethereum-mainnet,polygon-mainnet`. The portal listens as soon as
the process starts, serves each dataset once it is ready, and answers `503` for one still starting.
`GET /health` reports each dataset's state, and answers `503` while a dataset that was already
complete is not served yet, so a deployment keeps the task it replaces until then. A first
backfill does not hold it back. Before serving a dataset, `live` reads the last chunk of its lake the way
the portal does, so a lake it cannot read (missing credentials or CA certificates) stops the process
instead of answering `503` to every request.

On S3, any number of processes may write the same lakes, during a deployment for instance, and the
lakes stay consistent. (A local lake is meant for one process: its compare-and-swap is not atomic.) Each process writes its chunks to folders of its own, and a chunk becomes part of
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
- **At the head**, a request for blocks past it is held up to 5 seconds, until a block comes, before it gets its `204`. A squid asks again as soon as it gets one and records its position in its database on every answer, so answering at once would cost a database commit per round trip, tens a second, instead of one per block. The SQD portal holds such requests too.
- **What it serves:** log requests, optionally with each log's transaction, over the fields the lake stores. Anything else gets an explicit `400` instead of less data than was asked for: traces, state diffs, transaction requests, or fields the lake does not hold.
- **How it reads:** a query reads only the chunks whose block range it touches, one at a time in block order. A response ends at 20,000 logs, or at the end of the chunk it is on once it has read for 5 seconds, and the client asks for the rest from there; so a squid reindexing from early history reads each chunk once, however large the lake.

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
| `LAKE_PORTAL_URL` | `parity` | A portal already serving the lake, instead of serving `LAKE_DEST` from inside the process |
| `QUERY_FILE` | `parity` | A squid's query as JSON, `{ fields, logs: [{ where, include, range }] }`; by default, everything the lake holds. The default asks both portals for the contracts in the lake's own registry, so a contract discovery missed is missing on both sides and goes unnoticed: a squid's own query, with its own addresses, catches it |
| `LAKE_DIR` | `serve` | The local directory a lake was written to |
| `STOP_BLOCK`, `FROM_BLOCK` | `discover`, `backfill`, `follow`, `parity` | Bounds a run, for development and comparisons |
| `SQD_PORTAL_URL`, `SQD_PORTAL_API_KEY` | `discover`, `backfill`, `live`, `parity` | The SQD portal to read history from; the public one by default |
| `RPC_URL_<DATASET>` | `live`, `follow`, `parity`, the portal | JSON-RPC endpoint of a dataset, e.g. `RPC_URL_POLYGON_MAINNET`; `rpc.decentraland.org` by default |
| `PORT` | `live`, `serve` | Portal port, 8100 by default |
| `MAX_RANGE`, `ADDRESSES_PER_CALL` | `live`, `follow` | Blocks and addresses per `eth_getLogs` (2000 and 500) |
| `ABSORB_MAX_RANGE`, `ABSORB_CONCURRENCY` | `absorb` | Blocks per `eth_getLogs` while absorbing history, and ranges read at once (10,000 and 4) |
| `ABSORB_HEAP_MB` | `live` | The heap of the `absorb` it runs, which shares the task with the portal (768) |
| `ABSORB_DUCKDB_MEMORY_LIMIT`, `ABSORB_DUCKDB_MAX_TEMP` | `absorb` | The memory its chunk writes may use, and the disk they may spill to past it (`768MB` and `4GB`); the staged history takes disk besides |
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
