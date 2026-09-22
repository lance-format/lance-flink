# Config

Configuration options are grouped by area. The table connector uses `'connector' = 'lance'`; the
catalog types are `'lance'` (directory/S3) and `'lance-namespace'` (dir/rest).

## Table connector options (`connector = 'lance'`)

### Common

| Option | Required | Default | Description |
|---|---|---|---|
| `path` | ✅ | — | Path to the Lance dataset |
| `hadoop.*` | ❌ | — | Prefix for Hadoop-family filesystem config (e.g. `hadoop.tbdsfs.meta`); stripped and injected into the Hadoop `Configuration` used for path resolution |

### Read (Source)

| Option | Required | Default | Description |
|---|---|---|---|
| `read.batch-size` | ❌ | 1024 | Read batch size |
| `read.limit` | ❌ | — | Maximum rows to read (limit pushdown) |
| `read.columns` | ❌ | — | Columns to read, comma separated |
| `read.filter` | ❌ | — | SQL `WHERE`-style filter predicate |
| `read.version` | ❌ | — | Time travel: read a specific dataset version |
| `read.as-of-timestamp` | ❌ | — | Time travel: read as of an ISO-8601 timestamp (ignored when `read.version` is set) |

### Write (Sink)

| Option | Required | Default | Description |
|---|---|---|---|
| `write.batch-size` | ❌ | 1024 | Write batch size |
| `write.mode` | ❌ | append | `append` or `overwrite` |
| `write.max-rows-per-file` | ❌ | 1000000 | Maximum rows per data file |

### Vector index

| Option | Required | Default | Description |
|---|---|---|---|
| `index.type` | ❌ | IVF_PQ | `IVF_PQ`, `IVF_HNSW`, or `IVF_FLAT` |
| `index.column` | ❌ | — | Vector column name to index |
| `index.num-partitions` | ❌ | 256 | IVF partition count |
| `index.num-sub-vectors` | ❌ | — | PQ sub-vector count (auto if unset) |
| `index.num-bits` | ❌ | 8 | PQ quantization bits (1–16) |
| `index.max-level` | ❌ | 7 | HNSW max level |
| `index.m` | ❌ | 16 | HNSW connections per level |
| `index.ef-construction` | ❌ | 100 | HNSW construction search width |

### Vector search

| Option | Required | Default | Description |
|---|---|---|---|
| `vector.column` | ❌ | — | Vector search column name |
| `vector.metric` | ❌ | L2 | `L2`, `Cosine`, or `Dot` |
| `vector.nprobes` | ❌ | 20 | IVF search probe count |
| `vector.ef` | ❌ | 100 | HNSW search width |
| `vector.refine-factor` | ❌ | — | Refine factor for recall |

## Catalog options (`type = 'lance'`)

Directory or S3 warehouse.

| Option | Required | Default | Description |
|---|---|---|---|
| `warehouse` | ✅ | — | Warehouse path (local or `s3://…`) |
| `default-database` | ❌ | default | Default database name |
| `s3-access-key` | ❌ | — | S3 access key ID |
| `s3-secret-key` | ❌ | — | S3 secret access key |
| `s3-region` | ❌ | — | S3 region (e.g. `us-east-1`) |
| `s3-endpoint` | ❌ | — | S3 endpoint (for S3-compatible storage like MinIO) |
| `s3-virtual-hosted-style` | ❌ | true | Virtual-hosted-style URLs |
| `s3-allow-http` | ❌ | false | Allow HTTP (default HTTPS only) |

## Namespace catalog options (`type = 'lance-namespace'`)

| Option | Required | Default | Description |
|---|---|---|---|
| `impl` | ✅ | — | Namespace implementation: `dir` or `rest` |
| `root` | ❌ | — | Root path for directory namespace |
| `uri` | ❌ | — | URI for REST namespace |
| `default-database` | ❌ | default | Default database name |
