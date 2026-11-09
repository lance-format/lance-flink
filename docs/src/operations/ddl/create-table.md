# CREATE TABLE

Lance tables are created through the dynamic table factory (`connector = 'lance'`).
The actual Lance dataset is created lazily on first write.

## Minimal example

```sql
CREATE TABLE vectors (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/data/vectors'
);
```

## With a vector index

```sql
CREATE TABLE doc_embeddings (
    doc_id BIGINT,
    title STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/data/embeddings',
    'index.type' = 'IVF_PQ',
    'index.column' = 'embedding',
    'index.num-partitions' = '256',
    'index.num-sub-vectors' = '16',
    'index.num-bits' = '8',
    'vector.metric' = 'COSINE'
);
```

Index tuning knobs (all optional, shown with defaults):

| `index.type` | Options |
|---|---|
| `IVF_PQ` | `index.num-partitions` (256), `index.num-sub-vectors` (auto), `index.num-bits` (8) |
| `IVF_HNSW` | `index.num-partitions` (256), `index.max-level` (7), `index.m` (16), `index.ef-construction` (100) |
| `IVF_FLAT` | `index.num-partitions` (256) |

Building the index itself is done through `LanceIndexBuilder` (see
[Performance](../../performance.md)); the `index.*` table options carry the configuration.

## Required options

| Option | Description |
|---|---|
| `path` | Path to the Lance dataset |

## Primary key tables

Add `PRIMARY KEY ... NOT ENFORCED` to switch the table from append-only to upsert:

```sql
CREATE TABLE users (
    id BIGINT,
    name STRING,
    embedding ARRAY<FLOAT>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'lance',
    'path' = '/data/users'
);
```

Composite primary keys are supported, and the constraint is persisted with the dataset so it
survives a catalog round-trip. See [INSERT INTO](../dml/insert-into.md) for the changelog modes
this enables.

## Type mapping

| Lance / Arrow type | Flink type |
|---|---|
| Int8 / Int16 / Int32 / Int64 | TINYINT / SMALLINT / INT / BIGINT |
| Float32 / Float64 | FLOAT / DOUBLE |
| String | STRING |
| Boolean | BOOLEAN |
| Binary | BYTES |
| Date32 | DATE |
| Timestamp | TIMESTAMP |
| FixedSizeList\<Float\> | ARRAY\<FLOAT\> |

See [Config](../../config.md) for the full option reference.
