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
    'vector.metric' = 'COSINE'
);
```

## Required options

| Option | Description |
|---|---|
| `path` | Path to the Lance dataset |

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
