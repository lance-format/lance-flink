# Flink Lance Connector

## Introduction

The Apache Flink Connector for Lance allows Apache Flink to read and write datasets stored in the
[Lance](https://lance.org/) columnar format — an open lakehouse format optimized for multimodal AI
and vector search workloads.

By using the Flink Connector for Lance, you can run Flink's stream/batch processing, SQL querying,
and stateful pipelines directly on Lance datasets, including native vector search.

## Supported versions

| Module | Flink version | Required JDK |
|---|---|---|
| `lance-flink-1.20` | Apache Flink 1.20 | JDK 11+ |
| `lance-flink-2.1` | Apache Flink 2.1 | JDK 17+ |

Flink 1.18 and 1.19 reached EOL before the Flink 2.1 support landed and are no longer built or
released from 0.2.0 on.

## Features

The connector is built on the Flink Table API (`DynamicTableSource` / `DynamicTableSink`) plus
`CatalogFactory`. Specifically, you can use the Flink Connector for Lance to:

* **Read & Write Lance Datasets**: append and overwrite datasets via Flink SQL or the DataStream API.
* **Upsert & Delete**: declare a `PRIMARY KEY ... NOT ENFORCED` (single or composite) and ingest
  `+I` / `+U` / `-D` changelogs through `Dataset.mergeInsert`.
* **Schema Evolution**: `ALTER TABLE` add / rename / drop columns through the `lance` catalog.
* **Column, Filter, Limit & Aggregate Pushdown**: push projections, `WHERE` predicates, limits and
  aggregations down to Lance for efficient scans.
* **Vector Search**: KNN search over `ARRAY<FLOAT>` columns with `L2`, `Cosine`, and `Dot` metrics,
  via the `LanceVectorSearchFunction` table function.
* **Vector Index Building**: create `IVF_PQ`, `IVF_HNSW`, and `IVF_FLAT` indexes.
* **Time Travel**: read a historical version via `read.version` or `read.as-of-timestamp`.
* **Hadoop-compatible filesystems**: read `hdfs://`, `tbdsfs://` and `viewfs://` paths through the
  Hadoop `FileSystem` API.
* **Catalog Support**: a directory/S3 `lance` catalog and a `lance-namespace` catalog (dir / rest).

## Quick Start

Create a catalog and a table, then insert and query:

```sql
-- Create a directory-based catalog
CREATE CATALOG lance_catalog WITH (
    'type' = 'lance',
    'warehouse' = '/path/to/warehouse',
    'default-database' = 'default'
);

USE CATALOG lance_catalog;

-- Create a Lance table
CREATE TABLE vectors (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/data/vectors',
    'write.batch-size' = '1024'
);

-- Insert data
INSERT INTO vectors VALUES
    (1, 'Hello World', ARRAY[0.1, 0.2, 0.3, 0.4]);

-- Query data
SELECT * FROM vectors WHERE id > 0;
```

See [Install](install.md) for dependency setup and [Operations](operations/) for the full SQL
surface.
