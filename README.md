<p align="center">
  <img src="https://flink.apache.org/img/logo/png/1000/flink_squirrel_1000.png" alt="Flink Logo" width="100"/>
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img width="257" alt="Lance Logo" src="https://user-images.githubusercontent.com/917119/199353423-d3e202f7-0269-411d-8ff2-e747e419e492.png">
</p>

<h1 align="center">Flink Connector Lance</h1>

<p align="center">
  <strong>Apache Flink Connector for Lance Vector Database</strong>
</p>

<p align="center">
  <a href="https://github.com/lance-format/lance-flink/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"/>
  </a>
  <a href="https://github.com/lance-format/lance-flink/actions">
    <img src="https://img.shields.io/badge/build-passing-brightgreen.svg" alt="Build Status"/>
  </a>
  <img src="https://img.shields.io/badge/release-0.2.0-blue.svg" alt="Release"/>
  <img src="https://img.shields.io/badge/flink-1.20%20%7C%202.1-orange.svg" alt="Flink Version"/>
  <img src="https://img.shields.io/badge/lance-7.0.0-purple.svg" alt="Lance Version"/>
  <img src="https://img.shields.io/badge/java-11%20%7C%2017-red.svg" alt="Java Version"/>
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-documentation">Documentation</a> •
  <a href="#-contributing">Contributing</a>
</p>

---

## 📖 Overview

`lance-flink` is a high-performance Apache Flink connector for [Lance](https://lancedb.github.io/lance/), a modern columnar data format optimized for machine learning workloads and vector search. This connector enables seamless integration between Flink's powerful stream/batch processing capabilities and Lance's efficient vector storage.

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🔄 **Source & Sink** | Full read/write support for Lance datasets |
| 📊 **Table API & SQL** | Native Flink SQL DDL/DML support |
| 🔍 **Vector Search** | KNN search with L2, Cosine, Dot metrics |
| 📇 **Index Building** | IVF_PQ, IVF_HNSW, IVF_FLAT indexes |
| ✅ **Exactly-Once** | Checkpoint-based exactly-once semantics |
| 🔑 **Upsert & Delete** | Primary-key (including composite) changelog ingestion |
| 🧬 **Schema Evolution** | `ALTER TABLE` add / rename / drop columns (via the `lance` catalog) |
| 🕰️ **Time Travel** | Read historical versions by version or timestamp |
| 🎯 **Predicate Pushdown** | Projection, filter, limit and aggregate pushdown |
| 📁 **Catalog Support** | `lance` and `lance-namespace` (dir / rest) catalogs |

## 🚀 Quick Start

### Prerequisites

| Flink Version | Required JDK | Build Module |
|---------------|--------------|--------------|
| **1.20.x**    | JDK 11 / 17 / 21 | `lance-flink-1.20` |
| **2.1.x**     | JDK 17 / 21 | `lance-flink-2.1` |

- Maven 3.6+
- Lance 7.0.0 Java SDK

> **Note**: Flink 2.x reworked the Source/Sink APIs (FLIP-27 Source V2 + Sink V2 +
> `SupportsCommitter`). This connector shares most of its sources between 1.20 and 2.1 and isolates
> only the version-incompatible parts in the `lance-flink-2.1/` submodule, which provides
> replacement implementations.
>
> Flink 1.18 and 1.19 are no longer built or released from 0.2.0 on — both reached EOL before the
> Flink 2.1 support landed. Their artifacts stay at 0.1.0.

> **Flink 2.1 OVERWRITE semantics**: the "replace the dataset once per job" decision is owned by the
> serial `LanceCommitter` — `LanceSinkWriter` performs no `Dataset.drop` pre-cleanup at all. The
> committer tags every version it produces in OVERWRITE mode with the `flink.job-id` transaction
> property: the job's first commit issues `Overwrite` to replace the previous dataset, and later
> checkpoint commits switch to `Append` automatically. OVERWRITE jobs are therefore safe to run
> with parallelism &gt; 1.

### Maven Dependency

```xml
<!-- Flink 1.20 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>lance-flink-1.20</artifactId>
    <version>0.2.0</version>
</dependency>

<!-- Flink 2.1 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>lance-flink-2.1</artifactId>
    <version>0.2.0</version>
</dependency>
```

### Flink SQL Example

```sql
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

### DataStream API Example

```java
// Read from Lance
LanceSource source = LanceSource.builder()
    .path("/data/vectors")
    .batchSize(1024)
    .columns(Arrays.asList("id", "embedding"))
    .rowType(rowType)
    .build();

// Flink 2.1: FLIP-27 Source V2
DataStream<RowData> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "lance");
// Flink 1.20: legacy SourceFunction
// DataStream<RowData> stream = env.addSource(source);

// Write to Lance (Sink V2 on Flink 2.1, RichSinkFunction on Flink 1.20)
LanceSink sink = LanceSink.builder()
    .path("/data/output")
    .batchSize(1024)
    .writeMode(WriteMode.APPEND)
    .rowType(rowType)
    .build();

// Flink 2.1
stream.sinkTo(sink);
// Flink 1.20
// stream.addSink(sink);
```

## 📚 Documentation

### Table API / SQL

#### Create Catalog

```sql
CREATE CATALOG lance_catalog WITH (
    'type' = 'lance',
    'warehouse' = '/path/to/warehouse',
    'default-database' = 'default'
);

USE CATALOG lance_catalog;
```

#### Table Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `connector` | ✅ | - | Must be `lance` |
| `path` | ✅ | - | Lance dataset path |
| `read.batch-size` | ❌ | 1024 | Read batch size |
| `read.version` | ❌ | latest | Read a specific historical version |
| `read.as-of-timestamp` | ❌ | - | Read the version valid at this timestamp |
| `write.batch-size` | ❌ | 1024 | Write batch size |
| `write.mode` | ❌ | append | `append` or `overwrite` |
| `write.max-rows-per-file` | ❌ | 1000000 | Max rows per file |
| `write.memory-cap-bytes` | ❌ | -1 (unbounded) | Buffered-bytes threshold that forces an early fragment materialization |

### Vector Index Building

```java
LanceIndexBuilder builder = LanceIndexBuilder.builder()
    .datasetPath("/data/vectors")
    .columnName("embedding")
    .indexType(IndexType.IVF_PQ)
    .metricType(MetricType.L2)
    .numPartitions(256)
    .numSubVectors(16)
    .build();

builder.buildIndex();
```

#### Supported Index Types

| Index Type | Use Case | Parameters |
|------------|----------|------------|
| **IVF_PQ** | Large-scale, memory-efficient | `num_partitions`, `num_sub_vectors`, `num_bits` |
| **IVF_HNSW** | High recall, fast search | `num_partitions`, `m`, `ef_construction` |
| **IVF_FLAT** | Small datasets, exact search | `num_partitions` |

#### 📊 Index Selection Guide

| Scenario | Recommended Index | Reason |
|----------|------------------|--------|
| **< 100K vectors** | IVF_FLAT | High accuracy, acceptable performance |
| **100K - 10M vectors** | IVF_PQ | Good balance of accuracy and memory |
| **> 10M vectors** | IVF_PQ (tuned) | Optimize `num_partitions` and `num_sub_vectors` |
| **High recall required** | IVF_HNSW | Best accuracy, higher memory usage |
| **Memory constrained** | IVF_PQ | Most memory efficient |
| **Real-time search** | IVF_HNSW | Fastest query latency |

#### ⚙️ Index Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `index.type` | IVF_PQ | Index type: `IVF_PQ`, `IVF_HNSW`, `IVF_FLAT` |
| `index.column` | - | Vector column name for indexing |
| `index.num-partitions` | 256 | Number of IVF partitions |
| `index.num-sub-vectors` | 16 | PQ sub-vectors (IVF_PQ only) |
| `index.num-bits` | 8 | Bits per sub-vector (IVF_PQ only) |
| `index.m` | 16 | HNSW max connections (IVF_HNSW only) |
| `index.ef-construction` | 100 | HNSW build quality (IVF_HNSW only) |

#### 🔧 Index Building SQL Example

```sql
-- Create table with IVF_PQ index
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

-- Create table with IVF_HNSW index (high accuracy)
CREATE TABLE high_accuracy_vectors (
    id BIGINT,
    vector ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/data/ha_vectors',
    'index.type' = 'IVF_HNSW',
    'index.column' = 'vector',
    'index.num-partitions' = '128',
    'index.m' = '32',
    'index.ef-construction' = '200',
    'vector.metric' = 'L2'
);
```

### Vector Search

```java
LanceVectorSearch search = LanceVectorSearch.builder()
    .datasetPath("/data/vectors")
    .columnName("embedding")
    .metricType(MetricType.COSINE)
    .nprobes(20)
    .build();

search.open();
List<SearchResult> results = search.search(queryVector, 10);
search.close();
```

#### Distance Metrics

| Metric | Description | Range |
|--------|-------------|-------|
| **L2** | Euclidean distance | [0, ∞) |
| **Cosine** | Cosine similarity | [-1, 1] |
| **Dot** | Inner product | (-∞, ∞) |

### Type Mapping

| Lance/Arrow Type | Flink Type |
|------------------|------------|
| Int8/16/32/64 | TINYINT/SMALLINT/INT/BIGINT |
| Float32/64 | FLOAT/DOUBLE |
| String | STRING |
| Boolean | BOOLEAN |
| Binary | BYTES |
| Date32 | DATE |
| Timestamp | TIMESTAMP |
| FixedSizeList\<Float\> | ARRAY\<FLOAT\> |

## 🏗️ Project Structure

```
lance-flink/
├── pom.xml                          # parent POM (aggregates the two modules below)
├── lance-flink-1.20/                # Flink 1.20 build (JDK 11+)
├── lance-flink-2.1/                 # Flink 2.1 build (JDK 17+, FLIP-27 / Sink V2 overrides)
├── src/main/java/org/apache/flink/connector/lance/   # shared sources (both modules)
│   ├── LanceSource.java             # Source (Flink 1.20 legacy; overridden in 2.1)
│   ├── LanceSink.java               # Sink with checkpointing (overridden in 2.1)
│   ├── LanceUpsertSink.java         # Primary-key upsert / delete
│   ├── LanceInputFormat.java        # Batch input format
│   ├── LanceIndexBuilder.java       # Vector index builder
│   ├── LanceVectorSearch.java       # KNN search
│   ├── config/LanceOptions.java     # Configuration options
│   ├── converter/                   # RowData <-> Arrow conversion
│   ├── catalog/namespace/           # lance-namespace catalog configuration
│   ├── util/                        # Lance opener / Hadoop path resolver
│   └── table/                       # Table API, SQL factory, catalogs, UDFs
└── src/test/java/                   # unit & integration tests (*ITCase for surefire)
```

## 🔧 Build

```bash
# Build both modules with tests (JDK 17+ required for the Flink 2.1 module)
mvn clean package

# Build without tests
mvn clean package -DskipTests

# Run every test
mvn test

# Build / test a single module
mvn -pl lance-flink-2.1 test
mvn -pl lance-flink-1.20 test

# Check formatting before committing
mvn spotless:check
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📋 Roadmap

- [x] **Flink 2.1 support** (FLIP-27 Source V2 + Sink V2 / `SupportsCommitter`, JDK 17+ required, see #85)
- [x] **Primary-key upsert / delete and schema evolution** (see #76)
- [x] **Time travel** (`read.version` / `read.as-of-timestamp`)
- [x] **Hadoop-family filesystem reads** (`hdfs://` / `tbdsfs://` / `viewfs://`)
- [ ] Streaming CDC support
- [ ] Delta Lake interoperability
- [ ] Distributed index building
- [ ] GPU acceleration support
- [ ] Python Table API support

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Apache Flink](https://flink.apache.org/) - Stateful computations over data streams
- [LanceDB](https://lancedb.com/) - Modern columnar data format for ML
- [Apache Arrow](https://arrow.apache.org/) - Cross-language development platform

---

<p align="center">
  Made with ❤️ by the Flink Connector Lance Contributors
</p>
