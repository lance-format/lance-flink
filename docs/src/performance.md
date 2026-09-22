# Performance

This page covers vector index selection and the tuning knobs that most affect
read/search throughput and write amplification in the Lance Flink connector.

## Vector index types

| Index type | Best for | Memory | Recall | Key parameters |
|---|---|---|---|---|
| `IVF_FLAT` | Small datasets (< 100K vectors), exact-ish search | High | Highest | `index.num-partitions` |
| `IVF_PQ` | Large datasets, memory-constrained | Lowest | Good | `index.num-partitions`, `index.num-sub-vectors`, `index.num-bits` |
| `IVF_HNSW` | High recall, fast query latency | High | High | `index.num-partitions`, `index.m`, `index.ef-construction` |

## Index selection guide

| Scenario | Recommended index | Reason |
|---|---|---|
| < 100K vectors | `IVF_FLAT` | Highest accuracy, acceptable latency |
| 100K – 10M vectors | `IVF_PQ` | Good accuracy/memory trade-off |
| > 10M vectors | `IVF_PQ` (tuned) | Tune `index.num-partitions` and `index.num-sub-vectors` |
| High recall required | `IVF_HNSW` | Best accuracy, higher memory |
| Memory constrained | `IVF_PQ` | Most memory efficient |
| Real-time search | `IVF_HNSW` | Fastest query latency |

## Search tuning

| Option | Default | Effect |
|---|---|---|
| `vector.nprobes` | 20 | Number of IVF partitions probed per query. Higher = better recall, slower query |
| `vector.ef` | 100 | HNSW search width. Higher = better recall, slower query |
| `vector.refine-factor` | — | Refines top-k results by re-ranking candidates. Higher = better recall |

## Write amplification

| Option | Default | Effect |
|---|---|---|
| `write.batch-size` | 1024 | Rows buffered before a flush; larger = fewer, bigger writes |
| `write.max-rows-per-file` | 1000000 | Rows per data file; larger = fewer files, larger compaction units |

## Distance metrics

| Metric | Description | Range |
|---|---|---|
| `L2` | Euclidean distance | [0, ∞) |
| `Cosine` | Cosine similarity | [-1, 1] |
| `Dot` | Inner product | (-∞, ∞) |

> **Note:** No benchmark numbers are published yet. These knobs are the exposed
> surface; measure against your own workload to tune them.
