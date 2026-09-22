# Vector Search

The Lance Flink connector exposes a table function for KNN vector search over a
Lance dataset, powered by Lance's IVF / HNSW indexes.

## Function signature

The function is `LanceVectorSearchFunction` — a Flink `TableFunction` registered as a
temporary function before use:

```sql
CREATE TEMPORARY FUNCTION vector_search AS
    'org.apache.flink.connector.lance.table.LanceVectorSearchFunction'
    LANGUAGE JAVA USING JAR '/path/to/lance-flink-1.18-0.1.0.jar';
```

The registered function name (here `vector_search`) is user-chosen. Its `eval` overloads are:

```
vector_search(dataset_path, column_name, query_vector [, k [, metric]])
```

- `dataset_path` — path to the Lance dataset
- `column_name` — the vector column to search
- `query_vector` — the query vector (`ARRAY<FLOAT>`; also accepts `DECIMAL[]` / `DOUBLE[]` / `float[]`)
- `k` — number of nearest neighbours (default `10`)
- `metric` — distance metric (`L2` / `Cosine` / `Dot`, default `L2`)

The emitted row is the source row plus a `_distance DOUBLE` column.

## Distance metrics

| Metric | Description | Range |
|---|---|---|
| `L2` | Euclidean distance | [0, ∞) |
| `Cosine` | Cosine similarity | [-1, 1] |
| `Dot` | Inner product | (-∞, ∞) |

## Search options

Configured on the table DDL or via `LanceOptions`:

| Option | Default | Description |
|---|---|---|
| `vector.column` | — | Vector column name |
| `vector.metric` | `L2` | Distance metric |
| `vector.nprobes` | 20 | IVF probe count |
| `vector.ef` | 100 | HNSW search width |
| `vector.refine-factor` | — | Re-rank factor for recall |

## Example

```sql
CREATE TABLE vectors (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/data/vectors',
    'index.type' = 'IVF_PQ',
    'index.column' = 'embedding',
    'vector.metric' = 'COSINE'
);

-- KNN search for the 10 nearest vectors
SELECT *
FROM vectors,
     LATERAL TABLE(vector_search(
         '/data/vectors',
         'embedding',
         ARRAY[0.1, 0.2, 0.3, 0.4],
         10,
         'COSINE'
     ));
```
