# SELECT

The Lance Flink connector supports reading Lance datasets via Flink SQL `SELECT`.
Read optimizations are pushed down to Lance natively to reduce I/O.

## Supported pushdowns

| Ability | Interface | Notes |
|---|---|---|
| Column projection | `SupportsProjectionPushDown` | Only projected columns are read |
| Predicate (filter) | `SupportsFilterPushDown` | `WHERE` clauses are pushed to Lance |
| Limit | `SupportsLimitPushDown` | `LIMIT` is pushed down |
| Aggregate | `SupportsAggregatePushDown` | Eligible aggregates run natively |

## Predicate pushdown coverage

A predicate that cannot be translated faithfully is **not** pushed down. It is
returned to Flink, which evaluates it itself, so results are always correct —
only the I/O saving is lost.

Pushed down:

- Comparisons `=`, `!=`, `>`, `>=`, `<`, `<=`, and `LIKE`
- `AND`, `OR`, `NOT`
- `IS NULL`, `IS NOT NULL`
- `IN` and `BETWEEN` — the planner expands these into an `OR` chain and a
  `>=`/`<=` conjunction respectively before the connector sees them, so they
  push down through the rules above rather than as their own operators

Column names are emitted as backtick-quoted identifiers, so names containing
spaces, uppercase letters, or SQL keywords resolve correctly.

Evaluated by Flink instead:

- Columns whose name contains `.` — Lance reads a dot as nested-field access
  and provides no escape for it
- Columns whose name contains a backtick
- `NaN` and `Infinity` literals, which have no valid predicate form
- `TIME` literals, which Lance's filter grammar does not express

Temporal and decimal literals are rendered with the type prefix Lance requires
(`date '2026-03-14'`, `timestamp(3) '2026-03-14 15:09:26'`,
`decimal(10,2) '1234.50'`), so a `DATE` or `TIMESTAMP` column is compared as a
temporal value rather than as a string.

## Example

```sql
-- Projection + filter + limit are all pushed down
SELECT id, content
FROM lance_table
WHERE id > 100
LIMIT 10;
```

## Static read options

The same read behaviour can be configured declaratively on the table DDL
without relying on planner pushdown:

```sql
CREATE TABLE lance_table (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/data/vectors',
    'read.columns' = 'id,content',
    'read.filter' = 'id > 100',
    'read.limit' = '10'
);
```

| Option | Description |
|---|---|
| `read.columns` | Comma-separated columns to read |
| `read.filter` | SQL `WHERE`-style filter string |
| `read.limit` | Maximum rows to read |
