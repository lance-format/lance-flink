# CREATE CATALOG

The Lance Flink connector ships two catalog types, registered via SPI:

| `type` | Class | Description |
|---|---|---|
| `lance` | `LanceCatalogFactory` | Directory-based catalog over a warehouse path (local or S3) |
| `lance-namespace` | `LanceNamespaceCatalogFactory` | Catalog backed by a Lance namespace (dir or REST) |

## Directory catalog (`type = 'lance'`)

```sql
CREATE CATALOG lance_catalog WITH (
    'type' = 'lance',
    'warehouse' = '/path/to/warehouse',
    'default-database' = 'default'
);

USE CATALOG lance_catalog;
```

### S3 warehouse

```sql
CREATE CATALOG lance_s3_catalog WITH (
    'type' = 'lance',
    'warehouse' = 's3://bucket-name/warehouse',
    'default-database' = 'default',
    's3-access-key' = 'your-access-key',
    's3-secret-key' = 'your-secret-key',
    's3-region' = 'us-east-1',
    's3-endpoint' = 'https://s3.amazonaws.com'
);
```

| Option | Required | Default | Description |
|---|---|---|---|
| `warehouse` | ✅ | — | Warehouse path (local or `s3://`) |
| `default-database` | ❌ | `default` | Default database |
| `s3-access-key` | ❌ | — | S3 access key |
| `s3-secret-key` | ❌ | — | S3 secret key |
| `s3-region` | ❌ | — | S3 region |
| `s3-endpoint` | ❌ | — | S3 endpoint (for MinIO etc.) |
| `s3-virtual-hosted-style` | ❌ | `true` | Virtual-hosted-style URLs |
| `s3-allow-http` | ❌ | `false` | Allow HTTP |

## Namespace catalog (`type = 'lance-namespace'`)

```sql
-- Directory-based namespace
CREATE CATALOG my_lance WITH (
    'type' = 'lance-namespace',
    'impl' = 'dir',
    'root' = '/tmp/lance-warehouse'
);

-- REST-based namespace
CREATE CATALOG my_lance WITH (
    'type' = 'lance-namespace',
    'impl' = 'rest',
    'uri' = 'http://localhost:8080'
);
```

| Option | Required | Default | Description |
|---|---|---|---|
| `impl` | ✅ | — | `dir` or `rest` |
| `root` | ❌ | — | Root path for `dir` impl |
| `uri` | ❌ | — | URI for `rest` impl |
| `default-database` | ❌ | `default` | Default database |

## Supported DDL

| Statement | Status |
|---|---|
| `CREATE DATABASE` / `DROP DATABASE` / `ALTER DATABASE` | ✅ |
| `SHOW DATABASES` / `SHOW TABLES` | ✅ |
| `CREATE TABLE` / `DROP TABLE` / `RENAME TABLE` | ✅ |
| `ALTER TABLE` | ❌ — not supported (structure immutable) |
| `CREATE INDEX` | ❌ — no SQL DDL; configure `index.*` on the table |
