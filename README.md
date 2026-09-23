# esql-datasource-clickhouse

A custom **ES|QL Data Federation connector** that lets Elasticsearch query **ClickHouse** directly with ES|QL — no ETL, no JDBC, no data duplication.

```esql
FROM my_es_index
| WHERE user.id IN (FROM clickhouse_events | WHERE status == 500 | KEEP user_id)
| LIMIT 100
```

One statement, two databases. Add a CCS remote and it's three data planes (Elastic Cloud + on-prem Elasticsearch + ClickHouse) in a single query.

Built against **Elasticsearch v9.5.4** using the ES|QL Data Federation SPI (`x-pack/plugin/esql/.../datasources/spi`), modeled on the in-tree Arrow Flight connector (`esql-datasource-grpc`).

## How it works

- Speaks ClickHouse's **native HTTP interface** (port 8123; 8443 with `secure=true`) using `FORMAT ArrowStream`
- Arrow record batches are converted to ES|QL compute Blocks via the same `ArrowToEsql` machinery the Flight connector uses — **columnar end-to-end, no row-at-a-time conversion**
- Pushes down **column projection and LIMIT** (9.5.4 does not push filters to external datasets)
- Per-query ClickHouse settings ride as URL parameters; identifiers are validated (`^[A-Za-z_][A-Za-z0-9_]*$`) and back-quoted — no SQL smuggling via dataset definitions
- Cancellation: best-effort `KILL QUERY ... ASYNC` by per-query `query_id` + stream close
- URI form: `clickhouse://host[:port]/database/table` (schemes `clickhouse`, `ch`)

## Repository layout

```
plugin/               ES plugin module source (drop into x-pack/plugin/ of a v9.5.4 checkout)
deploy/es-image/      Dockerfile: stock ES 9.5.4 + this plugin baked in
deploy/clickhouse/    ClickHouse compose + config.d/ + users.d/ (read-only esql_reader account)
docs/                 This file and FINDINGS
```

## Build

The plugin must be built **in-tree** (it compiles against internal SPI):

```bash
git clone --depth 1 --branch v9.5.4 https://github.com/elastic/elasticsearch.git
cp -r plugin elasticsearch/x-pack/plugin/esql-datasource-clickhouse
cd elasticsearch
./gradlew :x-pack:plugin:esql-datasource-clickhouse:bundlePlugin
# → x-pack/plugin/esql-datasource-clickhouse/build/distributions/esql-datasource-clickhouse-9.5.4-SNAPSHOT.zip
```

Or grab the zip from Releases.

## Deploy

**1. Bake the plugin into the image** — the ES plugins directory is container-local (not a volume), so runtime installs evaporate on recreate:

```bash
cp esql-datasource-clickhouse-*.zip deploy/es-image/esql-datasource-clickhouse-9.5.4.zip
docker build -t elasticsearch-esql-clickhouse:9.5.4 deploy/es-image
```

**2. Run it on EVERY node** and set on every node (frozen tier included):

```yaml
- esql.federation.enabled=true
```

The gate is per-node: a data node without it **rejects federated work shipped to it**, producing intermittent failures depending on which node a plan touches. Roll nodes with `docker compose up -d --force-recreate <node>` — plain `restart` reuses the old image.

**3. ClickHouse** (see `deploy/clickhouse/`): set real password hashes (`echo -n 'pw' | sha256sum`) in `users.d/*.xml`. The `esql_reader` account is `readonly=2` deliberately — the connector sends settings as URL params, which `readonly=1` rejects.

## Register & query

```
PUT /_query/data_source/ch_prod
{ "type": "clickhouse", "settings": {} }

PUT /_query/dataset/clickhouse_events
{ "data_source": "ch_prod",
  "resource": "clickhouse://<ch-host>:8123/analytics/events",
  "settings": { "username": "esql_reader", "password": "<password>" } }

POST /_query
{ "query": "FROM clickhouse_events | STATS hits=COUNT(*) BY path, status | SORT hits DESC" }
```

**Credentials go on the dataset, not the data source** — see Finding 2. Dataset PUTs are lazy (no probe); the first schema resolution happens at query time.

Config keys: `endpoint`, `database`, `table`, `username`, `password`, `secure`, `connect_timeout_ms` (5000), `request_timeout_ms` (300000).

## Findings (the hard-won part)

Discovered building and debugging this against a live 9.5.4 cluster — none are documented upstream:

1. **CRUD registration requires a `DataSourceValidator`.** `PUT /_query/data_source` resolves `type` against `DataSourcePlugin#datasourceValidators()`; without one you get `unknown data source type [...]`. The in-tree Flight connector ships none and misleads — it is never registered via REST in release builds.
2. **The `_datasource` envelope reaches connectors undecrypted (9.5.4).** Secrets stored `secret=true` arrive at connectors as `EncryptedData` carriers: `DataSourceCredentials.decryptInPlace` is top-level-only and only `StorageProviderRegistry` flattens+decrypts the envelope — the connector path doesn't. Workaround: pass credentials as **dataset-level** settings (top-level, plaintext plumbing), or store them non-secret. Framework gap worth fixing upstream.
3. **JDK `HttpClient` defaults to HTTP/2 and sends an h2c upgrade** (`Upgrade: h2c`, `HTTP2-Settings`) on plaintext connections; ClickHouse mishandles it into `AUTHENTICATION_FAILED`. The client is pinned to `HTTP_1_1`.
4. **ClickHouse compresses Arrow output with LZ4_FRAME by default**; Arrow-Java needs the optional `arrow-compression` module to read it. Set `output_format_arrow_compression_method=none` (in the profile and/or as a connector param) — schema probes (`LIMIT 0`) succeed regardless, so this only surfaces on the first query with rows.
5. **The connector's config map includes internal keys** (`_datasource` envelope) at query time — strict key validation must flatten the envelope and ignore `_`-prefixed keys (`ClickHouseTarget.effective`).

Operational lessons: image-bake plugins; `--force-recreate` is the only honest roll; container keystores are ephemeral — secure settings (including `cluster.state.encryption.password.*`, which encrypted data-source secrets depend on) belong in a keystore-populating command wrapper or they die with the container.

## Limitations

- 9.5.4: filters are **not** pushed down to external datasets — only projection and LIMIT; keep tables or LIMITs reasonable
- `STOP` returns partial results; hard cancel fails the query (connector still fires `KILL QUERY` in ClickHouse)
- Single split (no parallel scan) in this version
- Passwords for this connector are effectively plaintext in cluster state (Finding 2) — use a dedicated read-only ClickHouse account with tight `<networks>` and per-query limits, as shipped in `deploy/clickhouse/`

## License / status

Lab-grade proof of concept against internal SPI — expect the SPI to change between minor versions; rebuild in-tree per version. Not an official Elastic product.
