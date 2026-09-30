# esql-datasource-clickhouse

Query **ClickHouse** — and, through it, **Apache Iceberg** — directly from Elasticsearch with **ES|QL**. No ETL, no JDBC, no data duplication.

```esql
FROM clickhouse_orders
| WHERE warehouse_code IN (FROM iceberg_shipments | WHERE status == "lost" | KEEP warehouse_code)
| STATS at_risk_revenue = SUM(order_price) BY warehouse_code
| SORT at_risk_revenue DESC
```

Orders in ClickHouse, shipments in an Iceberg table on S3, joined in one ES|QL statement — coordinated by Elasticsearch. Add cross-cluster search and a single query spans **five data planes**: local indices, Elastic Cloud, S3 objects, ClickHouse tables, and an Iceberg lakehouse.

Built on the ES|QL **Data Federation** framework (experimental in 9.5) using its connector SPI, modeled on the in-tree Arrow Flight connector. Targets **Elasticsearch 9.5.4** exactly; rebuild per version.

> Community project — not a supported Elastic product.

## How it works

- Speaks ClickHouse's native **HTTP interface** (8123; 8443 with `secure=true`) using `FORMAT ArrowStream`
- Arrow record batches convert straight into ES|QL compute blocks — **columnar end-to-end**
- **Column projection and LIMIT push down** to ClickHouse (filters do not, in 9.5.4 — keep a LIMIT or aggregate)
- Identifiers are validated (`^[A-Za-z_][A-Za-z0-9_]*$`) and back-quoted; per-query settings ride as URL parameters
- Best-effort cancellation via `KILL QUERY ... ASYNC` keyed on a per-query `query_id`
- URI form: `clickhouse://host[:port]/database/table_or_view`
- **Iceberg**: ClickHouse's `iceberg()` table function reads Iceberg tables on S3; wrap it in a view and the connector federates the lakehouse with zero plugin changes (read-only; v2 delete-file support in CH 24.8 is partial — append-only tables are the safe zone)

## Install

### Option A — Elastic Cloud Hosted (Extensions)

1. Use the release asset `esql-ch-cloud-9.5.4.zip` (the **flat** zip: `plugin-descriptor.properties` and the jar at the zip **root** — Cloud's validator does not descend into a wrapping folder).
2. Cloud console → **Features → Extensions → Upload extension**: type *Elasticsearch plugin*, version `9.5.4`.
3. Deployment → Edit → tick the extension under **Manage plugins and settings**.
4. Add to Elasticsearch user settings, in the same plan: `esql.federation.enabled: true`
5. Apply the plan (rolling restart), then `GET /_cat/plugins?v` — the plugin must show on **every** node.

Deployment version must equal the plugin version exactly. Serverless does not take custom plugins.

### Option B — Self-managed Docker (bake the image)

The ES plugins directory is container-local, so runtime installs evaporate on recreate — **bake the plugin into the image**:

```bash
cp esql-datasource-clickhouse-9.5.4.zip deploy/es-image/
docker build -t elasticsearch-esql-clickhouse:9.5.4 deploy/es-image
```

Run that image on **every node** (frozen tier included) with:

```yaml
- esql.federation.enabled=true
```

The gate is per-node: a node without it rejects federated work shipped to it, causing intermittent failures. Roll nodes with `docker compose up -d --force-recreate <node>` — plain `restart` reuses the old image.

### Option C — Build from source (in-tree)

The plugin compiles against internal SPI, so it builds inside an Elasticsearch checkout:

```bash
git clone --depth 1 --branch v9.5.4 https://github.com/elastic/elasticsearch.git
cp -r plugin elasticsearch/x-pack/plugin/esql-datasource-clickhouse
cd elasticsearch
./gradlew :x-pack:plugin:esql-datasource-clickhouse:bundlePlugin          # snapshot build
# release-versioned build (Cloud needs elasticsearch.version=9.5.4, no -SNAPSHOT):
./gradlew :x-pack:plugin:esql-datasource-clickhouse:bundlePlugin \
  -Dbuild.snapshot=false \
  -Dlicense.key=$PWD/x-pack/license-tools/src/test/resources/public.key
```

Output: `x-pack/plugin/esql-datasource-clickhouse/build/distributions/`. For Cloud, repackage flat (unzip, then re-zip from inside the folder so the descriptor sits at the root).

## ClickHouse setup

See `deploy/clickhouse/`. The essentials:

- A dedicated read-only account with **`readonly=2`** — the connector sends settings as URL parameters, which `readonly=1` rejects.
- **`output_format_arrow_compression_method=none`** in that account's profile — ClickHouse compresses Arrow output with LZ4_FRAME by default, which Arrow-Java can't read without an optional module. (Schema probes pass either way; the first query with rows fails without this.)
- Set real password hashes: `echo -n 'pw' | sha256sum` into `users.d/*.xml`.
- **S3-compatible stores that enforce SigV4 scope** (Garage, some MinIO configs): ClickHouse signs S3/Iceberg requests as `us-east-1` by default. Configure the region per endpoint or reads fail with `400 Authorization header malformed, unexpected scope`:

```xml
<clickhouse><s3><garage>
  <endpoint>http://YOUR-S3-HOST:3900/</endpoint>
  <region>garage</region>
</garage></s3></clickhouse>
```

### Iceberg via ClickHouse (optional)

```sql
CREATE VIEW analytics.iceberg_shipments_esql AS
SELECT shipment_id, warehouse_code, carrier, status,
       toFloat64(weight_kg) AS weight_kg,
       toDateTime64(ship_ts, 3) AS ship_ts
FROM iceberg('http://YOUR-S3-HOST:3900/BUCKET/namespace/table', 'ACCESS_KEY', 'SECRET_KEY');
```

Register the view like any table. Credentials live in the view DDL, so the read-only account never holds them.

## Register & query

```
PUT /_query/data_source/clickhouse-prod
{ "type": "clickhouse", "settings": {} }

PUT /_query/dataset/clickhouse_orders
{ "data_source": "clickhouse-prod",
  "resource": "clickhouse://ch-host:8123/analytics/orders_esql",
  "settings": { "username": "esql_reader", "password": "<password>" } }

POST /_query
{ "query": "FROM clickhouse_orders | STATS revenue = SUM(order_price) BY warehouse_code | SORT revenue DESC" }
```

- **Credentials go on the dataset, not the data source** — see Finding 2.
- Dataset PUTs are lazy: the first ClickHouse contact happens at query time, so registration errors surface on first query. The true cause of any `Failed to resolve metadata` is one `grep -A8` away in the coordinating node's log.
- Data source *names* are free; the *type* must be `clickhouse`. Multiple logical sources (e.g. one named `iceberg`) can share the type.
- **Decimal and Date columns**: not yet in the Arrow type mapping — expose a view casting `toFloat64(...)` / `toDateTime64(..., 3)` and register the view (v0.2 fixes this natively).

Config keys: `endpoint`, `database`, `table`, `username`, `password`, `secure`, `connect_timeout_ms` (5000), `request_timeout_ms` (300000).

## Findings

Discovered building and debugging this against live clusters; none are documented upstream:

1. **CRUD registration requires a `DataSourceValidator`.** `PUT /_query/data_source` resolves `type` against `DataSourcePlugin#datasourceValidators()`; without one: `unknown data source type`. The in-tree Flight exemplar ships none and misleads.
2. **The `_datasource` secrets envelope reaches connectors undecrypted (9.5.4).** Only the storage-provider path flattens and decrypts it; the connector path doesn't. Workaround: dataset-level credentials (top-level plumbing) stored non-secret. Framework gap worth fixing upstream.
3. **JDK `HttpClient` defaults to an HTTP/2 h2c upgrade** on plaintext, which ClickHouse turns into `AUTHENTICATION_FAILED`. The client is pinned to HTTP/1.1.
4. **ClickHouse compresses Arrow output with LZ4_FRAME by default**; Arrow-Java needs the optional `arrow-compression` module. Fixed server-side via the profile setting above.
5. **`readonly=2`, not `1`** — the connector's per-query settings ride as URL parameters.
6. **Arrow `Decimal(p,s,128)` and `Date` are unmapped** in this version — casting views until the mapping extension lands.
7. **SigV4-strict S3 stores reject ClickHouse's default `us-east-1` signing scope** with an opaque 400; per-endpoint `<region>` config fixes it. The `iceberg()` metadata phase retries through the failure while the data phase fails loudly — the symptom points everywhere except the cause.

Operational lessons: image-bake plugins; `--force-recreate` is the only honest roll; container keystores are ephemeral — secure settings (including `cluster.state.encryption.password.*`) belong in a keystore-populating command wrapper; `esql.federation.enabled` on every node; Cloud extension zips must be flat.

## Limitations

- Filters are not pushed down (9.5.4) — only projection and LIMIT
- Single split; no parallel scan in this version
- `STOP` returns partial results; hard cancel fails the query (a `KILL QUERY` still fires in ClickHouse)
- Credentials are effectively plaintext in cluster state (Finding 2) — use a dedicated read-only ClickHouse account with tight `<networks>` and per-query limits, as shipped
- No `ca_cert` / custom truststore keys yet: `secure=true` trusts the JVM default truststore only (public CAs)

## Roadmap (v0.2)

- Native Decimal/Date Arrow mapping (retire the casting views)
- Filter pushdown
- TLS trust configuration (`ca_cert`, hostname verification) + operator endpoint-allowlist parity with the S3 connector (FIPS groundwork)
- ClickHouse as a first-class type in Kibana's Data Federation UI
- Longer term: catalog-native `esql-datasource-iceberg` with SplitProvider parallel scans

## Credits

Built with [Sourcerer](https://github.com/elastic/sourcerer) — repo-scale code search across the Elasticsearch and Kibana trees mapped the undocumented Data Federation SPI, the connector registration path, and the Kibana UI internals.

## License / status

Lab-grade proof of concept against internal SPI; expect the SPI to move between minors — rebuild in-tree per version. Not an official Elastic product.
