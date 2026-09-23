/*
 * ClickHouse connector for ES|QL Data Federation.
 * Structure mirrors org.elasticsearch.xpack.esql.datasource.grpc.FlightConnectorFactory @ v9.5.4.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Factory for ClickHouse connectors. Handles {@code clickhouse://} and {@code ch://} URIs of the form:
 *
 * <pre>  clickhouse://host[:port]/database/table[?secure=true]</pre>
 *
 * Port defaults to 8123 (HTTP) or 8443 when {@code secure} is set. Schema resolution issues
 * {@code SELECT * FROM db.table LIMIT 0 FORMAT ArrowStream} and maps the returned Arrow schema
 * to ES|QL attributes with the same {@code ArrowToEsql} mapping the Flight connector uses.
 */
class ClickHouseConnectorFactory implements ConnectorFactory {

    static final int DEFAULT_HTTP_PORT = 8123;
    static final int DEFAULT_HTTPS_PORT = 8443;

    /** Config keys this connector claims; anything else is rejected as an unknown option. */
    private static final Set<String> CONFIG_KEYS = Set.of(
        "endpoint",
        "database",
        "table",
        "username",
        "password",
        "secure",
        "connect_timeout_ms",
        "request_timeout_ms"
    );

    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public boolean canHandle(String location) {
        return location.startsWith("clickhouse://") || location.startsWith("ch://");
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        // Flatten the _datasource envelope before strict key validation, otherwise
        // registered-dataset queries fail with "unknown option [_datasource]".
        ConfigKeyValidator.check(ClickHouseTarget.effective(config), List.of(CONFIG_KEYS));
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> rawConfig) {
        Map<String, Object> config = ClickHouseTarget.effective(rawConfig);
        ClickHouseTarget target = ClickHouseTarget.parse(location, config);
        try (
            ClickHouseHttp http = ClickHouseHttp.from(target.endpoint(), config);
            BufferAllocator allocator = new RootAllocator()
        ) {
            String sql = "SELECT * FROM " + target.qualifiedTable() + " LIMIT 0";
            try (
                InputStream in = http.arrowStream(sql, null, ClickHouseHttp.defaultFormatSettings(0));
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator)
            ) {
                Schema arrowSchema = reader.getVectorSchemaRoot().getSchema();
                List<Attribute> attributes = ClickHouseTypeMapping.toAttributes(arrowSchema);

                Map<String, Object> resolvedConfig = new HashMap<>();
                resolvedConfig.put("endpoint", target.endpoint());
                resolvedConfig.put("database", target.database());
                resolvedConfig.put("table", target.table());
                // Carry connection options forward so open()/execute() see them alongside
                // the resolved endpoint. Secrets (password) may be SecureString — pass the
                // object through untouched; never Objects.toString() into cluster state here.
                for (String key : List.of("username", "password", "secure", "connect_timeout_ms", "request_timeout_ms")) {
                    Object v = config.get(key);
                    if (v != null) {
                        resolvedConfig.put(key, v);
                    }
                }
                return new SimpleSourceMetadata(attributes, "clickhouse", location, null, null, null, resolvedConfig);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to resolve ClickHouse schema for [" + location + "]: " + e.getMessage(), e);
        }
    }

    @Override
    public Connector open(Map<String, Object> rawConfig) {
        Map<String, Object> config = ClickHouseTarget.effective(rawConfig);
        // Use Objects.toString — direct (String) cast would CCE on SecureString / non-String values.
        String endpoint = Objects.toString(config.get("endpoint"), null);
        if (endpoint == null) {
            throw new IllegalArgumentException("ClickHouse connector requires 'endpoint' in config");
        }
        return new ClickHouseConnector(endpoint, config);
    }
}
