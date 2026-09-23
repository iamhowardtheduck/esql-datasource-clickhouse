/*
 * ClickHouse connector for ES|QL Data Federation.
 * Structure mirrors org.elasticsearch.xpack.esql.datasource.grpc.GrpcDataSourcePlugin @ v9.5.4.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.Set;

/**
 * Registers the ClickHouse connector and storage SPI for ESQL.
 * Handles {@code clickhouse://} and {@code ch://} URIs.
 *
 * <p>Transport is ClickHouse's native HTTP interface (default port 8123) with
 * {@code FORMAT ArrowStream} output, so pages arrive columnar and are converted
 * to ES|QL {@link org.elasticsearch.compute.data.Block}s via the same
 * {@code ArrowToEsql} machinery the Arrow Flight connector uses. No JDBC driver,
 * no row-at-a-time conversion.
 *
 * <p>Availability still depends on the core federation gate:
 * {@code esql.federation.enabled: true} must be set on every node
 * (see org.elasticsearch.xpack.esql.datasources.Federation).
 */
public class ClickHouseDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedSchemes() {
        return Set.of("clickhouse", "ch");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        // ClickHouse is not a byte-addressable blob store; data is read through the
        // Connector. This minimal provider exists (exactly like FlightStorageProvider)
        // so ExternalSourceResolver can register a concrete FileList entry for the
        // scheme and satisfy the storage SPI contract.
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(ClickHouseStorageProvider::new);
        return Map.of("clickhouse", factory, "ch", factory);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        return Set.of("clickhouse", "ch");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of("clickhouse", new ClickHouseConnectorFactory());
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        // Without this registration, PUT /_query/data_source rejects the type with
        // "unknown data source type [clickhouse]" — the CRUD layer only accepts
        // types that ship a validator (Flight skips this because it is never
        // registered through the REST CRUD path in release builds).
        DataSourceValidator validator = new ClickHouseDataSourceValidator();
        return Map.of(validator.type(), validator);
    }
}
