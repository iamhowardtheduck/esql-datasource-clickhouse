/*
 * ClickHouse connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * CRUD-time validator for the {@code clickhouse} data source type.
 * Registering this via {@code DataSourcePlugin#datasourceValidators} is what
 * makes {@code PUT /_query/data_source} accept {@code "type": "clickhouse"}.
 */
final class ClickHouseDataSourceValidator implements DataSourceValidator {

    /** Datasource-level keys; must stay in sync with ClickHouseConnectorFactory.CONFIG_KEYS. */
    private static final Set<String> DATASOURCE_KEYS = Set.of(
        "endpoint",
        "database",
        "table",
        "username",
        "password",
        "secure",
        "connect_timeout_ms",
        "request_timeout_ms"
    );

    // Lab-grade trade-off: secret=true stores the password encrypted, but the 9.5.4 query
    // path (DatasetRewriter -> _datasource envelope) delivers it to CONNECTORS still encrypted
    // (decryptInPlace is top-level only; only StorageProviderRegistry flattens+decrypts).
    // Until that is fixed upstream, store it as a non-secret so the connector receives plaintext.
    private static final Set<String> SECRET_KEYS = Set.of();

    /** Dataset-level keys: none today; the resource URI carries database/table. */
    private static final Set<String> DATASET_KEYS = Set.of("username", "password", "secure", "connect_timeout_ms", "request_timeout_ms");

    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        if (datasourceSettings == null || datasourceSettings.isEmpty()) {
            return Map.of();
        }
        ValidationException errors = new ValidationException();
        Map<String, DataSourceSetting> out = new HashMap<>();
        for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
            String key = e.getKey();
            if (DATASOURCE_KEYS.contains(key) == false) {
                errors.addValidationError("unknown setting [" + key + "] for data source type [clickhouse]; recognised: " + DATASOURCE_KEYS);
                continue;
            }
            out.put(key, new DataSourceSetting(e.getValue(), SECRET_KEYS.contains(key)));
        }
        if (errors.validationErrors().isEmpty() == false) {
            throw errors;
        }
        return out;
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();
        if (resource == null || (resource.startsWith("clickhouse://") || resource.startsWith("ch://")) == false) {
            errors.addValidationError(
                "dataset resource must be a clickhouse:// or ch:// URI (clickhouse://host[:port]/database/table), got [" + resource + "]"
            );
        }
        if (datasetSettings != null) {
            for (String key : datasetSettings.keySet()) {
                if (DATASET_KEYS.contains(key) == false) {
                    errors.addValidationError("unknown dataset setting [" + key + "] for data source type [clickhouse]");
                }
            }
        }
        if (errors.validationErrors().isEmpty() == false) {
            throw errors;
        }
        return datasetSettings == null ? Map.of() : Map.copyOf(datasetSettings);
    }
}
