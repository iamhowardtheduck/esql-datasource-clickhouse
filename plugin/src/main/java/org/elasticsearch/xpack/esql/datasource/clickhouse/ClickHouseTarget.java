/*
 * ClickHouse connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Parsed {@code clickhouse://host[:port]/database/table} target plus identifier hygiene.
 *
 * <p>Identifiers (database, table, column names) are validated against a conservative
 * pattern and back-quoted before ever being spliced into SQL, so a hostile dataset
 * definition or projected-column name cannot smuggle SQL into the pushed-down query.
 */
record ClickHouseTarget(String endpoint, String database, String table) {

    private static final Pattern IDENT = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    /** Key under which the resolver nests the parent data source's settings (ExternalSourceResolver.DATASOURCE_CONFIG_KEY). */
    static final String DATASOURCE_ENVELOPE_KEY = "_datasource";

    /**
     * Effective config: the _datasource envelope flattened underneath dataset/query-level
     * keys (which win, matching the resolver's precedence), with _-prefixed internal
     * bookkeeping dropped. Every config consumer goes through here.
     */
    static Map<String, Object> effective(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> out = new HashMap<>();
        Object envelope = config.get(DATASOURCE_ENVELOPE_KEY);
        if (envelope instanceof Map<?, ?> ds) {
            for (Map.Entry<?, ?> e : ds.entrySet()) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
        }
        for (Map.Entry<String, Object> e : config.entrySet()) {
            if (e.getKey().startsWith("_") == false) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    static ClickHouseTarget parse(String location, Map<String, Object> config) {
        URI uri = URI.create(location);
        String scheme = uri.getScheme();
        if ("clickhouse".equals(scheme) == false && "ch".equals(scheme) == false) {
            throw new IllegalArgumentException("ClickHouse locations must use clickhouse:// or ch://, got: " + location);
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("ClickHouse location requires a host: " + location);
        }
        boolean secure = isSecure(uri, config);
        int port = uri.getPort() > 0
            ? uri.getPort()
            : (secure ? ClickHouseConnectorFactory.DEFAULT_HTTPS_PORT : ClickHouseConnectorFactory.DEFAULT_HTTP_PORT);
        String endpoint = (secure ? "https://" : "http://") + host + ":" + port;

        String path = uri.getPath();
        String database = Objects.toString(config.get("database"), null);
        String table = Objects.toString(config.get("table"), null);
        if (path != null && path.length() > 1) {
            String[] parts = path.substring(1).split("/");
            if (parts.length == 1) {
                table = parts[0];
                if (database == null) {
                    database = "default";
                }
            } else if (parts.length == 2) {
                database = parts[0];
                table = parts[1];
            } else {
                throw new IllegalArgumentException("ClickHouse location path must be /database/table or /table: " + location);
            }
        }
        if (database == null || table == null) {
            throw new IllegalArgumentException(
                "ClickHouse location must include /database/table (or provide 'database'/'table' in settings): " + location
            );
        }
        return new ClickHouseTarget(endpoint, checkIdent(database), checkIdent(table));
    }

    private static boolean isSecure(URI uri, Map<String, Object> config) {
        String q = uri.getQuery();
        if (q != null && (q.contains("secure=true") || q.contains("secure=1"))) {
            return true;
        }
        Object s = config.get("secure");
        return s != null && ("true".equalsIgnoreCase(Objects.toString(s, "false")) || Boolean.TRUE.equals(s));
    }

    String qualifiedTable() {
        return quote(database) + "." + quote(table);
    }

    static String checkIdent(String ident) {
        if (ident == null || IDENT.matcher(ident).matches() == false) {
            throw new IllegalArgumentException("Invalid ClickHouse identifier: [" + ident + "]");
        }
        return ident;
    }

    static String quote(String ident) {
        return "`" + checkIdent(ident) + "`";
    }
}
