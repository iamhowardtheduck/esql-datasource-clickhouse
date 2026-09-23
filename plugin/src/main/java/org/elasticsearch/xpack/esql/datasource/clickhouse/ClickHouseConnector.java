/*
 * ClickHouse connector for ES|QL Data Federation.
 * Structure mirrors org.elasticsearch.xpack.esql.datasource.grpc.FlightConnector @ v9.5.4.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A live connection to a ClickHouse HTTP endpoint. Each {@link #execute} builds a
 * projection- and limit-pushed SQL statement, tags it with a fresh {@code query_id}
 * (so ES|QL task cancellation can {@code KILL QUERY} server-side), and streams the
 * response back as Arrow batches.
 *
 * <p>What is pushed down today: column projection and {@code LIMIT}. Filters and
 * aggregations run in ES|QL after the rows arrive — same behavior the framework
 * documents for external datasets ("the filter ... is not applied to external
 * dataset(s)"). Extending this with a {@code FilterPushdownSupport} is the natural
 * follow-up once the basic path is proven out.
 */
class ClickHouseConnector implements Connector {

    private final Map<String, Object> config;
    private final ClickHouseHttp http;

    ClickHouseConnector(String endpoint, Map<String, Object> config) {
        this.config = config;
        this.http = ClickHouseHttp.from(endpoint, config);
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        Map<String, Object> requestConfig = ClickHouseTarget.effective(request.config());
        String database = ClickHouseTarget.checkIdent(configString(requestConfig, "database"));
        String table = ClickHouseTarget.checkIdent(configString(requestConfig, "table"));
        String sql = buildSql(database, table, request);
        String queryId = "esql-" + UUID.randomUUID();

        try {
            InputStream in = http.arrowStream(sql, queryId, ClickHouseHttp.defaultFormatSettings(request.batchSize()));
            return new ClickHouseResultCursor(http, queryId, in, request.attributes(), request.blockFactory());
        } catch (IOException e) {
            throw new UncheckedIOException("ClickHouse query failed for [" + database + "." + table + "]", e);
        }
    }

    private String configString(Map<String, Object> requestConfig, String key) {
        Object fromRequest = requestConfig.get(key);
        Object value = fromRequest != null ? fromRequest : config.get(key);
        String s = Objects.toString(value, null);
        if (s == null) {
            throw new IllegalArgumentException("ClickHouse connector requires '" + key + "' in config");
        }
        return s;
    }

    private static String buildSql(String database, String table, QueryRequest request) {
        List<String> projected = request.projectedColumns();
        String select;
        if (projected == null || projected.isEmpty()) {
            // Fall back to the resolved schema's column list rather than SELECT * so the
            // wire order always matches request.attributes().
            select = request.attributes().stream().map(a -> ClickHouseTarget.quote(a.name())).collect(Collectors.joining(", "));
        } else {
            select = projected.stream().map(ClickHouseTarget::quote).collect(Collectors.joining(", "));
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(select)
            .append(" FROM ")
            .append(ClickHouseTarget.quote(database))
            .append('.')
            .append(ClickHouseTarget.quote(table));
        int rowLimit = request.rowLimit();
        if (rowLimit != FormatReader.NO_LIMIT && rowLimit >= 0) {
            sql.append(" LIMIT ").append(rowLimit);
        }
        return sql.toString();
    }

    @Override
    public void close() throws IOException {
        http.close();
    }

    @Override
    public String toString() {
        return "ClickHouseConnector";
    }
}
