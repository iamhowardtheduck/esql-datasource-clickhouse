/*
 * ClickHouse connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Thin wrapper around ClickHouse's native HTTP interface using the JDK HttpClient.
 *
 * <p>Queries are POSTed as the request body with {@code FORMAT ArrowStream} appended, and
 * per-query ClickHouse settings travel as URL parameters. Credentials go in
 * {@code X-ClickHouse-User} / {@code X-ClickHouse-Key} headers. Requires the plugin's
 * {@code outbound_network} entitlement (see plugin-metadata/entitlement-policy.yaml).
 */
final class ClickHouseHttp implements Closeable {

    private final String endpoint;
    private final String username;
    private final String password;
    private final Duration requestTimeout;
    private final HttpClient client;

    private ClickHouseHttp(String endpoint, String username, String password, Duration connectTimeout, Duration requestTimeout) {
        this.endpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        this.username = username;
        this.password = password;
        this.requestTimeout = requestTimeout;
        this.client = HttpClient.newBuilder().connectTimeout(connectTimeout).followRedirects(HttpClient.Redirect.NEVER).build();
    }

    static ClickHouseHttp from(String endpoint, Map<String, Object> config) {
        // Objects.toString — values may be SecureString; a direct (String) cast would CCE.
        String username = Objects.toString(config.get("username"), "default");
        String password = Objects.toString(config.get("password"), "");
        long connectMs = parseLong(config.get("connect_timeout_ms"), 5_000L);
        long requestMs = parseLong(config.get("request_timeout_ms"), 300_000L);
        return new ClickHouseHttp(endpoint, username, password, Duration.ofMillis(connectMs), Duration.ofMillis(requestMs));
    }

    private static long parseLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(Objects.toString(value));
    }

    /**
     * Per-query settings that make ClickHouse's Arrow output line up with what
     * {@code ArrowToEsql} understands:
     * <ul>
     *   <li>{@code output_format_arrow_string_as_string=1} — emit String columns as Arrow Utf8
     *       instead of Binary, so they map to ES|QL keyword.</li>
     *   <li>{@code output_format_arrow_low_cardinality_as_dictionary=0} — flatten
     *       LowCardinality columns; dictionary-encoded vectors are not in the mapping.</li>
     *   <li>{@code max_block_size} — align ClickHouse batch size with the ES|QL page size.</li>
     * </ul>
     */
    static Map<String, String> defaultFormatSettings(int batchSize) {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("output_format_arrow_string_as_string", "1");
        settings.put("output_format_arrow_low_cardinality_as_dictionary", "0");
        if (batchSize > 0) {
            settings.put("max_block_size", Integer.toString(batchSize));
        }
        return settings;
    }

    /**
     * Executes {@code sql} with {@code FORMAT ArrowStream} and returns the raw response stream.
     * Caller owns the stream and must close it (closing also releases the connection).
     */
    InputStream arrowStream(String sql, String queryId, Map<String, String> settings) throws IOException {
        StringBuilder url = new StringBuilder(endpoint).append("/?default_format=ArrowStream");
        if (queryId != null) {
            url.append("&query_id=").append(URLEncoder.encode(queryId, StandardCharsets.UTF_8));
        }
        for (Map.Entry<String, String> e : settings.entrySet()) {
            url.append('&')
                .append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8))
                .append('=')
                .append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
        }
        String body = sql + " FORMAT ArrowStream";
        HttpResponse<InputStream> response = send(url.toString(), body);
        if (response.statusCode() != 200) {
            String error;
            try (InputStream err = response.body()) {
                error = new String(err.readNBytes(8 * 1024), StandardCharsets.UTF_8);
            }
            throw new IOException("ClickHouse returned HTTP " + response.statusCode() + " for query [" + sql + "]: " + error);
        }
        return response.body();
    }

    /** Best-effort {@code KILL QUERY} for a cancelled ES|QL task. Never throws. */
    void killQuery(String queryId) {
        if (queryId == null) {
            return;
        }
        try {
            // queryId is a UUID we generated ourselves, safe to inline.
            HttpResponse<InputStream> response = send(endpoint + "/", "KILL QUERY WHERE query_id = '" + queryId + "' ASYNC");
            response.body().close();
        } catch (Exception e) {
            // Cancellation is advisory; the stream close already stops local consumption.
        }
    }

    private HttpResponse<InputStream> send(String url, String body) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
            .timeout(requestTimeout)
            .header("X-ClickHouse-User", username)
            .header("X-ClickHouse-Key", password)
            .header("Content-Type", "text/plain; charset=UTF-8")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while calling ClickHouse at [" + endpoint + "]", e);
        }
    }

    @Override
    public void close() {
        // JDK HttpClient (17+) has no explicit close in all versions; on 21+ it is AutoCloseable.
        if (client instanceof AutoCloseable ac) {
            try {
                ac.close();
            } catch (Exception e) {
                // best effort
            }
        }
    }
}
