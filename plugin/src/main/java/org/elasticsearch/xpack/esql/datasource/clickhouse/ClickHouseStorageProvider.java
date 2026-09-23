/*
 * ClickHouse connector for ES|QL Data Federation.
 * Structure mirrors org.elasticsearch.xpack.esql.datasource.grpc.FlightStorageProvider @ v9.5.4.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Minimal {@link StorageProvider} for {@code clickhouse://} and {@code ch://} locations.
 * <p>
 * ClickHouse is not a byte-addressable blob store; ESQL reads ClickHouse data through the
 * {@link org.elasticsearch.xpack.esql.datasources.spi.Connector} instead. This provider
 * exists so ExternalSourceResolver can register a concrete FileList entry (length / mtime)
 * and satisfy the storage SPI contract — the same role FlightStorageProvider plays for
 * Arrow Flight sources.
 */
public final class ClickHouseStorageProvider implements StorageProvider {

    @Override
    public StorageObject newObject(StoragePath path) {
        validateScheme(path);
        return new ClickHouseStorageObject(path, 0L, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateScheme(path);
        return new ClickHouseStorageObject(path, length, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateScheme(path);
        return new ClickHouseStorageObject(path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("ClickHouse does not support directory listing");
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateScheme(path);
        // Existence is verified for real at schema-resolution time by the connector's
        // authenticated LIMIT 0 probe; this unauthenticated hook reports the location as
        // present so resolution proceeds to that probe.
        return true;
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("clickhouse", "ch");
    }

    @Override
    public boolean supportsStableMetadata() {
        // A live table has no per-object last-modified identity to key a schema/stats cache
        // entry on; bypass caching rather than cache under an unknowable version.
        return false;
    }

    @Override
    public void close() {
        // Nothing to close; the connector owns the HTTP client.
    }

    private static void validateScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if ("clickhouse".equals(scheme) == false && "ch".equals(scheme) == false) {
            throw new IllegalArgumentException("ClickHouseStorageProvider only supports clickhouse:// and ch:// schemes, got: " + scheme);
        }
    }

    private record ClickHouseStorageObject(StoragePath path, long knownLength, Instant knownLastModified) implements StorageObject {

        @Override
        public InputStream newStream() throws IOException {
            throw notByteAddressable();
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            throw notByteAddressable();
        }

        private static IOException notByteAddressable() {
            return new IOException("ClickHouse sources are read via the ClickHouse connector, not as byte streams");
        }

        @Override
        public long length() throws IOException {
            return knownLength;
        }

        @Override
        public Instant lastModified() throws IOException {
            return knownLastModified;
        }

        @Override
        public boolean exists() throws IOException {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
