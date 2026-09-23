/*
 * ClickHouse connector for ES|QL Data Federation.
 * Structure mirrors org.elasticsearch.xpack.esql.datasource.grpc.FlightResultCursor @ v9.5.4.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Wraps a ClickHouse {@code FORMAT ArrowStream} HTTP response as a {@link ResultCursor}.
 * Each call to {@link #next()} converts the current {@link VectorSchemaRoot} batch into
 * an ESQL {@link Page} using {@link ClickHouseTypeMapping}.
 */
class ClickHouseResultCursor implements ResultCursor {

    private final ClickHouseHttp http;
    private final String queryId;
    private final InputStream stream;
    private final ArrowStreamReader reader;
    private final List<Attribute> attributes;
    private final BlockFactory blockFactory;
    private boolean hasNextBatch;

    ClickHouseResultCursor(ClickHouseHttp http, String queryId, InputStream stream, List<Attribute> attributes, BlockFactory blockFactory) {
        this.http = http;
        this.queryId = queryId;
        this.stream = stream;
        this.attributes = attributes;
        this.blockFactory = blockFactory;
        this.reader = new ArrowStreamReader(stream, blockFactory.arrowAllocator());
        this.hasNextBatch = advance();
    }

    @Override
    public boolean hasNext() {
        return hasNextBatch;
    }

    @Override
    public Page next() {
        try {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int rowCount = root.getRowCount();
            Block[] blocks = new Block[attributes.size()];
            for (int col = 0; col < attributes.size(); col++) {
                blocks[col] = ClickHouseTypeMapping.toBlock(root.getVector(attributes.get(col).name()), rowCount, blockFactory);
            }
            hasNextBatch = advance();
            return new Page(rowCount, blocks);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading ClickHouse Arrow batch", e);
        }
    }

    private boolean advance() {
        try {
            return reader.loadNextBatch();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed advancing ClickHouse Arrow stream", e);
        }
    }

    @Override
    public void cancel() {
        http.killQuery(queryId);
        try {
            stream.close();
        } catch (IOException e) {
            // stream teardown on cancel is best effort
        }
    }

    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } finally {
            stream.close();
        }
    }
}
