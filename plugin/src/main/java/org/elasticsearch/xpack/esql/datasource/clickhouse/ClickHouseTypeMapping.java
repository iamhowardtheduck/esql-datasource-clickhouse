/*
 * ClickHouse connector for ES|QL Data Federation.
 * Near-verbatim adaptation of org.elasticsearch.xpack.esql.datasource.grpc.FlightTypeMapping @ v9.5.4 —
 * ClickHouse's ArrowStream output and Arrow Flight batches are the same wire objects,
 * so the conversion into ES|QL Blocks is shared via ArrowToEsql in the core esql plugin.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.arrow.ArrowToEsql;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps between Apache Arrow types (as emitted by ClickHouse {@code FORMAT ArrowStream})
 * and ESQL types. Handles both schema conversion (Arrow Field to ESQL Attribute) and
 * data conversion (Arrow FieldVector to ESQL Block).
 *
 * <p>ClickHouse type notes:
 * <ul>
 *   <li>{@code String} arrives as Utf8 when {@code output_format_arrow_string_as_string=1}
 *       (the connector always sets it) → keyword.</li>
 *   <li>{@code Nullable(T)} arrives as a nullable Arrow field → Nullability.TRUE.</li>
 *   <li>{@code DateTime}/{@code DateTime64} arrive as Arrow timestamps → datetime.</li>
 *   <li>Unsupported Arrow types (e.g. dictionary, nested Map/Tuple) fail fast with a clear
 *       message — project them away or cast in a ClickHouse view.</li>
 * </ul>
 */
final class ClickHouseTypeMapping {

    private ClickHouseTypeMapping() {}

    /**
     * Convert an Arrow schema into ES|QL attributes, honoring Arrow's field-level nullability
     * flag (ClickHouse's {@code Nullable(...)} wrapper). Defaulting to non-nullable would
     * mislead planner rules (COALESCE simplification, IS NULL / IS NOT NULL rewriting) into
     * dropping legitimate null rows.
     */
    static List<Attribute> toAttributes(Schema schema) {
        List<Attribute> attributes = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            var mapping = ArrowToEsql.forField(field);
            if (mapping == null) {
                throw new IllegalArgumentException(
                    "Unsupported Arrow vector type from ClickHouse: " + field.getType() + " (column [" + field.getName() + "])"
                );
            }
            Nullability nullability = field.isNullable() ? Nullability.TRUE : Nullability.FALSE;
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, field.getName(), mapping.dataType(), nullability, null, false));
        }
        return attributes;
    }

    static <V extends ValueVector> V transfer(V vector, BlockFactory blockFactory) {
        var tp = vector.getTransferPair(blockFactory.arrowAllocator());
        tp.transfer();
        @SuppressWarnings("unchecked")
        var result = (V) tp.getTo();
        return result;
    }

    static Block toBlock(FieldVector arrowVector, int rowCount, BlockFactory blockFactory) {
        // Trim the vector to the expected size (doesn't shrink buffers)
        if (arrowVector.getValueCount() > rowCount) {
            arrowVector.setValueCount(rowCount);
        }

        // The stream reader's root vectors are reused across batches; transfer to the block
        // factory's allocator so Blocks outlive the reader, exactly as the Flight connector does.
        try (var vector = transfer(arrowVector, blockFactory)) {
            var mapping = ArrowToEsql.forField(arrowVector.getField());
            if (mapping == null) {
                throw new IllegalArgumentException("Unsupported Arrow vector type from ClickHouse: " + vector.getField().getType());
            }
            return mapping.convert(vector, blockFactory);
        }
    }
}
