package org.tegra.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Serializes and deserializes {@link VertexData} (vertex ID + property map).
 * <p>
 * Wire format:
 * <pre>
 *   [vertexId: long]
 *   [properties: PropertyMap]
 * </pre>
 */
public final class VertexDataSerializer implements TegraSerializer<VertexData> {

    private final PropertyMapSerializer propertyMapSerializer = new PropertyMapSerializer();

    @Override
    public void serialize(VertexData value, DataOutput out) throws IOException {
        out.writeLong(value.vertexId());
        propertyMapSerializer.serialize(value.properties(), out);
    }

    @Override
    public VertexData deserialize(DataInput in) throws IOException {
        long vertexId = in.readLong();
        Map<String, PropertyValue> properties = propertyMapSerializer.deserialize(in);
        return new VertexData(vertexId, properties);
    }

    @Override
    public int estimateSize(VertexData value) {
        return 8 + propertyMapSerializer.estimateSize(value.properties());
    }
}
