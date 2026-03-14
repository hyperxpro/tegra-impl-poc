package org.tegra.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Serializes and deserializes {@link EdgeData} (edge key + property map).
 * <p>
 * Wire format:
 * <pre>
 *   [srcId: long] [dstId: long] [discriminator: short]
 *   [properties: PropertyMap]
 * </pre>
 */
public final class EdgeDataSerializer implements TegraSerializer<EdgeData> {

    private final PropertyMapSerializer propertyMapSerializer = new PropertyMapSerializer();

    @Override
    public void serialize(EdgeData value, DataOutput out) throws IOException {
        EdgeKey ek = value.edgeKey();
        out.writeLong(ek.srcId());
        out.writeLong(ek.dstId());
        out.writeShort(ek.discriminator());
        propertyMapSerializer.serialize(value.properties(), out);
    }

    @Override
    public EdgeData deserialize(DataInput in) throws IOException {
        long srcId = in.readLong();
        long dstId = in.readLong();
        short discriminator = in.readShort();
        EdgeKey edgeKey = new EdgeKey(srcId, dstId, discriminator);
        Map<String, PropertyValue> properties = propertyMapSerializer.deserialize(in);
        return new EdgeData(edgeKey, properties);
    }

    @Override
    public int estimateSize(EdgeData value) {
        return 18 + propertyMapSerializer.estimateSize(value.properties());
    }
}
