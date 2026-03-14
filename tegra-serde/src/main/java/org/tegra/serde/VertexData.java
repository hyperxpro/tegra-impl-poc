package org.tegra.serde;

import java.util.Map;

/**
 * Data stored for a vertex in the graph: its ID and a property map.
 *
 * @param vertexId   the vertex identifier
 * @param properties the property map (may be empty, never null)
 */
public record VertexData(long vertexId, Map<String, PropertyValue> properties) {

    public VertexData {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
    }
}
