package org.tegra.serde;

import java.util.Map;

/**
 * Data stored for an edge in the graph: its key and a property map.
 *
 * @param edgeKey    the composite edge key (src, dst, discriminator)
 * @param properties the property map (may be empty, never null)
 */
public record EdgeData(EdgeKey edgeKey, Map<String, PropertyValue> properties) {

    public EdgeData {
        if (edgeKey == null) {
            throw new IllegalArgumentException("edgeKey must not be null");
        }
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
    }
}
