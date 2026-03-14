package org.tegra.api;

import org.tegra.serde.VertexData;

/**
 * Functional interface for merging two vertex data values.
 * Used when creating a new snapshot via union of two snapshots
 * where both contain the same vertex.
 */
@FunctionalInterface
public interface MergeFunction {

    /**
     * Merges vertex data from two snapshots.
     * Called for vertices present in both snapshots.
     *
     * @param fromA vertex data from snapshot A
     * @param fromB vertex data from snapshot B
     * @return the merged vertex data
     */
    VertexData merge(VertexData fromA, VertexData fromB);
}
