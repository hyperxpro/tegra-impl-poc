package org.tegra.api;

import org.tegra.pds.common.DiffEntry;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents the difference between two graph snapshots.
 *
 * @param vertexChanges changes to vertices
 * @param edgeChanges   changes to edges (keyed by source vertex ID)
 * @param <V>           the vertex property type
 * @param <E>           the edge property type
 */
public record GraphDelta<V, E>(
        List<DiffEntry<Long, V>> vertexChanges,
        List<DiffEntry<Long, E>> edgeChanges
) {

    /**
     * Returns the set of vertex IDs that were directly changed.
     */
    public Set<Long> changedVertexIds() {
        Set<Long> ids = new HashSet<>();
        for (var vc : vertexChanges) {
            ids.add(vc.key());
        }
        return ids;
    }

    /**
     * Returns the set of vertex IDs affected by either vertex or edge changes.
     */
    public Set<Long> affectedVertexIds() {
        Set<Long> ids = changedVertexIds();
        for (var ec : edgeChanges) {
            ids.add(ec.key());
        }
        return ids;
    }
}
