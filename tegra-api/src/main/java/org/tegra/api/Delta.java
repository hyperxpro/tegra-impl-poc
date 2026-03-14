package org.tegra.api;

import org.tegra.serde.EdgeKey;

import java.util.HashSet;
import java.util.Set;

/**
 * Represents the difference between two graph snapshots.
 * Contains sets of added, removed, and modified vertices and edges.
 *
 * @param addedVertices    vertex IDs present in B but not A
 * @param removedVertices  vertex IDs present in A but not B
 * @param modifiedVertices vertex IDs present in both but with different properties
 * @param addedEdges       edges present in B but not A
 * @param removedEdges     edges present in A but not B
 * @param modifiedEdges    edges present in both but with different properties
 */
public record Delta(
        Set<Long> addedVertices,
        Set<Long> removedVertices,
        Set<Long> modifiedVertices,
        Set<EdgeKey> addedEdges,
        Set<EdgeKey> removedEdges,
        Set<EdgeKey> modifiedEdges
) {

    /**
     * Returns the set of all "affected" vertex IDs: added, removed, modified vertices,
     * plus source and destination vertices of added, removed, and modified edges.
     */
    public Set<Long> affectedVertices() {
        Set<Long> affected = new HashSet<>();
        affected.addAll(addedVertices);
        affected.addAll(removedVertices);
        affected.addAll(modifiedVertices);
        for (EdgeKey ek : addedEdges) {
            affected.add(ek.srcId());
            affected.add(ek.dstId());
        }
        for (EdgeKey ek : removedEdges) {
            affected.add(ek.srcId());
            affected.add(ek.dstId());
        }
        for (EdgeKey ek : modifiedEdges) {
            affected.add(ek.srcId());
            affected.add(ek.dstId());
        }
        return affected;
    }

    /**
     * Returns true if there are no differences.
     */
    public boolean isEmpty() {
        return addedVertices.isEmpty()
                && removedVertices.isEmpty()
                && modifiedVertices.isEmpty()
                && addedEdges.isEmpty()
                && removedEdges.isEmpty()
                && modifiedEdges.isEmpty();
    }
}
