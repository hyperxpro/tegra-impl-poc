package org.tegra.compute.ice;

import org.tegra.api.GraphSnapshot;

import java.util.HashSet;
import java.util.Set;

/**
 * Expands a set of vertex IDs to include their 1-hop neighborhood
 * (both in-neighbors and out-neighbors).
 */
public final class NeighborhoodExpander {

    /**
     * Expand the given vertex set by one hop in both directions.
     *
     * @param snapshot the graph to look up neighbors in
     * @param vertices the seed set of vertex IDs
     * @param <V>      vertex value type
     * @param <E>      edge value type
     * @return a new set containing the original vertices plus all 1-hop neighbors
     */
    public <V, E> Set<Long> expandOneHop(GraphSnapshot<V, E> snapshot, Set<Long> vertices) {
        Set<Long> expanded = new HashSet<>(vertices);

        for (long vid : vertices) {
            snapshot.outEdges(vid).forEach(edge -> expanded.add(edge.dst()));
            snapshot.inEdges(vid).forEach(edge -> expanded.add(edge.src()));
        }

        return expanded;
    }
}
