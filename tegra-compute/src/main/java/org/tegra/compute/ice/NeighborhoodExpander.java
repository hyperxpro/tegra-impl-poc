package org.tegra.compute.ice;

import org.tegra.api.SubgraphView;
import org.tegra.compute.gas.EdgeDirection;
import org.tegra.serde.EdgeData;
import org.tegra.store.GraphView;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Expands a set of candidate vertices by 1-hop to include their neighbors.
 * Candidates become active vertices; their neighbors become boundary vertices.
 * Used by ICE to determine the subgraph on which GAS runs.
 */
public final class NeighborhoodExpander {

    /**
     * Expands candidates by 1-hop in the given direction.
     *
     * @param candidates the initial set of candidate vertex IDs
     * @param graph      the graph to expand within
     * @param direction  which edge direction to follow for expansion
     * @return a SubgraphView with active (candidates) and boundary (neighbors) sets
     */
    public SubgraphView expand(Set<Long> candidates, GraphView graph, EdgeDirection direction) {
        Set<Long> activeVertices = new HashSet<>(candidates);
        Set<Long> boundaryVertices = new HashSet<>();

        for (long vertexId : candidates) {
            if (direction == EdgeDirection.OUT || direction == EdgeDirection.BOTH) {
                Iterator<EdgeData> outEdges = graph.outEdges(vertexId);
                while (outEdges.hasNext()) {
                    EdgeData ed = outEdges.next();
                    long dst = ed.edgeKey().dstId();
                    if (!activeVertices.contains(dst)) {
                        boundaryVertices.add(dst);
                    }
                }
            }

            if (direction == EdgeDirection.IN || direction == EdgeDirection.BOTH) {
                Iterator<EdgeData> inEdges = graph.inEdges(vertexId);
                while (inEdges.hasNext()) {
                    EdgeData ed = inEdges.next();
                    long src = ed.edgeKey().srcId();
                    if (!activeVertices.contains(src)) {
                        boundaryVertices.add(src);
                    }
                }
            }
        }

        return new SubgraphView(activeVertices, boundaryVertices, graph);
    }
}
