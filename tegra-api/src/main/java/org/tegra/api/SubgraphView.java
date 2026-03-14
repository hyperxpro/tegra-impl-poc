package org.tegra.api;

import org.tegra.serde.EdgeData;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * View of a subgraph with active and boundary vertex classification.
 * Active vertices are those that must recompute, boundary vertices are
 * their 1-hop neighbors (used for gather phase in GAS computations).
 */
public final class SubgraphView {

    private final Set<Long> activeVertices;
    private final Set<Long> boundaryVertices;
    private final GraphView backingGraph;

    public SubgraphView(Set<Long> activeVertices, Set<Long> boundaryVertices, GraphView backingGraph) {
        this.activeVertices = Set.copyOf(activeVertices);
        this.boundaryVertices = Set.copyOf(boundaryVertices);
        this.backingGraph = backingGraph;
    }

    /**
     * Returns true if the vertex is active (needs to recompute).
     */
    public boolean isActive(long vertexId) {
        return activeVertices.contains(vertexId);
    }

    /**
     * Returns true if the vertex is a boundary vertex (1-hop neighbor of active).
     */
    public boolean isBoundary(long vertexId) {
        return boundaryVertices.contains(vertexId);
    }

    /**
     * Returns the set of active vertex IDs.
     */
    public Set<Long> activeVertexIds() {
        return activeVertices;
    }

    /**
     * Returns the set of boundary vertex IDs.
     */
    public Set<Long> boundaryVertexIds() {
        return boundaryVertices;
    }

    /**
     * Returns an iterator over active vertex data.
     */
    public Iterator<VertexData> activeVertices() {
        List<VertexData> result = new ArrayList<>();
        for (long id : activeVertices) {
            VertexData vd = backingGraph.vertex(id);
            if (vd != null) {
                result.add(vd);
            }
        }
        return result.iterator();
    }

    /**
     * Returns an iterator over boundary vertex data.
     */
    public Iterator<VertexData> boundaryVertices() {
        List<VertexData> result = new ArrayList<>();
        for (long id : boundaryVertices) {
            VertexData vd = backingGraph.vertex(id);
            if (vd != null) {
                result.add(vd);
            }
        }
        return result.iterator();
    }

    /**
     * Returns an iterator over edges relevant to the subgraph
     * (edges where at least one endpoint is active or boundary).
     */
    public Iterator<EdgeData> relevantEdges() {
        List<EdgeData> result = new ArrayList<>();
        Iterator<EdgeData> all = backingGraph.edges();
        while (all.hasNext()) {
            EdgeData ed = all.next();
            long src = ed.edgeKey().srcId();
            long dst = ed.edgeKey().dstId();
            if (activeVertices.contains(src) || activeVertices.contains(dst)
                    || boundaryVertices.contains(src) || boundaryVertices.contains(dst)) {
                result.add(ed);
            }
        }
        return result.iterator();
    }

    /**
     * Returns the backing graph view.
     */
    public GraphView backingGraph() {
        return backingGraph;
    }
}
