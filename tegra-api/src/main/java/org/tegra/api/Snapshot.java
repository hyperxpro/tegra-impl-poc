package org.tegra.api;

import org.tegra.serde.EdgeData;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.version.ByteArray;

import java.util.Iterator;

/**
 * An immutable graph snapshot with snapshot-aware operators.
 * Wraps a GraphView and provides a higher-level API.
 */
public final class Snapshot {

    private final GraphView graphView;
    private final ByteArray versionId;

    public Snapshot(GraphView graphView, ByteArray versionId) {
        this.graphView = graphView;
        this.versionId = versionId;
    }

    /**
     * Returns the underlying graph view.
     */
    public GraphView graph() {
        return graphView;
    }

    /**
     * Returns the version ID of this snapshot.
     */
    public ByteArray id() {
        return versionId;
    }

    /**
     * Returns an iterator over all vertices in this snapshot.
     */
    public Iterator<VertexData> vertices() {
        return graphView.vertices();
    }

    /**
     * Returns an iterator over all edges in this snapshot.
     */
    public Iterator<EdgeData> edges() {
        return graphView.edges();
    }

    /**
     * Returns the number of vertices in this snapshot.
     */
    public long vertexCount() {
        return graphView.vertexCount();
    }

    /**
     * Returns the number of edges in this snapshot.
     */
    public long edgeCount() {
        return graphView.edgeCount();
    }

    /**
     * Looks up a vertex by ID.
     */
    public VertexData vertex(long vertexId) {
        return graphView.vertex(vertexId);
    }

    /**
     * Looks up an edge.
     */
    public EdgeData edge(long srcId, long dstId, short disc) {
        return graphView.edge(srcId, dstId, disc);
    }

    /**
     * Returns an iterator over outgoing edges from the given vertex.
     */
    public Iterator<EdgeData> outEdges(long vertexId) {
        return graphView.outEdges(vertexId);
    }

    @Override
    public String toString() {
        return "Snapshot[id=" + versionId + ", vertices=" + vertexCount() + ", edges=" + edgeCount() + "]";
    }
}
