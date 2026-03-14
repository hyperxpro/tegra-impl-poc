package org.tegra.store;

import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.KeyCodec;
import org.tegra.serde.VertexData;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Read-only snapshot access backed by immutable pART roots.
 * Multiple threads can concurrently read from the same GraphView
 * without synchronization, since all data is immutable.
 */
public final class GraphView {

    private final ArtNode<VertexData> vertexRoot;
    private final ArtNode<EdgeData> edgeRoot;
    private final ByteArray versionId;

    public GraphView(ArtNode<VertexData> vertexRoot, ArtNode<EdgeData> edgeRoot, ByteArray versionId) {
        this.vertexRoot = vertexRoot;
        this.edgeRoot = edgeRoot;
        this.versionId = versionId;
    }

    /**
     * Returns the version ID of this snapshot.
     */
    public ByteArray versionId() {
        return versionId;
    }

    /**
     * Returns the vertex root node (may be null for empty graph).
     */
    public ArtNode<VertexData> vertexRoot() {
        return vertexRoot;
    }

    /**
     * Returns the edge root node (may be null for empty graph).
     */
    public ArtNode<EdgeData> edgeRoot() {
        return edgeRoot;
    }

    /**
     * Looks up a vertex by ID.
     *
     * @return the vertex data, or null if not found
     */
    public VertexData vertex(long vertexId) {
        PersistentART<VertexData> art = PersistentART.fromRoot(vertexRoot);
        return art.lookup(KeyCodec.encodeVertexKey(vertexId));
    }

    /**
     * Looks up an edge by source, destination, and discriminator.
     *
     * @return the edge data, or null if not found
     */
    public EdgeData edge(long srcId, long dstId, short disc) {
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        return art.lookup(KeyCodec.encodeEdgeKey(srcId, dstId, disc));
    }

    /**
     * Returns an iterator over all vertices.
     */
    public Iterator<VertexData> vertices() {
        List<VertexData> result = new ArrayList<>();
        PersistentART<VertexData> art = PersistentART.fromRoot(vertexRoot);
        art.forEach((k, v) -> result.add(v));
        return result.iterator();
    }

    /**
     * Returns an iterator over all edges.
     */
    public Iterator<EdgeData> edges() {
        List<EdgeData> result = new ArrayList<>();
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        art.forEach((k, v) -> result.add(v));
        return result.iterator();
    }

    /**
     * Returns an iterator over all outgoing edges from the given vertex.
     * Uses prefix matching on the edge ART with the source vertex ID prefix.
     */
    public Iterator<EdgeData> outEdges(long vertexId) {
        List<EdgeData> result = new ArrayList<>();
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        byte[] prefix = KeyCodec.edgeSourcePrefix(vertexId);
        art.prefixIterator(prefix, (k, v) -> result.add(v));
        return result.iterator();
    }

    /**
     * Returns an iterator over all incoming edges to the given vertex.
     * Currently uses scan-based approach (iterates all edges and filters).
     */
    public Iterator<EdgeData> inEdges(long vertexId) {
        List<EdgeData> result = new ArrayList<>();
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        art.forEach((k, v) -> {
            if (v.edgeKey().dstId() == vertexId) {
                result.add(v);
            }
        });
        return result.iterator();
    }

    /**
     * Returns the number of vertices in this snapshot.
     */
    public long vertexCount() {
        PersistentART<VertexData> art = PersistentART.fromRoot(vertexRoot);
        return art.size();
    }

    /**
     * Returns the number of edges in this snapshot.
     */
    public long edgeCount() {
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        return art.size();
    }
}
