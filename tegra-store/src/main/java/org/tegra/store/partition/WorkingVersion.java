package org.tegra.store.partition;

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
import java.util.Map;

/**
 * Mutable graph state between branch and commit.
 * Holds mutable ArtNode references for vertex and edge trees.
 * Each mutation uses PersistentART.insert/remove to create new tree roots via path-copying.
 * <p>
 * This class is NOT thread-safe -- a working version is exclusive to the caller.
 */
public final class WorkingVersion {

    private ArtNode<VertexData> vertexRoot;
    private ArtNode<EdgeData> edgeRoot;
    private final ByteArray sourceVersionId;

    public WorkingVersion(ArtNode<VertexData> vertexRoot, ArtNode<EdgeData> edgeRoot, ByteArray sourceVersionId) {
        this.vertexRoot = vertexRoot;
        this.edgeRoot = edgeRoot;
        this.sourceVersionId = sourceVersionId;
    }

    /**
     * Returns the source version ID that this working version was branched from.
     */
    public ByteArray sourceVersionId() {
        return sourceVersionId;
    }

    /**
     * Returns the current vertex root.
     */
    public ArtNode<VertexData> vertexRoot() {
        return vertexRoot;
    }

    /**
     * Returns the current edge root.
     */
    public ArtNode<EdgeData> edgeRoot() {
        return edgeRoot;
    }

    /**
     * Adds or updates a vertex.
     */
    public void putVertex(long vertexId, VertexData data) {
        PersistentART<VertexData> art = PersistentART.fromRoot(vertexRoot);
        art = art.insert(KeyCodec.encodeVertexKey(vertexId), data);
        this.vertexRoot = art.root();
    }

    /**
     * Removes a vertex.
     */
    public void removeVertex(long vertexId) {
        PersistentART<VertexData> art = PersistentART.fromRoot(vertexRoot);
        art = art.remove(KeyCodec.encodeVertexKey(vertexId));
        this.vertexRoot = art.root();
    }

    /**
     * Adds or updates an edge.
     */
    public void putEdge(long srcId, long dstId, short disc, EdgeData data) {
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        art = art.insert(KeyCodec.encodeEdgeKey(srcId, dstId, disc), data);
        this.edgeRoot = art.root();
    }

    /**
     * Removes an edge.
     */
    public void removeEdge(long srcId, long dstId, short disc) {
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        art = art.remove(KeyCodec.encodeEdgeKey(srcId, dstId, disc));
        this.edgeRoot = art.root();
    }

    /**
     * Looks up a vertex by ID.
     *
     * @return the vertex data, or null if not found
     */
    public VertexData getVertex(long vertexId) {
        PersistentART<VertexData> art = PersistentART.fromRoot(vertexRoot);
        return art.lookup(KeyCodec.encodeVertexKey(vertexId));
    }

    /**
     * Looks up an edge by source, destination, and discriminator.
     *
     * @return the edge data, or null if not found
     */
    public EdgeData getEdge(long srcId, long dstId, short disc) {
        PersistentART<EdgeData> art = PersistentART.fromRoot(edgeRoot);
        return art.lookup(KeyCodec.encodeEdgeKey(srcId, dstId, disc));
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
}
