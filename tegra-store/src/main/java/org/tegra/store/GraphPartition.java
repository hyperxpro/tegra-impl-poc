package org.tegra.store;

import org.tegra.api.Edge;
import org.tegra.pds.hamt.PersistentHAMT;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A single partition's mutable graph data backed by persistent data structures.
 * <p>
 * Each mutation produces new HAMT roots via path copying, so snapshots taken
 * before mutations remain valid and unchanged.
 *
 * @param <V> vertex property type
 * @param <E> edge property type
 */
public final class GraphPartition<V, E> {

    private PersistentHAMT<Long, V> vertexData;
    private PersistentHAMT<Long, List<Edge<E>>> outEdges;
    private PersistentHAMT<Long, List<Edge<E>>> inEdges;

    public GraphPartition() {
        this.vertexData = PersistentHAMT.empty();
        this.outEdges = PersistentHAMT.empty();
        this.inEdges = PersistentHAMT.empty();
    }

    /**
     * Adds or updates a vertex with the given properties.
     */
    public void addVertex(long id, V properties) {
        vertexData = vertexData.put(id, properties);
    }

    /**
     * Removes a vertex and all its incident edges.
     */
    public void removeVertex(long id) {
        vertexData = vertexData.remove(id);
        // Remove outgoing edges and corresponding incoming entries
        List<Edge<E>> out = nullable(outEdges.get(id));
        for (Edge<E> edge : out) {
            removeFromInEdges(edge.dst(), id);
        }
        outEdges = outEdges.remove(id);
        // Remove incoming edges and corresponding outgoing entries
        List<Edge<E>> in = nullable(inEdges.get(id));
        for (Edge<E> edge : in) {
            removeFromOutEdges(edge.src(), id);
        }
        inEdges = inEdges.remove(id);
    }

    /**
     * Adds a directed edge from {@code src} to {@code dst}.
     */
    public void addEdge(long src, long dst, E properties) {
        Edge<E> edge = new Edge<>(src, dst, properties);

        List<Edge<E>> currentOut = new ArrayList<>(nullable(outEdges.get(src)));
        currentOut.add(edge);
        outEdges = outEdges.put(src, Collections.unmodifiableList(currentOut));

        List<Edge<E>> currentIn = new ArrayList<>(nullable(inEdges.get(dst)));
        currentIn.add(edge);
        inEdges = inEdges.put(dst, Collections.unmodifiableList(currentIn));
    }

    /**
     * Removes the directed edge from {@code src} to {@code dst}.
     */
    public void removeEdge(long src, long dst) {
        removeFromOutEdges(src, dst);
        removeFromInEdges(dst, src);
    }

    /**
     * Returns the vertex properties, or {@code null} if absent.
     */
    public V getVertex(long id) {
        return vertexData.get(id);
    }

    /**
     * Returns the outgoing edges from the given vertex.
     */
    public List<Edge<E>> getOutEdges(long vertexId) {
        return nullable(outEdges.get(vertexId));
    }

    /**
     * Returns the incoming edges to the given vertex.
     */
    public List<Edge<E>> getInEdges(long vertexId) {
        return nullable(inEdges.get(vertexId));
    }

    /**
     * Takes a point-in-time snapshot of this partition's current state.
     */
    public VersionRoot<V, E> snapshot() {
        return new VersionRoot<>(vertexData, outEdges, inEdges, Instant.now());
    }

    /**
     * Returns the number of vertices in this partition.
     */
    public long vertexCount() {
        return vertexData.size();
    }

    /**
     * Returns the total number of edges in this partition.
     */
    public long edgeCount() {
        long[] count = {0};
        outEdges.forEach((k, edges) -> count[0] += edges.size());
        return count[0];
    }

    private void removeFromOutEdges(long src, long dst) {
        List<Edge<E>> current = nullable(outEdges.get(src));
        List<Edge<E>> filtered = current.stream()
                .filter(e -> e.dst() != dst)
                .toList();
        if (filtered.isEmpty()) {
            outEdges = outEdges.remove(src);
        } else {
            outEdges = outEdges.put(src, filtered);
        }
    }

    private void removeFromInEdges(long dst, long src) {
        List<Edge<E>> current = nullable(inEdges.get(dst));
        List<Edge<E>> filtered = current.stream()
                .filter(e -> e.src() != src)
                .toList();
        if (filtered.isEmpty()) {
            inEdges = inEdges.remove(dst);
        } else {
            inEdges = inEdges.put(dst, filtered);
        }
    }

    private static <T> List<T> nullable(List<T> list) {
        return list != null ? list : List.of();
    }
}
