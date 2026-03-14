package org.tegra.api;

import org.tegra.pds.hamt.PersistentHAMT;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Mutable working copy of a graph that produces new immutable snapshots.
 * <p>
 * Each mutation creates a new version of the affected persistent data structure,
 * preserving structural sharing with previous versions.
 *
 * @param <V> the vertex property type
 * @param <E> the edge property type
 */
public final class MutableGraphView<V, E> implements GraphView<V, E> {

    private PersistentHAMT<Long, V> vertexData;
    private PersistentHAMT<Long, List<Edge<E>>> outEdgeIndex;
    private PersistentHAMT<Long, List<Edge<E>>> inEdgeIndex;

    /**
     * Package-private constructor; obtain via {@link GraphSnapshot#asMutable()}.
     */
    MutableGraphView(PersistentHAMT<Long, V> vertexData,
                     PersistentHAMT<Long, List<Edge<E>>> outEdgeIndex,
                     PersistentHAMT<Long, List<Edge<E>>> inEdgeIndex) {
        this.vertexData = vertexData;
        this.outEdgeIndex = outEdgeIndex;
        this.inEdgeIndex = inEdgeIndex;
    }

    /**
     * Adds or replaces a vertex with the given ID and properties.
     */
    public void addVertex(long id, V properties) {
        vertexData = vertexData.put(id, properties);
    }

    /**
     * Removes a vertex and all its incident edges.
     */
    public void removeVertex(long id) {
        vertexData = vertexData.remove(id);
        // Remove outgoing edges from this vertex
        List<Edge<E>> outList = outEdgeIndex.get(id);
        if (outList != null) {
            for (Edge<E> edge : outList) {
                removeFromInEdgeIndex(edge.dst(), id);
            }
            outEdgeIndex = outEdgeIndex.remove(id);
        }
        // Remove incoming edges to this vertex
        List<Edge<E>> inList = inEdgeIndex.get(id);
        if (inList != null) {
            for (Edge<E> edge : inList) {
                removeFromOutEdgeIndex(edge.src(), id);
            }
            inEdgeIndex = inEdgeIndex.remove(id);
        }
    }

    /**
     * Adds an edge from src to dst with the given properties.
     */
    public void addEdge(long src, long dst, E properties) {
        Edge<E> edge = new Edge<>(src, dst, properties);

        // Update outgoing edge index
        List<Edge<E>> outEdges = outEdgeIndex.get(src);
        List<Edge<E>> newOut = new ArrayList<>(outEdges != null ? outEdges : List.of());
        newOut.add(edge);
        outEdgeIndex = outEdgeIndex.put(src, List.copyOf(newOut));

        // Update incoming edge index
        List<Edge<E>> inEdgesForDst = inEdgeIndex.get(dst);
        List<Edge<E>> newIn = new ArrayList<>(inEdgesForDst != null ? inEdgesForDst : List.of());
        newIn.add(edge);
        inEdgeIndex = inEdgeIndex.put(dst, List.copyOf(newIn));
    }

    /**
     * Removes the edge from src to dst.
     */
    public void removeEdge(long src, long dst) {
        // Update outgoing edge index
        List<Edge<E>> outList = outEdgeIndex.get(src);
        if (outList != null) {
            List<Edge<E>> filtered = outList.stream()
                    .filter(e -> e.dst() != dst)
                    .toList();
            if (filtered.isEmpty()) {
                outEdgeIndex = outEdgeIndex.remove(src);
            } else {
                outEdgeIndex = outEdgeIndex.put(src, filtered);
            }
        }

        // Update incoming edge index
        List<Edge<E>> inList = inEdgeIndex.get(dst);
        if (inList != null) {
            List<Edge<E>> filtered = inList.stream()
                    .filter(e -> e.src() != src)
                    .toList();
            if (filtered.isEmpty()) {
                inEdgeIndex = inEdgeIndex.remove(dst);
            } else {
                inEdgeIndex = inEdgeIndex.put(dst, filtered);
            }
        }
    }

    /**
     * Updates the properties of an existing vertex.
     *
     * @throws IllegalArgumentException if the vertex does not exist
     */
    public void setVertexProperty(long id, V properties) {
        if (!vertexData.containsKey(id)) {
            throw new IllegalArgumentException("Vertex " + id + " does not exist");
        }
        vertexData = vertexData.put(id, properties);
    }

    /**
     * Freezes this mutable view into an immutable snapshot with the given ID.
     */
    public GraphSnapshot<V, E> toSnapshot(SnapshotId id) {
        return new GraphSnapshot<>(vertexData, outEdgeIndex, inEdgeIndex, id);
    }

    @Override
    public long vertexCount() {
        return vertexData.size();
    }

    @Override
    public long edgeCount() {
        long[] count = {0};
        outEdgeIndex.forEach((k, edges) -> count[0] += edges.size());
        return count[0];
    }

    @Override
    public Optional<Vertex<V>> vertex(long id) {
        V props = vertexData.get(id);
        return props != null ? Optional.of(new Vertex<>(id, props)) : Optional.empty();
    }

    @Override
    public Stream<Vertex<V>> vertices() {
        List<Vertex<V>> result = new ArrayList<>();
        vertexData.forEach((id, props) -> result.add(new Vertex<>(id, props)));
        return result.stream();
    }

    @Override
    public Stream<Edge<E>> outEdges(long vertexId) {
        List<Edge<E>> edges = outEdgeIndex.get(vertexId);
        return edges != null ? edges.stream() : Stream.empty();
    }

    @Override
    public Stream<Edge<E>> inEdges(long vertexId) {
        List<Edge<E>> edges = inEdgeIndex.get(vertexId);
        return edges != null ? edges.stream() : Stream.empty();
    }

    @Override
    public Stream<Edge<E>> edges() {
        List<Edge<E>> all = new ArrayList<>();
        outEdgeIndex.forEach((k, edgeList) -> all.addAll(edgeList));
        return all.stream();
    }

    private void removeFromInEdgeIndex(long vertexId, long srcToRemove) {
        List<Edge<E>> edges = inEdgeIndex.get(vertexId);
        if (edges != null) {
            List<Edge<E>> filtered = edges.stream()
                    .filter(e -> e.src() != srcToRemove)
                    .toList();
            if (filtered.isEmpty()) {
                inEdgeIndex = inEdgeIndex.remove(vertexId);
            } else {
                inEdgeIndex = inEdgeIndex.put(vertexId, filtered);
            }
        }
    }

    private void removeFromOutEdgeIndex(long vertexId, long dstToRemove) {
        List<Edge<E>> edges = outEdgeIndex.get(vertexId);
        if (edges != null) {
            List<Edge<E>> filtered = edges.stream()
                    .filter(e -> e.dst() != dstToRemove)
                    .toList();
            if (filtered.isEmpty()) {
                outEdgeIndex = outEdgeIndex.remove(vertexId);
            } else {
                outEdgeIndex = outEdgeIndex.put(vertexId, filtered);
            }
        }
    }
}
