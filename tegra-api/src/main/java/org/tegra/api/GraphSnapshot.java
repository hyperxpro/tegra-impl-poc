package org.tegra.api;

import org.tegra.pds.hamt.PersistentHAMT;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Immutable point-in-time view of a graph, backed by persistent data structures.
 * <p>
 * Structural sharing via HAMT ensures that creating new snapshots from existing
 * ones is O(log n) in space and time for each mutation.
 *
 * @param <V> the vertex property type
 * @param <E> the edge property type
 */
public final class GraphSnapshot<V, E> implements GraphView<V, E> {

    private final PersistentHAMT<Long, V> vertexData;
    private final PersistentHAMT<Long, List<Edge<E>>> outEdgeIndex;
    private final PersistentHAMT<Long, List<Edge<E>>> inEdgeIndex;
    private final SnapshotId id;

    /**
     * Creates a new {@code GraphSnapshot} from the given persistent data structures.
     *
     * @param vertexData   vertex ID to vertex properties
     * @param outEdgeIndex vertex ID to outgoing edge list
     * @param inEdgeIndex  vertex ID to incoming edge list
     * @param id           the snapshot identifier
     * @param <V>          vertex property type
     * @param <E>          edge property type
     * @return a new immutable graph snapshot
     */
    public static <V, E> GraphSnapshot<V, E> create(
            PersistentHAMT<Long, V> vertexData,
            PersistentHAMT<Long, List<Edge<E>>> outEdgeIndex,
            PersistentHAMT<Long, List<Edge<E>>> inEdgeIndex,
            SnapshotId id) {
        return new GraphSnapshot<>(vertexData, outEdgeIndex, inEdgeIndex, id);
    }

    /**
     * Package-private constructor used by {@link MutableGraphView} and {@link Timelapse}.
     */
    GraphSnapshot(PersistentHAMT<Long, V> vertexData,
                  PersistentHAMT<Long, List<Edge<E>>> outEdgeIndex,
                  PersistentHAMT<Long, List<Edge<E>>> inEdgeIndex,
                  SnapshotId id) {
        this.vertexData = vertexData;
        this.outEdgeIndex = outEdgeIndex;
        this.inEdgeIndex = inEdgeIndex;
        this.id = id;
    }

    /**
     * Returns this snapshot's identifier.
     */
    public SnapshotId id() {
        return id;
    }

    /**
     * Creates a mutable working copy of this snapshot for making changes.
     */
    public MutableGraphView<V, E> asMutable() {
        return new MutableGraphView<>(vertexData, outEdgeIndex, inEdgeIndex);
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

    /**
     * Returns the underlying vertex data (package-private for diff computation).
     */
    PersistentHAMT<Long, V> vertexData() {
        return vertexData;
    }

    /**
     * Returns the underlying outgoing edge index (package-private for diff computation).
     */
    PersistentHAMT<Long, List<Edge<E>>> outEdgeIndex() {
        return outEdgeIndex;
    }

    /**
     * Returns the underlying incoming edge index (package-private for diff computation).
     */
    PersistentHAMT<Long, List<Edge<E>>> inEdgeIndex() {
        return inEdgeIndex;
    }
}
