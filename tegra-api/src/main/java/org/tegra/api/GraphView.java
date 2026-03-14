package org.tegra.api;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Sealed read-only interface for graph traversal.
 *
 * @param <V> the vertex property type
 * @param <E> the edge property type
 */
public sealed interface GraphView<V, E> permits GraphSnapshot, MutableGraphView {

    /**
     * Returns the number of vertices in this graph.
     */
    long vertexCount();

    /**
     * Returns the number of edges in this graph.
     */
    long edgeCount();

    /**
     * Returns the vertex with the given ID, if it exists.
     */
    Optional<Vertex<V>> vertex(long id);

    /**
     * Returns a stream of all vertices.
     */
    Stream<Vertex<V>> vertices();

    /**
     * Returns a stream of outgoing edges from the given vertex.
     */
    Stream<Edge<E>> outEdges(long vertexId);

    /**
     * Returns a stream of incoming edges to the given vertex.
     */
    Stream<Edge<E>> inEdges(long vertexId);

    /**
     * Returns a stream of all edges.
     */
    Stream<Edge<E>> edges();
}
