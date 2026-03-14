package org.tegra.api;

/**
 * Functional interface for graph algorithms that execute against an immutable snapshot.
 *
 * @param <V> the vertex property type
 * @param <E> the edge property type
 * @param <R> the result type
 */
@FunctionalInterface
public interface GraphAlgorithm<V, E, R> {

    /**
     * Executes this algorithm against the given graph snapshot.
     *
     * @param snapshot the immutable graph snapshot
     * @return the algorithm result
     */
    R execute(GraphSnapshot<V, E> snapshot);
}
