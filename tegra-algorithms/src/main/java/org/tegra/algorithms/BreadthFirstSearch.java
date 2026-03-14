package org.tegra.algorithms;

import org.tegra.api.EdgeDirection;
import org.tegra.compute.gas.VertexProgram;

/**
 * Breadth-first search from a source vertex in the GAS model.
 * <p>
 * Computes the hop distance from the source vertex to all reachable vertices.
 * The source is initialized to depth 0; all others to {@link Integer#MAX_VALUE}.
 * Each superstep propagates {@code neighborDepth + 1} along outgoing edges.
 *
 * @param <E> edge property type (unused)
 */
public final class BreadthFirstSearch<E> implements VertexProgram<Integer, E, Integer> {

    private final long sourceVertex;

    /**
     * Creates a BFS program from the given source vertex.
     *
     * @param sourceVertex the vertex ID from which to compute BFS depths
     */
    public BreadthFirstSearch(long sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    @Override
    public String name() {
        return "BFS";
    }

    @Override
    public EdgeDirection gatherDirection() {
        return EdgeDirection.IN;
    }

    @Override
    public EdgeDirection scatterDirection() {
        return EdgeDirection.OUT;
    }

    @Override
    public Integer gather(Integer vertexValue, E edgeValue, Integer neighborValue) {
        return neighborValue == Integer.MAX_VALUE ? Integer.MAX_VALUE : neighborValue + 1;
    }

    @Override
    public Integer sum(Integer a, Integer b) {
        return Math.min(a, b);
    }

    @Override
    public Integer apply(Integer currentValue, Integer gathered) {
        return Math.min(currentValue, gathered);
    }

    @Override
    public boolean scatter(Integer updatedValue, Integer oldValue, E edgeValue) {
        return !updatedValue.equals(oldValue);
    }

    @Override
    public Integer identity() {
        return Integer.MAX_VALUE;
    }

    /**
     * Returns the initial depth value for the given vertex.
     * The source vertex gets 0; all others get {@link Integer#MAX_VALUE}.
     *
     * @param vertexId the vertex identifier
     * @return initial BFS depth
     */
    public Integer initialValue(long vertexId) {
        return vertexId == sourceVertex ? 0 : Integer.MAX_VALUE;
    }

    /**
     * Returns the source vertex ID.
     */
    public long sourceVertex() {
        return sourceVertex;
    }
}
