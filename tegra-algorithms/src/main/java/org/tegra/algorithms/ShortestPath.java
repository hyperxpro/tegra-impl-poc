package org.tegra.algorithms;

import org.tegra.api.EdgeDirection;
import org.tegra.compute.gas.VertexProgram;

/**
 * Single-source shortest path (SSSP) via Bellman-Ford in the GAS model.
 * <p>
 * Edge properties are interpreted as weights (distances). The source vertex
 * is initialized to distance 0.0; all others to {@link Double#MAX_VALUE}.
 * Each superstep relaxes distances by gathering the minimum
 * {@code neighborDistance + edgeWeight} from in-neighbors.
 */
public final class ShortestPath implements VertexProgram<Double, Double, Double> {

    private final long sourceVertex;

    /**
     * Creates an SSSP program from the given source vertex.
     *
     * @param sourceVertex the vertex ID from which to compute shortest paths
     */
    public ShortestPath(long sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    @Override
    public String name() {
        return "SSSP";
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
    public Double gather(Double vertexValue, Double edgeValue, Double neighborValue) {
        return neighborValue + edgeValue;
    }

    @Override
    public Double sum(Double a, Double b) {
        return Math.min(a, b);
    }

    @Override
    public Double apply(Double currentValue, Double gathered) {
        return Math.min(currentValue, gathered);
    }

    @Override
    public boolean scatter(Double updatedValue, Double oldValue, Double edgeValue) {
        return !updatedValue.equals(oldValue);
    }

    @Override
    public Double identity() {
        return Double.MAX_VALUE;
    }

    /**
     * Returns the initial distance value for the given vertex.
     * The source vertex gets 0.0; all others get {@link Double#MAX_VALUE}.
     *
     * @param vertexId the vertex identifier
     * @return initial distance
     */
    public Double initialValue(long vertexId) {
        return vertexId == sourceVertex ? 0.0 : Double.MAX_VALUE;
    }

    /**
     * Returns the source vertex ID.
     */
    public long sourceVertex() {
        return sourceVertex;
    }
}
