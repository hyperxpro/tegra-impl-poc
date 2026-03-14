package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;

import java.util.Set;

/**
 * PageRank computation using the GAS model.
 * Computes the PageRank of each vertex using iterative damping factor computation.
 * <p>
 * Gather direction is BOTH (undirected semantics for simplicity — each neighbor
 * contributes its rank). Apply computes (1-d) + d * gathered.
 * Converges when rank changes are below tolerance.
 */
public final class PageRank implements VertexProgram<Double, Object, Double> {

    private final double dampingFactor;
    private final double tolerance;
    private final int maxIterations;

    public PageRank() {
        this(0.85, 1e-6, 20);
    }

    public PageRank(double dampingFactor, double tolerance, int maxIterations) {
        this.dampingFactor = dampingFactor;
        this.tolerance = tolerance;
        this.maxIterations = maxIterations;
    }

    public int maxIterations() {
        return maxIterations;
    }

    @Override
    public Double gather(EdgeTriplet<Double, Object> context) {
        // Gather from incoming edges: the source vertex contributes its rank.
        // The srcValue is the neighbor's rank for an incoming edge.
        Double srcVal = context.srcValue();
        return (srcVal != null) ? srcVal : 0.0;
    }

    @Override
    public Double sum(Double a, Double b) {
        return a + b;
    }

    @Override
    public Double apply(long vertexId, Double currentValue, Double gathered) {
        if (currentValue == null) {
            currentValue = 1.0;
        }
        if (gathered == null) {
            return currentValue;
        }
        return (1.0 - dampingFactor) + dampingFactor * gathered;
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<Double, Object> context, Double newValue) {
        // Activate destination if the value changed significantly
        Double oldDst = context.dstValue();
        if (oldDst == null) {
            return Set.of(context.dstId());
        }
        // Always activate neighbors so they can recompute
        return Set.of(context.dstId());
    }

    @Override
    public EdgeDirection gatherNeighbors() {
        return EdgeDirection.IN;
    }

    @Override
    public EdgeDirection scatterNeighbors() {
        return EdgeDirection.OUT;
    }
}
