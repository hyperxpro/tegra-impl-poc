package org.tegra.algorithms;

import org.tegra.api.EdgeDirection;
import org.tegra.compute.gas.VertexProgram;

/**
 * PageRank algorithm implemented as a GAS vertex program.
 * <p>
 * Each vertex's rank is iteratively refined by gathering rank contributions
 * from in-neighbors and applying the damping factor formula:
 * {@code rank = (1 - d) + d * sum(neighbor_ranks)}.
 *
 * @param <E> edge property type (unused by PageRank)
 */
public final class PageRank<E> implements VertexProgram<Double, E, Double> {

    private final double dampingFactor;
    private final double tolerance;
    private final int maxIter;

    /**
     * Creates a PageRank program with the specified parameters.
     *
     * @param dampingFactor probability of following a link (typically 0.85)
     * @param tolerance     convergence threshold for per-vertex rank change
     * @param maxIterations maximum number of supersteps
     */
    public PageRank(double dampingFactor, double tolerance, int maxIterations) {
        this.dampingFactor = dampingFactor;
        this.tolerance = tolerance;
        this.maxIter = maxIterations;
    }

    /**
     * Creates a PageRank program with default parameters:
     * damping=0.85, tolerance=1e-6, maxIterations=20.
     */
    public PageRank() {
        this(0.85, 1e-6, 20);
    }

    @Override
    public String name() {
        return "PageRank";
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
    public Double gather(Double vertexValue, E edgeValue, Double neighborValue) {
        return neighborValue;
    }

    @Override
    public Double sum(Double a, Double b) {
        return a + b;
    }

    @Override
    public Double apply(Double currentValue, Double gathered) {
        return (1.0 - dampingFactor) + dampingFactor * gathered;
    }

    @Override
    public boolean scatter(Double updatedValue, Double oldValue, E edgeValue) {
        return Math.abs(updatedValue - oldValue) > tolerance;
    }

    @Override
    public Double identity() {
        return 0.0;
    }

    @Override
    public boolean hasConverged(Double oldValue, Double newValue) {
        return Math.abs(oldValue - newValue) < tolerance;
    }

    @Override
    public int maxIterations() {
        return maxIter;
    }

    /**
     * Returns the damping factor.
     */
    public double dampingFactor() {
        return dampingFactor;
    }

    /**
     * Returns the convergence tolerance.
     */
    public double tolerance() {
        return tolerance;
    }
}
