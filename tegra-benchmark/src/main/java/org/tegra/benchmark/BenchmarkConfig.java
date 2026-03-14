package org.tegra.benchmark;

/**
 * Configuration for benchmark experiments.
 *
 * @param graphScale     R-MAT scale parameter (num vertices = 2^scale)
 * @param edgeFactor     edges per vertex
 * @param numSnapshots   number of snapshots to create
 * @param mutationRate   fraction of edges to mutate per snapshot
 * @param maxIterations  max algorithm iterations
 * @param algorithmName  algorithm to run
 */
public record BenchmarkConfig(
        int graphScale,
        int edgeFactor,
        int numSnapshots,
        double mutationRate,
        int maxIterations,
        String algorithmName
) {

    /**
     * Small configuration for quick tests.
     */
    public static BenchmarkConfig small() {
        return new BenchmarkConfig(10, 16, 10, 0.01, 20, "PageRank");
    }

    /**
     * Medium configuration for more thorough benchmarks.
     */
    public static BenchmarkConfig medium() {
        return new BenchmarkConfig(14, 16, 50, 0.01, 20, "PageRank");
    }
}
