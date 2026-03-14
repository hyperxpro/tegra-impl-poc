package org.tegra.benchmark;

import java.util.Map;

/**
 * Results from a single benchmark experiment.
 *
 * @param experimentName           name of the experiment
 * @param totalTimeMs              total wall-clock time in milliseconds
 * @param avgSnapshotRetrievalMs   average snapshot retrieval latency in milliseconds
 * @param fullComputationMs        time for full (non-incremental) computation in milliseconds
 * @param incrementalComputationMs time for incremental computation in milliseconds
 * @param speedup                  ratio of full to incremental computation time
 * @param peakMemoryBytes          peak memory usage in bytes
 * @param numSnapshots             number of snapshots processed
 * @param extraMetrics             additional experiment-specific metrics
 */
public record BenchmarkResult(
        String experimentName,
        long totalTimeMs,
        long avgSnapshotRetrievalMs,
        long fullComputationMs,
        long incrementalComputationMs,
        double speedup,
        long peakMemoryBytes,
        int numSnapshots,
        Map<String, Object> extraMetrics
) {}
