package org.tegra.benchmark;

import org.tegra.algorithms.ConnectedComponents;
import org.tegra.algorithms.PageRank;
import org.tegra.benchmark.dataset.GraphLoadResult;
import org.tegra.benchmark.dataset.RmatGraphGenerator;
import org.tegra.benchmark.workload.WorkloadGenerator;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.compute.ice.HeuristicSwitchOracle;
import org.tegra.compute.ice.IceEngine;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.store.GraphView;
import org.tegra.store.log.GraphMutation;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Main benchmark runner that orchestrates experiment execution.
 * Generates graphs, creates snapshots with mutations, runs full and incremental
 * computations, and collects performance metrics.
 */
public final class BenchmarkRunner {

    private final GasEngine gasEngine;
    private final IceEngine iceEngine;
    private final MetricsCollector metrics;

    public BenchmarkRunner() {
        this.gasEngine = new GasEngine();
        this.iceEngine = new IceEngine(gasEngine, new HeuristicSwitchOracle());
        this.metrics = new MetricsCollector();
    }

    /**
     * Runs a single experiment with the given configuration.
     *
     * @param experimentName the name of the experiment
     * @param config         the benchmark configuration
     * @return the benchmark result
     */
    public BenchmarkResult runExperiment(String experimentName, BenchmarkConfig config) {
        long totalStart = System.nanoTime();

        // 1. Generate graph using RmatGraphGenerator
        PartitionStore store = new PartitionStore();
        ByteArray baseVersionId = ByteArray.fromString("base_v0");
        RmatGraphGenerator generator = new RmatGraphGenerator(config.graphScale(), config.edgeFactor());
        GraphLoadResult loadResult = generator.load(store, baseVersionId);

        metrics.recordLatency("graphGeneration", System.nanoTime() - totalStart);

        // 2. Create snapshots with workload generator
        GraphView baseGraph = store.retrieve(baseVersionId);
        WorkloadGenerator workloadGen = new WorkloadGenerator();
        List<List<GraphMutation>> mutationBatches =
                workloadGen.generateEvolution(baseGraph, config.mutationRate(), config.numSnapshots());

        // Apply mutations to create snapshots
        List<GraphView> snapshots = new ArrayList<>();
        snapshots.add(baseGraph);

        ByteArray currentVersionId = baseVersionId;
        for (int i = 0; i < mutationBatches.size(); i++) {
            long snapStart = System.nanoTime();

            WorkingVersion wv = store.branch(currentVersionId);
            for (GraphMutation mutation : mutationBatches.get(i)) {
                applyMutation(wv, mutation);
            }

            ByteArray newVersionId = ByteArray.fromString("snapshot_" + (i + 1));
            store.commit(wv, newVersionId);
            GraphView snapshotView = store.retrieve(newVersionId);
            snapshots.add(snapshotView);
            currentVersionId = newVersionId;

            metrics.recordLatency("snapshotCreation", System.nanoTime() - snapStart);
        }

        // Measure snapshot retrieval latency
        long retrievalTotalNanos = 0;
        int retrievalCount = Math.min(10, snapshots.size());
        for (int i = 0; i < retrievalCount; i++) {
            long retrieveStart = System.nanoTime();
            store.retrieve(snapshots.get(i).versionId());
            long retrieveElapsed = System.nanoTime() - retrieveStart;
            retrievalTotalNanos += retrieveElapsed;
            metrics.recordLatency("snapshotRetrieval", retrieveElapsed);
        }
        long avgRetrievalMs = (retrievalTotalNanos / retrievalCount) / 1_000_000;

        // 3. Get the vertex program
        VertexProgram<?, ?, ?> program = createProgram(config.algorithmName(), config.maxIterations());

        // 4. Run full computation on first snapshot
        long fullStart = System.nanoTime();
        @SuppressWarnings("unchecked")
        Map<Long, Object> fullResult = executeFullComputation(
                snapshots.get(0), (VertexProgram<Object, Object, Object>) program,
                config.maxIterations(), config.algorithmName());
        long fullComputationNanos = System.nanoTime() - fullStart;
        long fullComputationMs = fullComputationNanos / 1_000_000;
        metrics.recordLatency("fullComputation", fullComputationNanos);

        // 5. Run ICE incremental on subsequent snapshots
        long incrementalTotalNanos = 0;
        @SuppressWarnings("unchecked")
        VertexProgram<Object, Object, Object> typedProgram =
                (VertexProgram<Object, Object, Object>) program;
        Map<Long, Object> previousResult = fullResult;
        GraphView previousGraph = snapshots.get(0);

        for (int i = 1; i < snapshots.size(); i++) {
            long iceStart = System.nanoTime();
            Map<Long, Object> iceResult = iceEngine.incPregel(
                    snapshots.get(i), previousResult, previousGraph,
                    typedProgram, config.maxIterations());
            long iceElapsed = System.nanoTime() - iceStart;
            incrementalTotalNanos += iceElapsed;
            metrics.recordLatency("incrementalComputation", iceElapsed);

            previousResult = iceResult;
            previousGraph = snapshots.get(i);
        }

        long incrementalComputationMs = snapshots.size() > 1
                ? (incrementalTotalNanos / (snapshots.size() - 1)) / 1_000_000
                : 0;

        // 6. Collect metrics
        double speedup = incrementalComputationMs > 0
                ? (double) fullComputationMs / incrementalComputationMs
                : 1.0;

        long peakMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        metrics.recordMemory("peakMemory", peakMemory);

        long totalTimeMs = (System.nanoTime() - totalStart) / 1_000_000;

        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("vertexCount", loadResult.vertexCount());
        extra.put("edgeCount", loadResult.edgeCount());
        extra.put("graphLoadTimeMs", loadResult.loadTimeMs());
        extra.put("algorithm", config.algorithmName());

        return new BenchmarkResult(
                experimentName,
                totalTimeMs,
                avgRetrievalMs,
                fullComputationMs,
                incrementalComputationMs,
                speedup,
                peakMemory,
                config.numSnapshots(),
                extra
        );
    }

    /**
     * Runs experiments for all supported algorithms.
     *
     * @param config the benchmark configuration
     * @return list of results, one per algorithm
     */
    public List<BenchmarkResult> runAll(BenchmarkConfig config) {
        List<BenchmarkResult> results = new ArrayList<>();
        for (String algo : List.of("PageRank", "ConnectedComponents")) {
            BenchmarkConfig algoConfig = new BenchmarkConfig(
                    config.graphScale(), config.edgeFactor(),
                    config.numSnapshots(), config.mutationRate(),
                    config.maxIterations(), algo);
            results.add(runExperiment(algo + "_benchmark", algoConfig));
        }
        return results;
    }

    /**
     * Exports benchmark results to CSV and summary text files.
     *
     * @param outputDir the directory to write output files
     * @param results   the benchmark results to export
     */
    public void exportResults(Path outputDir, List<BenchmarkResult> results) {
        try {
            Files.createDirectories(outputDir);

            // Write CSV
            Path csvPath = outputDir.resolve("benchmark_results.csv");
            try (PrintWriter csv = new PrintWriter(Files.newBufferedWriter(csvPath))) {
                csv.println("experiment,totalTimeMs,avgRetrievalMs,fullComputationMs,"
                        + "incrementalComputationMs,speedup,peakMemoryBytes,numSnapshots");
                for (BenchmarkResult r : results) {
                    csv.printf("%s,%d,%d,%d,%d,%.2f,%d,%d%n",
                            r.experimentName(), r.totalTimeMs(), r.avgSnapshotRetrievalMs(),
                            r.fullComputationMs(), r.incrementalComputationMs(),
                            r.speedup(), r.peakMemoryBytes(), r.numSnapshots());
                }
            }

            // Write summary
            Path summaryPath = outputDir.resolve("benchmark_summary.txt");
            try (PrintWriter summary = new PrintWriter(Files.newBufferedWriter(summaryPath))) {
                summary.println("=== Tegra Benchmark Summary ===");
                summary.println();
                for (BenchmarkResult r : results) {
                    summary.printf("Experiment: %s%n", r.experimentName());
                    summary.printf("  Total time:       %d ms%n", r.totalTimeMs());
                    summary.printf("  Avg retrieval:    %d ms%n", r.avgSnapshotRetrievalMs());
                    summary.printf("  Full computation: %d ms%n", r.fullComputationMs());
                    summary.printf("  ICE computation:  %d ms%n", r.incrementalComputationMs());
                    summary.printf("  Speedup:          %.2fx%n", r.speedup());
                    summary.printf("  Peak memory:      %d bytes%n", r.peakMemoryBytes());
                    summary.printf("  Snapshots:        %d%n", r.numSnapshots());
                    if (!r.extraMetrics().isEmpty()) {
                        summary.printf("  Extra metrics:    %s%n", r.extraMetrics());
                    }
                    summary.println();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to export results to " + outputDir, e);
        }
    }

    private VertexProgram<?, ?, ?> createProgram(String algorithmName, int maxIterations) {
        return switch (algorithmName) {
            case "PageRank" -> new PageRank(0.85, 1e-6, maxIterations);
            case "ConnectedComponents" -> new ConnectedComponents();
            default -> throw new IllegalArgumentException("Unknown algorithm: " + algorithmName);
        };
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Object> executeFullComputation(
            GraphView graph, VertexProgram<Object, Object, Object> program,
            int maxIterations, String algorithmName) {

        // Initialize vertex values based on algorithm type
        Map<Long, Object> initialValues = new HashMap<>();
        var it = graph.vertices();
        while (it.hasNext()) {
            long vid = it.next().vertexId();
            switch (algorithmName) {
                case "PageRank" -> initialValues.put(vid, 1.0);
                case "ConnectedComponents" -> initialValues.put(vid, vid);
                default -> initialValues.put(vid, null);
            }
        }

        return gasEngine.execute(graph, program, initialValues, maxIterations);
    }

    private void applyMutation(WorkingVersion wv, GraphMutation mutation) {
        switch (mutation) {
            case GraphMutation.AddVertex av ->
                    wv.putVertex(av.vertexData().vertexId(), av.vertexData());
            case GraphMutation.RemoveVertex rv ->
                    wv.removeVertex(rv.vertexId());
            case GraphMutation.AddEdge ae -> {
                EdgeKey ek = ae.edgeData().edgeKey();
                wv.putEdge(ek.srcId(), ek.dstId(), ek.discriminator(), ae.edgeData());
            }
            case GraphMutation.RemoveEdge re ->
                    wv.removeEdge(re.srcId(), re.dstId(), re.discriminator());
            case GraphMutation.UpdateVertexProperty uvp -> {
                // Read-modify-write vertex property
                var vd = wv.getVertex(uvp.vertexId());
                if (vd != null) {
                    var newProps = new HashMap<>(vd.properties());
                    newProps.put(uvp.propertyKey(), uvp.value());
                    wv.putVertex(uvp.vertexId(),
                            new org.tegra.serde.VertexData(uvp.vertexId(), newProps));
                }
            }
            case GraphMutation.UpdateEdgeProperty uep -> {
                var ed = wv.getEdge(uep.srcId(), uep.dstId(), uep.discriminator());
                if (ed != null) {
                    var newProps = new HashMap<>(ed.properties());
                    newProps.put(uep.propertyKey(), uep.value());
                    wv.putEdge(uep.srcId(), uep.dstId(), uep.discriminator(),
                            new org.tegra.serde.EdgeData(ed.edgeKey(), newProps));
                }
            }
        }
    }
}
