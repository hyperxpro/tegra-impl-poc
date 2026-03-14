package org.tegra.examples;

import org.tegra.algorithms.PageRank;
import org.tegra.benchmark.dataset.GraphLoadResult;
import org.tegra.benchmark.dataset.RmatGraphGenerator;
import org.tegra.benchmark.workload.WorkloadGenerator;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.ParallelSnapshotExecutor;
import org.tegra.compute.ice.HeuristicSwitchOracle;
import org.tegra.compute.ice.IceEngine;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.store.GraphView;
import org.tegra.store.log.GraphMutation;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates sliding window queries across multiple graph snapshots.
 * Uses ParallelSnapshotExecutor to run PageRank on a window of consecutive snapshots.
 */
public final class SlidingWindowExample {

    public static void main(String[] args) {
        System.out.println("=== Sliding Window Example ===");
        System.out.println();

        // 1. Create base graph
        PartitionStore store = new PartitionStore();
        ByteArray baseVersionId = ByteArray.fromString("base");
        RmatGraphGenerator generator = new RmatGraphGenerator(6, 8, 99L); // 64 vertices
        GraphLoadResult loadResult = generator.load(store, baseVersionId);

        System.out.println("Generated base graph:");
        System.out.println("  Vertices: " + loadResult.vertexCount());
        System.out.println("  Edges:    " + loadResult.edgeCount());
        System.out.println();

        // 2. Generate 20 snapshots with small mutations
        int totalSnapshots = 20;
        double mutationRate = 0.02; // 2% per snapshot
        GraphView baseGraph = store.retrieve(baseVersionId);

        WorkloadGenerator workloadGen = new WorkloadGenerator(123L);
        List<List<GraphMutation>> mutationBatches =
                workloadGen.generateEvolution(baseGraph, mutationRate, totalSnapshots);

        // Apply mutations to create all snapshots
        List<GraphView> allSnapshots = new ArrayList<>();
        allSnapshots.add(baseGraph);

        ByteArray currentVersionId = baseVersionId;
        for (int i = 0; i < mutationBatches.size(); i++) {
            WorkingVersion wv = store.branch(currentVersionId);
            for (GraphMutation mutation : mutationBatches.get(i)) {
                applyMutation(wv, mutation);
            }
            ByteArray newVersionId = ByteArray.fromString("snap_" + (i + 1));
            store.commit(wv, newVersionId);
            allSnapshots.add(store.retrieve(newVersionId));
            currentVersionId = newVersionId;
        }

        System.out.println("Created " + totalSnapshots + " snapshots with "
                + String.format("%.0f%%", mutationRate * 100) + " mutation rate each");
        System.out.println();

        // 3. Run PageRank on a sliding window of 5 consecutive snapshots
        int windowSize = 5;
        GasEngine gasEngine = new GasEngine();
        IceEngine iceEngine = new IceEngine(gasEngine, new HeuristicSwitchOracle(0.5));
        ParallelSnapshotExecutor executor = new ParallelSnapshotExecutor(gasEngine, iceEngine);

        PageRank pageRank = new PageRank(0.85, 1e-6, 20);

        // Initialize values
        Map<Long, Double> initialValues = new HashMap<>();
        var vertexIt = baseGraph.vertices();
        while (vertexIt.hasNext()) {
            initialValues.put(vertexIt.next().vertexId(), 1.0);
        }

        System.out.println("Running PageRank on sliding windows of size " + windowSize + ":");
        System.out.println();

        // 4. Use ParallelSnapshotExecutor for concurrent computation on window
        for (int windowStart = 0; windowStart + windowSize <= allSnapshots.size(); windowStart += windowSize) {
            List<GraphView> window = allSnapshots.subList(windowStart, windowStart + windowSize);

            long startTime = System.nanoTime();
            Map<Integer, Map<Long, Double>> results = executor.executeParallel(
                    window, pageRank, initialValues, 20);
            long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

            // 5. Print results per snapshot in the window
            System.out.println("Window [" + windowStart + ".." + (windowStart + windowSize - 1) + "] "
                    + "completed in " + elapsedMs + " ms");
            for (int i = 0; i < window.size(); i++) {
                Map<Long, Double> snapshotResult = results.get(i);
                if (snapshotResult != null) {
                    double maxRank = snapshotResult.values().stream()
                            .mapToDouble(Double::doubleValue).max().orElse(0.0);
                    double avgRank = snapshotResult.values().stream()
                            .mapToDouble(Double::doubleValue).average().orElse(0.0);
                    System.out.printf("  Snapshot %d: vertices=%d, maxRank=%.4f, avgRank=%.4f%n",
                            windowStart + i, snapshotResult.size(), maxRank, avgRank);
                }
            }
            System.out.println();
        }

        System.out.println("Done.");
    }

    private static void applyMutation(WorkingVersion wv, GraphMutation mutation) {
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
