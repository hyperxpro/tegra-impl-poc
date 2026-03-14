package org.tegra.examples;

import org.tegra.algorithms.PageRank;
import org.tegra.benchmark.dataset.GraphLoadResult;
import org.tegra.benchmark.dataset.RmatGraphGenerator;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.ice.HeuristicSwitchOracle;
import org.tegra.compute.ice.IceEngine;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.HashMap;
import java.util.Map;

/**
 * Demonstrates incremental PageRank computation using ICE.
 * Compares full recomputation with incremental computation after graph changes.
 */
public final class IncrementalPageRankExample {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        System.out.println("=== Incremental PageRank Example ===");
        System.out.println();

        // 1. Create graph with R-MAT generator (small scale)
        PartitionStore store = new PartitionStore();
        ByteArray v1Id = ByteArray.fromString("v1");
        RmatGraphGenerator generator = new RmatGraphGenerator(6, 8, 42L); // 64 vertices
        GraphLoadResult loadResult = generator.load(store, v1Id);

        System.out.println("Generated R-MAT graph:");
        System.out.println("  Vertices: " + loadResult.vertexCount());
        System.out.println("  Edges:    " + loadResult.edgeCount());
        System.out.println("  Load time: " + loadResult.loadTimeMs() + " ms");
        System.out.println();

        GraphView graph1 = store.retrieve(v1Id);

        // 2. Run full PageRank
        GasEngine gasEngine = new GasEngine();
        PageRank pageRank = new PageRank(0.85, 1e-6, 20);

        // Initialize all vertices with rank 1.0
        Map<Long, Double> initialValues = new HashMap<>();
        var vertexIt = graph1.vertices();
        while (vertexIt.hasNext()) {
            initialValues.put(vertexIt.next().vertexId(), 1.0);
        }

        long fullStart = System.nanoTime();
        Map<Long, Double> fullResult = gasEngine.execute(
                graph1, (PageRank) pageRank, initialValues, 20);
        long fullTimeMs = (System.nanoTime() - fullStart) / 1_000_000;

        System.out.println("Full PageRank computation:");
        System.out.println("  Time: " + fullTimeMs + " ms");
        printTopRanks(fullResult, 5);
        System.out.println();

        // 3. Add some edges to create a new snapshot
        WorkingVersion wv = store.branch(v1Id);
        int newEdges = 10;
        long numVertices = loadResult.vertexCount();
        for (int i = 0; i < newEdges; i++) {
            long src = i % numVertices;
            long dst = (i * 7 + 3) % numVertices;
            if (src != dst) {
                wv.putEdge(src, dst, (short) 1,
                        new EdgeData(new EdgeKey(src, dst, (short) 1), Map.of()));
            }
        }
        ByteArray v2Id = ByteArray.fromString("v2");
        store.commit(wv, v2Id);
        GraphView graph2 = store.retrieve(v2Id);

        System.out.println("Added " + newEdges + " new edges to create snapshot v2");
        System.out.println("  New edge count: " + graph2.edgeCount());
        System.out.println();

        // 4. Run ICE incremental PageRank
        IceEngine iceEngine = new IceEngine(gasEngine, new HeuristicSwitchOracle(0.5));

        long iceStart = System.nanoTime();
        Map<Long, Double> iceResult = iceEngine.incPregel(
                graph2,
                (Map<Long, Double>) (Map<?, ?>) fullResult,
                graph1,
                (PageRank) pageRank,
                20);
        long iceTimeMs = (System.nanoTime() - iceStart) / 1_000_000;

        System.out.println("ICE Incremental PageRank:");
        System.out.println("  Time: " + iceTimeMs + " ms");
        printTopRanks(iceResult, 5);
        System.out.println();

        // 5. Compare results with full recomputation on the new graph
        long fullRecompStart = System.nanoTime();
        Map<Long, Double> fullRecompResult = gasEngine.execute(
                graph2, (PageRank) pageRank, initialValues, 20);
        long fullRecompTimeMs = (System.nanoTime() - fullRecompStart) / 1_000_000;

        System.out.println("Full recomputation on updated graph:");
        System.out.println("  Time: " + fullRecompTimeMs + " ms");
        printTopRanks(fullRecompResult, 5);
        System.out.println();

        // 6. Print speedup
        double speedup = fullRecompTimeMs > 0 ? (double) fullRecompTimeMs / Math.max(1, iceTimeMs) : 1.0;
        System.out.println("Speedup (full / ICE): " + String.format("%.2fx", speedup));
        System.out.println();
        System.out.println("Done.");
    }

    private static void printTopRanks(Map<Long, Double> ranks, int topN) {
        System.out.println("  Top " + topN + " vertices by rank:");
        ranks.entrySet().stream()
                .sorted(Map.Entry.<Long, Double>comparingByValue().reversed())
                .limit(topN)
                .forEach(e -> System.out.printf("    Vertex %d: %.6f%n", e.getKey(), e.getValue()));
    }
}
