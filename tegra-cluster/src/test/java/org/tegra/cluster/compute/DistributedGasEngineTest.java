package org.tegra.cluster.compute;

import org.junit.jupiter.api.Test;
import org.tegra.cluster.ClusterManager;
import org.tegra.cluster.NodeDescriptor;
import org.tegra.cluster.partition.HashPartitioning;
import org.tegra.cluster.partition.PartitionStrategy;
import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for the DistributedGasEngine.
 * Verifies that multi-partition GAS produces results consistent with
 * single-partition execution.
 */
class DistributedGasEngineTest {

    /**
     * Simple PageRank vertex program matching GasEngineTest.
     */
    static class SimplePageRank implements VertexProgram<Double, Map<String, ?>, Double> {
        @Override
        public Double gather(EdgeTriplet<Double, Map<String, ?>> context) {
            return context.srcValue() != null ? context.srcValue() : 0.0;
        }

        @Override
        public Double sum(Double a, Double b) {
            return a + b;
        }

        @Override
        public Double apply(long vertexId, Double currentValue, Double gathered) {
            if (gathered == null) gathered = 0.0;
            return 0.15 + 0.85 * gathered;
        }

        @Override
        public Set<Long> scatter(EdgeTriplet<Double, Map<String, ?>> context, Double newValue) {
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

    /**
     * Connected components via GAS.
     */
    static class ConnectedComponents implements VertexProgram<Long, Map<String, ?>, Long> {
        @Override
        public Long gather(EdgeTriplet<Long, Map<String, ?>> context) {
            return context.srcValue() != null ? context.srcValue() : context.srcId();
        }

        @Override
        public Long sum(Long a, Long b) {
            if (a == null) return b;
            if (b == null) return a;
            return Math.min(a, b);
        }

        @Override
        public Long apply(long vertexId, Long currentValue, Long gathered) {
            long curr = (currentValue != null) ? currentValue : vertexId;
            if (gathered == null) return curr;
            return Math.min(curr, gathered);
        }

        @Override
        public Set<Long> scatter(EdgeTriplet<Long, Map<String, ?>> context, Long newValue) {
            if (context.dstValue() == null || newValue < context.dstValue()) {
                return Set.of(context.dstId());
            }
            return Set.of();
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

    /**
     * Sets up a multi-partition cluster with the given edges split across partitions.
     * Returns a pair of (ClusterManager, versionId).
     */
    private record ClusterSetup(ClusterManager cluster, ByteArray versionId) {}

    private ClusterSetup setupCluster(long[][] edges, int numPartitions) {
        PartitionStrategy strategy = new HashPartitioning();

        // Collect all vertex IDs
        Set<Long> allVertexIds = new HashSet<>();
        for (long[] edge : edges) {
            allVertexIds.add(edge[0]);
            allVertexIds.add(edge[1]);
        }

        // Create partition stores
        Map<Integer, PartitionStore> stores = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            stores.put(i, new PartitionStore());
        }

        ByteArray v0 = ByteArray.fromString("v0");
        ByteArray v1 = ByteArray.fromString("v1");

        // Initialize all stores
        for (int i = 0; i < numPartitions; i++) {
            stores.get(i).createInitialVersion(v0);
        }

        // Branch working versions
        Map<Integer, WorkingVersion> workingVersions = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            workingVersions.put(i, stores.get(i).branch(v0));
        }

        // Add vertices to their assigned partitions
        for (long vid : allVertexIds) {
            int pid = strategy.partitionForVertex(vid, numPartitions);
            WorkingVersion wv = workingVersions.get(pid);
            wv.putVertex(vid, new VertexData(vid, Map.of()));
        }

        // Add edges to their assigned partitions (follow source vertex)
        for (long[] edge : edges) {
            long srcId = edge[0];
            long dstId = edge[1];
            int pid = strategy.partitionForEdge(srcId, dstId, numPartitions);
            WorkingVersion wv = workingVersions.get(pid);
            EdgeKey ek = new EdgeKey(srcId, dstId, (short) 0);
            wv.putEdge(srcId, dstId, (short) 0, new EdgeData(ek, Map.of()));
        }

        // Commit all
        for (int i = 0; i < numPartitions; i++) {
            stores.get(i).commit(workingVersions.get(i), v1);
        }

        // Build node descriptors
        List<Integer> allPartitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            allPartitions.add(i);
        }
        NodeDescriptor node = new NodeDescriptor("localhost", 8080, 0, allPartitions);

        ClusterManager cluster = new ClusterManager(
                List.of(node), strategy, numPartitions, stores);
        cluster.start();

        return new ClusterSetup(cluster, v1);
    }

    /**
     * Builds a single-partition graph for comparison.
     */
    private GraphView buildSinglePartitionGraph(long[][] edges) {
        PartitionStore store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("single_v0");
        store.createInitialVersion(v0);
        WorkingVersion working = store.branch(v0);

        Set<Long> vertexIds = new HashSet<>();
        for (long[] edge : edges) {
            vertexIds.add(edge[0]);
            vertexIds.add(edge[1]);
        }

        for (long vid : vertexIds) {
            working.putVertex(vid, new VertexData(vid, Map.of()));
        }

        for (long[] edge : edges) {
            EdgeKey ek = new EdgeKey(edge[0], edge[1], (short) 0);
            working.putEdge(edge[0], edge[1], (short) 0, new EdgeData(ek, Map.of()));
        }

        ByteArray v1 = ByteArray.fromString("single_v1");
        store.commit(working, v1);
        return store.retrieve(v1);
    }

    @Test
    void multiPartitionPageRankMatchesSinglePartition() {
        // Triangle: 1->2, 2->3, 3->1
        long[][] edges = {{1, 2}, {2, 3}, {3, 1}};

        // Single-partition baseline
        GasEngine singleEngine = new GasEngine();
        GraphView singleGraph = buildSinglePartitionGraph(edges);
        Map<Long, Double> initial = Map.of(1L, 1.0, 2L, 1.0, 3L, 1.0);
        Map<Long, Double> singleResult = singleEngine.execute(
                singleGraph, new SimplePageRank(), initial, 20);

        // Multi-partition (2 partitions)
        ClusterSetup setup = setupCluster(edges, 2);
        DistributedGasEngine distEngine = new DistributedGasEngine(setup.cluster());

        Map<Integer, Map<Long, Double>> distResult = distEngine.execute(
                setup.versionId(), new SimplePageRank(), initial, 20);

        // Merge distributed results
        Map<Long, Double> mergedResult = new HashMap<>();
        for (Map<Long, Double> partValues : distResult.values()) {
            mergedResult.putAll(partValues);
        }

        // Verify same number of vertices
        assertThat(mergedResult).hasSize(singleResult.size());

        // Verify values match (within tolerance)
        for (Map.Entry<Long, Double> entry : singleResult.entrySet()) {
            assertThat(mergedResult.get(entry.getKey()))
                    .as("Vertex %d PageRank", entry.getKey())
                    .isCloseTo(entry.getValue(), within(0.01));
        }
    }

    @Test
    void multiPartitionPageRankOnLargerGraph() {
        // Star graph with spread vertex IDs: 10->20, 10->30, 20->10, 30->10
        long[][] edges = {
                {10, 20}, {10, 30},
                {20, 10}, {30, 10}
        };

        Map<Long, Double> initial = Map.of(
                10L, 1.0, 20L, 1.0, 30L, 1.0);

        // Single-partition baseline
        GasEngine singleEngine = new GasEngine();
        GraphView singleGraph = buildSinglePartitionGraph(edges);
        Map<Long, Double> singleResult = singleEngine.execute(
                singleGraph, new SimplePageRank(), initial, 20);

        // Multi-partition (2 partitions)
        ClusterSetup setup = setupCluster(edges, 2);
        DistributedGasEngine distEngine = new DistributedGasEngine(setup.cluster());

        Map<Integer, Map<Long, Double>> distResult = distEngine.execute(
                setup.versionId(), new SimplePageRank(), initial, 20);

        Map<Long, Double> mergedResult = new HashMap<>();
        for (Map<Long, Double> partValues : distResult.values()) {
            mergedResult.putAll(partValues);
        }

        assertThat(mergedResult).hasSize(singleResult.size());

        for (Map.Entry<Long, Double> entry : singleResult.entrySet()) {
            assertThat(mergedResult.get(entry.getKey()))
                    .as("Vertex %d PageRank", entry.getKey())
                    .isCloseTo(entry.getValue(), within(0.01));
        }
    }

    @Test
    void connectedComponentsAcrossPartitions() {
        // Two disconnected components: {1,2,3} and {4,5}
        long[][] edges = {
                {1, 2}, {2, 3}, {3, 1},
                {4, 5}, {5, 4}
        };

        Map<Long, Long> initial = Map.of(
                1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L, 5L, 5L);

        ClusterSetup setup = setupCluster(edges, 2);
        DistributedGasEngine distEngine = new DistributedGasEngine(setup.cluster());

        Map<Integer, Map<Long, Long>> distResult = distEngine.execute(
                setup.versionId(), new ConnectedComponents(), initial, 20);

        Map<Long, Long> merged = new HashMap<>();
        for (Map<Long, Long> partValues : distResult.values()) {
            merged.putAll(partValues);
        }

        assertThat(merged).hasSize(5);
        // Component 1: all should have min ID = 1
        assertThat(merged.get(1L)).isEqualTo(1L);
        assertThat(merged.get(2L)).isEqualTo(1L);
        assertThat(merged.get(3L)).isEqualTo(1L);
        // Component 2: all should have min ID = 4
        assertThat(merged.get(4L)).isEqualTo(4L);
        assertThat(merged.get(5L)).isEqualTo(4L);
    }
}
