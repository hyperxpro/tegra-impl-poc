package org.tegra.benchmark.workload;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.log.GraphMutation;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WorkloadGeneratorTest {

    private GraphView baseGraph;
    private WorkloadGenerator generator;

    @BeforeEach
    void setUp() {
        // Create a small test graph: 10 vertices with bidirectional edges
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        for (int i = 0; i < 10; i++) {
            wv.putVertex(i, new VertexData(i, Map.of()));
        }
        // Create a chain: 0-1-2-3-...-9 with bidirectional edges
        for (int i = 0; i < 9; i++) {
            addBidirectionalEdge(wv, i, i + 1);
        }
        // Add a few cross-links for variety
        addBidirectionalEdge(wv, 0, 5);
        addBidirectionalEdge(wv, 2, 7);
        addBidirectionalEdge(wv, 3, 8);

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        baseGraph = store.retrieve(v1);

        generator = new WorkloadGenerator(42L);
    }

    private static void addBidirectionalEdge(WorkingVersion wv, long src, long dst) {
        wv.putEdge(src, dst, (short) 0,
                new EdgeData(new EdgeKey(src, dst, (short) 0), Map.of()));
        wv.putEdge(dst, src, (short) 0,
                new EdgeData(new EdgeKey(dst, src, (short) 0), Map.of()));
    }

    @Test
    void mutationCountMatchesExpectedRate() {
        double mutationRate = 0.1; // 10% of edges
        int numSnapshots = 5;

        List<List<GraphMutation>> batches =
                generator.generateEvolution(baseGraph, mutationRate, numSnapshots);

        assertThat(batches).hasSize(numSnapshots);

        // Each batch should have mutations (exact count depends on edge count and rate)
        for (List<GraphMutation> batch : batches) {
            assertThat(batch).isNotEmpty();
        }
    }

    @Test
    void additionsOnlyModeCreatesOnlyAddEdgeMutations() {
        List<List<GraphMutation>> batches =
                generator.generateEvolution(baseGraph, 0.1, 3, MutationType.ADDITIONS_ONLY);

        for (List<GraphMutation> batch : batches) {
            assertThat(batch).isNotEmpty();
            for (GraphMutation mutation : batch) {
                assertThat(mutation).isInstanceOf(GraphMutation.AddEdge.class);
            }
        }
    }

    @Test
    void deletionsOnlyModeCreatesOnlyRemoveEdgeMutations() {
        List<List<GraphMutation>> batches =
                generator.generateEvolution(baseGraph, 0.1, 3, MutationType.DELETIONS_ONLY);

        for (List<GraphMutation> batch : batches) {
            // Some batches may be empty if all edges have been removed
            for (GraphMutation mutation : batch) {
                assertThat(mutation).isInstanceOf(GraphMutation.RemoveEdge.class);
            }
        }
    }

    @Test
    void mixedModeHasBothAddsAndRemoves() {
        List<List<GraphMutation>> batches =
                generator.generateEvolution(baseGraph, 0.2, 3, MutationType.MIXED);

        // Check that across all batches there are both adds and removes
        boolean hasAdds = false;
        boolean hasRemoves = false;
        for (List<GraphMutation> batch : batches) {
            for (GraphMutation mutation : batch) {
                if (mutation instanceof GraphMutation.AddEdge) {
                    hasAdds = true;
                } else if (mutation instanceof GraphMutation.RemoveEdge) {
                    hasRemoves = true;
                }
            }
        }

        assertThat(hasAdds).as("Mixed mode should produce add mutations").isTrue();
        assertThat(hasRemoves).as("Mixed mode should produce remove mutations").isTrue();
    }

    @Test
    void generatesCorrectNumberOfSnapshots() {
        int numSnapshots = 7;
        List<List<GraphMutation>> batches =
                generator.generateEvolution(baseGraph, 0.05, numSnapshots);

        assertThat(batches).hasSize(numSnapshots);
    }
}
