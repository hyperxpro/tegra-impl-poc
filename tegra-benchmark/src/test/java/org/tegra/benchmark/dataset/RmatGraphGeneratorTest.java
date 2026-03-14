package org.tegra.benchmark.dataset;

import org.junit.jupiter.api.Test;
import org.tegra.serde.EdgeData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.version.ByteArray;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RmatGraphGeneratorTest {

    @Test
    void generatedGraphHasExpectedVertexCount() {
        int scale = 6; // 2^6 = 64 vertices
        PartitionStore store = new PartitionStore();
        ByteArray versionId = ByteArray.fromString("rmat_test");
        RmatGraphGenerator generator = new RmatGraphGenerator(scale, 4);

        GraphLoadResult result = generator.load(store, versionId);

        assertThat(result.vertexCount()).isEqualTo(1L << scale);

        // Verify the actual graph has the right vertex count
        GraphView graph = store.retrieve(versionId);
        assertThat(graph.vertexCount()).isEqualTo(1L << scale);
    }

    @Test
    void generatedGraphHasApproximatelyExpectedEdgeCount() {
        int scale = 6;
        int edgeFactor = 8;
        PartitionStore store = new PartitionStore();
        ByteArray versionId = ByteArray.fromString("rmat_edges");
        RmatGraphGenerator generator = new RmatGraphGenerator(scale, edgeFactor);

        GraphLoadResult result = generator.load(store, versionId);

        long expectedEdges = (1L << scale) * edgeFactor;
        // Due to self-loop removal and deduplication, actual count will be less
        // but should be within a reasonable fraction of the target
        assertThat(result.edgeCount()).isGreaterThan(0);
        assertThat(result.edgeCount()).isLessThanOrEqualTo(expectedEdges);
        // Should have at least 50% of target edges
        assertThat(result.edgeCount()).isGreaterThan(expectedEdges / 2);
    }

    @Test
    void generatedGraphHasPowerLawDegreeDistribution() {
        int scale = 8; // 256 vertices
        PartitionStore store = new PartitionStore();
        ByteArray versionId = ByteArray.fromString("rmat_powerlaw");
        RmatGraphGenerator generator = new RmatGraphGenerator(scale, 16);

        generator.load(store, versionId);
        GraphView graph = store.retrieve(versionId);

        // Count out-degree per vertex
        Map<Long, Integer> outDegree = new HashMap<>();
        Iterator<EdgeData> edges = graph.edges();
        while (edges.hasNext()) {
            EdgeData ed = edges.next();
            outDegree.merge(ed.edgeKey().srcId(), 1, Integer::sum);
        }

        // In a power-law distribution, some vertices should have much higher
        // degree than average (hubs). Check that max degree is significantly
        // greater than the average degree.
        int maxDegree = outDegree.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        double avgDegree = outDegree.values().stream().mapToInt(Integer::intValue).average().orElse(0);

        assertThat(maxDegree).isGreaterThan((int) (avgDegree * 2));
    }

    @Test
    void generatedGraphHasNoSelfLoops() {
        int scale = 6;
        PartitionStore store = new PartitionStore();
        ByteArray versionId = ByteArray.fromString("rmat_noselfloops");
        RmatGraphGenerator generator = new RmatGraphGenerator(scale, 16);

        generator.load(store, versionId);
        GraphView graph = store.retrieve(versionId);

        Iterator<EdgeData> edges = graph.edges();
        while (edges.hasNext()) {
            EdgeData ed = edges.next();
            assertThat(ed.edgeKey().srcId())
                    .as("Edge should not be a self-loop: src=%d dst=%d",
                            ed.edgeKey().srcId(), ed.edgeKey().dstId())
                    .isNotEqualTo(ed.edgeKey().dstId());
        }
    }

    @Test
    void deterministicWithSameSeed() {
        int scale = 5;
        int edgeFactor = 8;

        PartitionStore store1 = new PartitionStore();
        ByteArray v1 = ByteArray.fromString("rmat_seed1");
        new RmatGraphGenerator(scale, edgeFactor, 123L).load(store1, v1);

        PartitionStore store2 = new PartitionStore();
        ByteArray v2 = ByteArray.fromString("rmat_seed2");
        new RmatGraphGenerator(scale, edgeFactor, 123L).load(store2, v2);

        GraphView g1 = store1.retrieve(v1);
        GraphView g2 = store2.retrieve(v2);

        assertThat(g1.vertexCount()).isEqualTo(g2.vertexCount());
        assertThat(g1.edgeCount()).isEqualTo(g2.edgeCount());
    }
}
