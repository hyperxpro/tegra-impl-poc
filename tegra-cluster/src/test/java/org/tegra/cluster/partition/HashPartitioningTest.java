package org.tegra.cluster.partition;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for all partition strategy implementations.
 */
class HashPartitioningTest {

    // ---- HashPartitioning tests ----

    @Test
    void hashPartitioningVertexProducesValidPartitionIds() {
        HashPartitioning strategy = new HashPartitioning();
        int numPartitions = 4;

        for (long vid = 0; vid < 100; vid++) {
            int partition = strategy.partitionForVertex(vid, numPartitions);
            assertThat(partition)
                    .as("Partition for vertex %d", vid)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(numPartitions);
        }
    }

    @Test
    void hashPartitioningDistributesEvenly() {
        HashPartitioning strategy = new HashPartitioning();
        int numPartitions = 4;
        int numVertices = 10_000;

        Map<Integer, Integer> counts = new HashMap<>();
        for (long vid = 0; vid < numVertices; vid++) {
            int p = strategy.partitionForVertex(vid, numPartitions);
            counts.merge(p, 1, Integer::sum);
        }

        // Each partition should have roughly numVertices / numPartitions = 2500
        // Allow +-30% tolerance for hash distribution
        int expected = numVertices / numPartitions;
        for (int p = 0; p < numPartitions; p++) {
            int count = counts.getOrDefault(p, 0);
            assertThat(count)
                    .as("Partition %d count", p)
                    .isGreaterThan(expected / 3)
                    .isLessThan(expected * 3);
        }
    }

    @Test
    void hashPartitioningEdgeConsistentWithVertex() {
        HashPartitioning strategy = new HashPartitioning();
        int numPartitions = 4;

        // Edge follows source vertex partition
        for (long srcId = 0; srcId < 50; srcId++) {
            for (long dstId = 0; dstId < 50; dstId++) {
                int edgePartition = strategy.partitionForEdge(srcId, dstId, numPartitions);
                int vertexPartition = strategy.partitionForVertex(srcId, numPartitions);
                assertThat(edgePartition)
                        .as("Edge (%d,%d) should follow src vertex partition", srcId, dstId)
                        .isEqualTo(vertexPartition);
            }
        }
    }

    // ---- EdgePartition2D tests ----

    @Test
    void edgePartition2DProducesValidPartitionIds() {
        EdgePartition2D strategy = new EdgePartition2D();
        int numPartitions = 4;

        for (long srcId = 0; srcId < 50; srcId++) {
            for (long dstId = 0; dstId < 50; dstId++) {
                int partition = strategy.partitionForEdge(srcId, dstId, numPartitions);
                assertThat(partition)
                        .as("Partition for edge (%d,%d)", srcId, dstId)
                        .isGreaterThanOrEqualTo(0)
                        .isLessThan(numPartitions);
            }
        }
    }

    @Test
    void edgePartition2DVertexProducesValidPartitionIds() {
        EdgePartition2D strategy = new EdgePartition2D();
        int numPartitions = 4;

        for (long vid = 0; vid < 100; vid++) {
            int partition = strategy.partitionForVertex(vid, numPartitions);
            assertThat(partition)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(numPartitions);
        }
    }

    // ---- RandomVertexCut tests ----

    @Test
    void randomVertexCutProducesValidPartitionIds() {
        RandomVertexCut strategy = new RandomVertexCut();
        int numPartitions = 4;

        for (long srcId = 0; srcId < 50; srcId++) {
            for (long dstId = 0; dstId < 50; dstId++) {
                int partition = strategy.partitionForEdge(srcId, dstId, numPartitions);
                assertThat(partition)
                        .as("Partition for edge (%d,%d)", srcId, dstId)
                        .isGreaterThanOrEqualTo(0)
                        .isLessThan(numPartitions);
            }
        }
    }

    @Test
    void randomVertexCutVertexProducesValidPartitionIds() {
        RandomVertexCut strategy = new RandomVertexCut();
        int numPartitions = 4;

        for (long vid = 0; vid < 100; vid++) {
            int partition = strategy.partitionForVertex(vid, numPartitions);
            assertThat(partition)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(numPartitions);
        }
    }

    // ---- CanonicalRandomVertexCut tests ----

    @Test
    void canonicalRandomVertexCutProducesValidPartitionIds() {
        CanonicalRandomVertexCut strategy = new CanonicalRandomVertexCut();
        int numPartitions = 4;

        for (long srcId = 0; srcId < 50; srcId++) {
            for (long dstId = 0; dstId < 50; dstId++) {
                int partition = strategy.partitionForEdge(srcId, dstId, numPartitions);
                assertThat(partition)
                        .as("Partition for edge (%d,%d)", srcId, dstId)
                        .isGreaterThanOrEqualTo(0)
                        .isLessThan(numPartitions);
            }
        }
    }

    @Test
    void canonicalRandomVertexCutColocatesReverseEdges() {
        CanonicalRandomVertexCut strategy = new CanonicalRandomVertexCut();
        int numPartitions = 8;

        // Edges (u, v) and (v, u) should be in the same partition
        for (long u = 1; u < 30; u++) {
            for (long v = u + 1; v < 30; v++) {
                int p1 = strategy.partitionForEdge(u, v, numPartitions);
                int p2 = strategy.partitionForEdge(v, u, numPartitions);
                assertThat(p1)
                        .as("Edge (%d,%d) and (%d,%d) should be co-located", u, v, v, u)
                        .isEqualTo(p2);
            }
        }
    }

    @Test
    void canonicalRandomVertexCutVertexProducesValidPartitionIds() {
        CanonicalRandomVertexCut strategy = new CanonicalRandomVertexCut();
        int numPartitions = 4;

        for (long vid = 0; vid < 100; vid++) {
            int partition = strategy.partitionForVertex(vid, numPartitions);
            assertThat(partition)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(numPartitions);
        }
    }

    // ---- Cross-strategy: all produce valid IDs ----

    @Test
    void allStrategiesProduceValidPartitionIdsForVariousPartitionCounts() {
        PartitionStrategy[] strategies = {
                new HashPartitioning(),
                new EdgePartition2D(),
                new RandomVertexCut(),
                new CanonicalRandomVertexCut()
        };

        int[] partitionCounts = {1, 2, 3, 4, 7, 16};

        for (PartitionStrategy strategy : strategies) {
            for (int numPartitions : partitionCounts) {
                for (long vid = 0; vid < 100; vid++) {
                    int vp = strategy.partitionForVertex(vid, numPartitions);
                    assertThat(vp)
                            .as("%s vertex %d with %d partitions",
                                    strategy.getClass().getSimpleName(), vid, numPartitions)
                            .isGreaterThanOrEqualTo(0)
                            .isLessThan(numPartitions);
                }

                for (long srcId = 0; srcId < 20; srcId++) {
                    for (long dstId = 0; dstId < 20; dstId++) {
                        int ep = strategy.partitionForEdge(srcId, dstId, numPartitions);
                        assertThat(ep)
                                .as("%s edge (%d,%d) with %d partitions",
                                        strategy.getClass().getSimpleName(), srcId, dstId, numPartitions)
                                .isGreaterThanOrEqualTo(0)
                                .isLessThan(numPartitions);
                    }
                }
            }
        }
    }
}
