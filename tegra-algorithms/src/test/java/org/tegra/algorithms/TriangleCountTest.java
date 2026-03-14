package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.api.GraphSnapshot;
import org.tegra.api.TestGraphs;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TriangleCountTest {

    private final TriangleCount<Double, Double> tc = new TriangleCount<>();

    @Test
    void testTriangleGraph() {
        // Triangle: 1--2, 2--3, 3--1 (all bidirectional)
        // Each vertex participates in exactly 1 triangle.
        GraphSnapshot<Double, Double> graph = TestGraphs.triangleGraph();
        Map<Long, Long> counts = tc.execute(graph);

        assertThat(counts).containsEntry(1L, 1L);
        assertThat(counts).containsEntry(2L, 1L);
        assertThat(counts).containsEntry(3L, 1L);
    }

    @Test
    void testNoTriangles() {
        // Linear graph: 1 -> 2 -> 3 -> 4 -> 5
        // No triangles in a path graph (even when treated as undirected,
        // there are no cycles of length 3).
        GraphSnapshot<Double, Double> graph = TestGraphs.linearGraph();
        Map<Long, Long> counts = tc.execute(graph);

        for (long v = 1; v <= 5; v++) {
            assertThat(counts.get(v)).isEqualTo(0L);
        }
    }

    @Test
    void testCompleteGraph() {
        // K4: 4 vertices, all pairs connected.
        // Each vertex participates in C(3,2) = 3 triangles
        // (pick 2 of the other 3 vertices to form a triangle).
        GraphSnapshot<Double, Double> graph = TestGraphs.completeGraph4();
        Map<Long, Long> counts = tc.execute(graph);

        assertThat(counts).hasSize(4);
        for (long v = 1; v <= 4; v++) {
            assertThat(counts.get(v)).isEqualTo(3L);
        }
    }

    @Test
    void testStarGraphNoTriangles() {
        // Star graph: center 0, spokes 1-5.
        // No triangles unless spokes are connected to each other.
        GraphSnapshot<Double, Double> graph = TestGraphs.starGraph();
        Map<Long, Long> counts = tc.execute(graph);

        for (Map.Entry<Long, Long> entry : counts.entrySet()) {
            assertThat(entry.getValue()).isEqualTo(0L);
        }
    }

    @Test
    void testEmptyGraph() {
        // Single vertex, no edges, no triangles.
        var tcLong = new TriangleCount<Long, Double>();
        GraphSnapshot<Long, Double> graph = TestGraphs.singleVertexGraph();
        Map<Long, Long> counts = tcLong.execute(graph);

        assertThat(counts).containsEntry(1L, 0L);
    }
}
