package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ShortestPathTest {

    @Test
    void testMetadata() {
        var sssp = new ShortestPath(1L);
        assertThat(sssp.name()).isEqualTo("SSSP");
        assertThat(sssp.gatherDirection()).isEqualTo(EdgeDirection.IN);
        assertThat(sssp.scatterDirection()).isEqualTo(EdgeDirection.OUT);
        assertThat(sssp.sourceVertex()).isEqualTo(1L);
    }

    @Test
    void testIdentity() {
        var sssp = new ShortestPath(1L);
        assertThat(sssp.identity()).isEqualTo(Double.MAX_VALUE);
    }

    @Test
    void testInitialValue() {
        var sssp = new ShortestPath(1L);
        assertThat(sssp.initialValue(1L)).isEqualTo(0.0);
        assertThat(sssp.initialValue(2L)).isEqualTo(Double.MAX_VALUE);
        assertThat(sssp.initialValue(99L)).isEqualTo(Double.MAX_VALUE);
    }

    @Test
    void testGatherAddsEdgeWeight() {
        var sssp = new ShortestPath(1L);
        // neighbor distance + edge weight
        assertThat(sssp.gather(5.0, 3.0, 2.0)).isCloseTo(5.0, within(1e-12));
    }

    @Test
    void testSumTakesMinimum() {
        var sssp = new ShortestPath(1L);
        assertThat(sssp.sum(3.0, 7.0)).isEqualTo(3.0);
        assertThat(sssp.sum(10.0, 2.0)).isEqualTo(2.0);
    }

    @Test
    void testApplyTakesMinimum() {
        var sssp = new ShortestPath(1L);
        assertThat(sssp.apply(5.0, 3.0)).isEqualTo(3.0);
        assertThat(sssp.apply(1.0, 7.0)).isEqualTo(1.0);
    }

    @Test
    void testScatterOnChange() {
        var sssp = new ShortestPath(1L);
        assertThat(sssp.scatter(3.0, 5.0, 1.0)).isTrue();
        assertThat(sssp.scatter(5.0, 5.0, 1.0)).isFalse();
    }

    @Test
    void testLinearGraph() {
        // Graph: 1 -> 2 -> 3 -> 4 -> 5, all edges weight 1.0, source = 1
        var sssp = new ShortestPath(1L);

        // Initial values
        double d1 = sssp.initialValue(1L); // 0.0
        double d2 = sssp.initialValue(2L); // MAX_VALUE
        double d3 = sssp.initialValue(3L); // MAX_VALUE

        // Iteration 1: vertex 2 gathers from vertex 1 (in-neighbor via edge 1->2)
        double gathered2 = sssp.gather(d2, 1.0, d1); // 0.0 + 1.0 = 1.0
        d2 = sssp.apply(d2, gathered2); // min(MAX_VALUE, 1.0) = 1.0

        assertThat(d2).isCloseTo(1.0, within(1e-12));

        // Iteration 2: vertex 3 gathers from vertex 2
        double gathered3 = sssp.gather(d3, 1.0, d2); // 1.0 + 1.0 = 2.0
        d3 = sssp.apply(d3, gathered3); // min(MAX_VALUE, 2.0) = 2.0

        assertThat(d3).isCloseTo(2.0, within(1e-12));
    }

    @Test
    void testStarGraph() {
        // Star: 0 -> 1, 0 -> 2, 0 -> 3, all weight 1.0, source = 0
        var sssp = new ShortestPath(0L);

        double d0 = sssp.initialValue(0L); // 0.0
        double d1 = sssp.initialValue(1L); // MAX_VALUE

        // Vertex 1 gathers from vertex 0
        double gathered1 = sssp.gather(d1, 1.0, d0); // 0.0 + 1.0 = 1.0
        d1 = sssp.apply(d1, gathered1); // min(MAX_VALUE, 1.0) = 1.0

        assertThat(d1).isCloseTo(1.0, within(1e-12));
    }

    @Test
    void testUnreachable() {
        // Source = 1, vertex 4 is disconnected
        var sssp = new ShortestPath(1L);

        double d4 = sssp.initialValue(4L); // MAX_VALUE
        // No in-neighbors, so gathered = identity = MAX_VALUE
        double gathered4 = sssp.identity();
        d4 = sssp.apply(d4, gathered4); // min(MAX_VALUE, MAX_VALUE) = MAX_VALUE

        assertThat(d4).isEqualTo(Double.MAX_VALUE);
    }

    @Test
    void testWeightedEdges() {
        // 1 --(2.0)--> 2 --(3.0)--> 3, source = 1
        var sssp = new ShortestPath(1L);

        double d1 = 0.0;
        double d2 = Double.MAX_VALUE;
        double d3 = Double.MAX_VALUE;

        // Iteration 1
        d2 = sssp.apply(d2, sssp.gather(d2, 2.0, d1)); // min(MAX, 0+2) = 2.0
        assertThat(d2).isCloseTo(2.0, within(1e-12));

        // Iteration 2
        d3 = sssp.apply(d3, sssp.gather(d3, 3.0, d2)); // min(MAX, 2+3) = 5.0
        assertThat(d3).isCloseTo(5.0, within(1e-12));
    }
}
