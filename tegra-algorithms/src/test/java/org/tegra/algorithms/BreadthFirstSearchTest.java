package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;

import static org.assertj.core.api.Assertions.assertThat;

class BreadthFirstSearchTest {

    @Test
    void testMetadata() {
        var bfs = new BreadthFirstSearch<Double>(1L);
        assertThat(bfs.name()).isEqualTo("BFS");
        assertThat(bfs.gatherDirection()).isEqualTo(EdgeDirection.IN);
        assertThat(bfs.scatterDirection()).isEqualTo(EdgeDirection.OUT);
        assertThat(bfs.sourceVertex()).isEqualTo(1L);
    }

    @Test
    void testIdentity() {
        var bfs = new BreadthFirstSearch<>(1L);
        assertThat(bfs.identity()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testInitialValue() {
        var bfs = new BreadthFirstSearch<>(1L);
        assertThat(bfs.initialValue(1L)).isEqualTo(0);
        assertThat(bfs.initialValue(2L)).isEqualTo(Integer.MAX_VALUE);
        assertThat(bfs.initialValue(99L)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testGatherIncrementsDepth() {
        var bfs = new BreadthFirstSearch<Double>(1L);
        assertThat(bfs.gather(Integer.MAX_VALUE, 1.0, 3)).isEqualTo(4);
        assertThat(bfs.gather(Integer.MAX_VALUE, 1.0, 0)).isEqualTo(1);
    }

    @Test
    void testGatherMaxValueNeighbor() {
        var bfs = new BreadthFirstSearch<Double>(1L);
        // Neighbor not yet reached -> gather returns MAX_VALUE (no overflow)
        assertThat(bfs.gather(Integer.MAX_VALUE, 1.0, Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testSumTakesMinimum() {
        var bfs = new BreadthFirstSearch<>(1L);
        assertThat(bfs.sum(3, 7)).isEqualTo(3);
        assertThat(bfs.sum(Integer.MAX_VALUE, 2)).isEqualTo(2);
    }

    @Test
    void testApplyTakesMinimum() {
        var bfs = new BreadthFirstSearch<>(1L);
        assertThat(bfs.apply(5, 3)).isEqualTo(3);
        assertThat(bfs.apply(1, 7)).isEqualTo(1);
    }

    @Test
    void testScatterOnChange() {
        var bfs = new BreadthFirstSearch<Double>(1L);
        assertThat(bfs.scatter(1, Integer.MAX_VALUE, 1.0)).isTrue();
        assertThat(bfs.scatter(3, 3, 1.0)).isFalse();
    }

    @Test
    void testFromSource() {
        // 1 -> 2 -> 3, source = 1
        var bfs = new BreadthFirstSearch<Double>(1L);

        int d1 = bfs.initialValue(1L); // 0
        int d2 = bfs.initialValue(2L); // MAX_VALUE
        int d3 = bfs.initialValue(3L); // MAX_VALUE

        // Iteration 1: vertex 2 gathers from vertex 1
        int gathered2 = bfs.gather(d2, 1.0, d1); // 0 + 1 = 1
        d2 = bfs.apply(d2, gathered2); // min(MAX, 1) = 1
        assertThat(d2).isEqualTo(1);

        // Iteration 2: vertex 3 gathers from vertex 2
        int gathered3 = bfs.gather(d3, 1.0, d2); // 1 + 1 = 2
        d3 = bfs.apply(d3, gathered3); // min(MAX, 2) = 2
        assertThat(d3).isEqualTo(2);
    }

    @Test
    void testLinearGraph() {
        // 1 -> 2 -> 3 -> 4 -> 5, source = 1
        var bfs = new BreadthFirstSearch<Double>(1L);

        int[] depths = new int[6]; // 1-indexed
        for (int i = 1; i <= 5; i++) {
            depths[i] = bfs.initialValue(i);
        }
        assertThat(depths[1]).isEqualTo(0);

        // Simulate iterations
        for (int iter = 0; iter < 4; iter++) {
            int[] next = new int[6];
            System.arraycopy(depths, 0, next, 0, 6);
            for (int v = 2; v <= 5; v++) {
                // Each vertex v gathers from v-1 (its in-neighbor)
                int gathered = bfs.gather(depths[v], 1.0, depths[v - 1]);
                next[v] = bfs.apply(depths[v], gathered);
            }
            depths = next;
        }

        assertThat(depths[1]).isEqualTo(0);
        assertThat(depths[2]).isEqualTo(1);
        assertThat(depths[3]).isEqualTo(2);
        assertThat(depths[4]).isEqualTo(3);
        assertThat(depths[5]).isEqualTo(4);
    }

    @Test
    void testUnreachable() {
        // Source = 1, vertex 4 is isolated
        var bfs = new BreadthFirstSearch<Double>(1L);

        int d4 = bfs.initialValue(4L); // MAX_VALUE
        // No in-neighbors
        int gathered = bfs.identity(); // MAX_VALUE
        d4 = bfs.apply(d4, gathered); // min(MAX, MAX) = MAX

        assertThat(d4).isEqualTo(Integer.MAX_VALUE);
    }
}
