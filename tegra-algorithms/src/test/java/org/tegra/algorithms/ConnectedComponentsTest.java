package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectedComponentsTest {

    private final ConnectedComponents<Double> cc = new ConnectedComponents<>();

    @Test
    void testMetadata() {
        assertThat(cc.name()).isEqualTo("CC");
        assertThat(cc.gatherDirection()).isEqualTo(EdgeDirection.BOTH);
        assertThat(cc.scatterDirection()).isEqualTo(EdgeDirection.BOTH);
    }

    @Test
    void testIdentity() {
        assertThat(cc.identity()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testGatherReturnsNeighborValue() {
        assertThat(cc.gather(5L, 1.0, 3L)).isEqualTo(3L);
    }

    @Test
    void testSumTakesMinimum() {
        assertThat(cc.sum(5L, 3L)).isEqualTo(3L);
        assertThat(cc.sum(1L, 7L)).isEqualTo(1L);
        assertThat(cc.sum(Long.MAX_VALUE, 42L)).isEqualTo(42L);
    }

    @Test
    void testApplyTakesMinimum() {
        assertThat(cc.apply(5L, 3L)).isEqualTo(3L);
        assertThat(cc.apply(1L, 7L)).isEqualTo(1L);
    }

    @Test
    void testScatterOnChange() {
        assertThat(cc.scatter(3L, 5L, 1.0)).isTrue();
        assertThat(cc.scatter(5L, 5L, 1.0)).isFalse();
    }

    @Test
    void testSingleComponent() {
        // Simulate CC on triangle graph: vertices 1, 2, 3 all connected.
        // All should converge to label 1 (the minimum ID).

        // Initial: each vertex has its own ID as label
        // Iteration 1: each gathers min of neighbor labels
        long label1 = cc.apply(1L, cc.sum(cc.gather(1L, 1.0, 2L), cc.gather(1L, 1.0, 3L)));
        long label2 = cc.apply(2L, cc.sum(cc.gather(2L, 1.0, 1L), cc.gather(2L, 1.0, 3L)));
        long label3 = cc.apply(3L, cc.sum(cc.gather(3L, 1.0, 1L), cc.gather(3L, 1.0, 2L)));

        assertThat(label1).isEqualTo(1L); // min(1, 2, 3) but apply(1, min(2,3)) = min(1,2) = 1
        assertThat(label2).isEqualTo(1L); // apply(2, min(1,3)) = min(2, 1) = 1
        assertThat(label3).isEqualTo(1L); // apply(3, min(1,2)) = min(3, 1) = 1
    }

    @Test
    void testTwoDisconnectedComponents() {
        // Component A: {1, 2, 3}, Component B: {4, 5, 6}
        // After CC, A should have label 1, B should have label 4.

        // Simulate vertex 4 gathering from neighbors 5, 6
        long label4 = cc.apply(4L, cc.sum(cc.gather(4L, 1.0, 5L), cc.gather(4L, 1.0, 6L)));
        long label5 = cc.apply(5L, cc.sum(cc.gather(5L, 1.0, 4L), cc.gather(5L, 1.0, 6L)));
        long label6 = cc.apply(6L, cc.sum(cc.gather(6L, 1.0, 4L), cc.gather(6L, 1.0, 5L)));

        assertThat(label4).isEqualTo(4L); // min(4, min(5,6)) = min(4, 5) = 4
        assertThat(label5).isEqualTo(4L); // min(5, min(4,6)) = min(5, 4) = 4
        assertThat(label6).isEqualTo(4L); // min(6, min(4,5)) = min(6, 4) = 4
    }

    @Test
    void testSingleVertex() {
        // A single vertex with no neighbors gets identity from gather,
        // then apply(1, Long.MAX_VALUE) = min(1, MAX_VALUE) = 1
        long label = cc.apply(1L, cc.identity());
        assertThat(label).isEqualTo(1L);
    }

    @Test
    void testLinearGraph() {
        // 1 -- 2 -- 3 -- 4 (undirected)
        // After enough iterations, all should converge to label 1.

        // Iteration 1:
        long l1 = cc.apply(1L, cc.gather(1L, 1.0, 2L)); // min(1, 2) = 1
        long l2 = cc.apply(2L, cc.sum(cc.gather(2L, 1.0, 1L), cc.gather(2L, 1.0, 3L))); // min(2, min(1,3)) = 1
        long l3 = cc.apply(3L, cc.sum(cc.gather(3L, 1.0, 2L), cc.gather(3L, 1.0, 4L))); // min(3, min(2,4)) = 2
        long l4 = cc.apply(4L, cc.gather(4L, 1.0, 3L)); // min(4, 3) = 3

        assertThat(l1).isEqualTo(1L);
        assertThat(l2).isEqualTo(1L);
        assertThat(l3).isEqualTo(2L);
        assertThat(l4).isEqualTo(3L);

        // Iteration 2 (using updated labels):
        long l1b = cc.apply(l1, cc.gather(l1, 1.0, l2)); // min(1, 1) = 1
        long l2b = cc.apply(l2, cc.sum(cc.gather(l2, 1.0, l1), cc.gather(l2, 1.0, l3))); // min(1, min(1,2)) = 1
        long l3b = cc.apply(l3, cc.sum(cc.gather(l3, 1.0, l2), cc.gather(l3, 1.0, l4))); // min(2, min(1,3)) = 1
        long l4b = cc.apply(l4, cc.sum(cc.identity(), cc.gather(l4, 1.0, l3))); // min(3, 2) = 2

        assertThat(l3b).isEqualTo(1L);
        assertThat(l4b).isEqualTo(2L);

        // Iteration 3:
        long l4c = cc.apply(l4b, cc.gather(l4b, 1.0, l3b)); // min(2, 1) = 1
        assertThat(l4c).isEqualTo(1L);
    }

    @Test
    void testHasConvergedDefaultEquality() {
        assertThat(cc.hasConverged(5L, 5L)).isTrue();
        assertThat(cc.hasConverged(5L, 3L)).isFalse();
    }
}
