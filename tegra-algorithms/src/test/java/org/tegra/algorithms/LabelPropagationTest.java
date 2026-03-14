package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;

import static org.assertj.core.api.Assertions.assertThat;

class LabelPropagationTest {

    private final LabelPropagation<Double> lp = new LabelPropagation<>();

    @Test
    void testMetadata() {
        assertThat(lp.name()).isEqualTo("LabelProp");
        assertThat(lp.gatherDirection()).isEqualTo(EdgeDirection.BOTH);
        assertThat(lp.scatterDirection()).isEqualTo(EdgeDirection.BOTH);
    }

    @Test
    void testIdentity() {
        assertThat(lp.identity()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testGatherReturnsNeighborLabel() {
        assertThat(lp.gather(5L, 1.0, 3L)).isEqualTo(3L);
    }

    @Test
    void testSumTakesMinimum() {
        assertThat(lp.sum(5L, 3L)).isEqualTo(3L);
        assertThat(lp.sum(1L, 7L)).isEqualTo(1L);
    }

    @Test
    void testApplyTakesMinimumLabel() {
        assertThat(lp.apply(5L, 3L)).isEqualTo(3L);
        assertThat(lp.apply(1L, 7L)).isEqualTo(1L);
    }

    @Test
    void testScatterOnChange() {
        assertThat(lp.scatter(3L, 5L, 1.0)).isTrue();
        assertThat(lp.scatter(5L, 5L, 1.0)).isFalse();
    }

    @Test
    void testSingleCommunity() {
        // All vertices connected: after propagation, all should have min label
        long label1 = lp.apply(1L, lp.sum(lp.gather(1L, 1.0, 2L), lp.gather(1L, 1.0, 3L)));
        long label2 = lp.apply(2L, lp.sum(lp.gather(2L, 1.0, 1L), lp.gather(2L, 1.0, 3L)));
        long label3 = lp.apply(3L, lp.sum(lp.gather(3L, 1.0, 1L), lp.gather(3L, 1.0, 2L)));

        assertThat(label1).isEqualTo(1L);
        assertThat(label2).isEqualTo(1L);
        assertThat(label3).isEqualTo(1L);
    }

    @Test
    void testTwoCommunities() {
        // Community A: {1, 2}, Community B: {10, 11}
        // After propagation within each community:
        long labelA1 = lp.apply(1L, lp.gather(1L, 1.0, 2L)); // min(1, 2) = 1
        long labelA2 = lp.apply(2L, lp.gather(2L, 1.0, 1L)); // min(2, 1) = 1
        long labelB1 = lp.apply(10L, lp.gather(10L, 1.0, 11L)); // min(10, 11) = 10
        long labelB2 = lp.apply(11L, lp.gather(11L, 1.0, 10L)); // min(11, 10) = 10

        assertThat(labelA1).isEqualTo(1L);
        assertThat(labelA2).isEqualTo(1L);
        assertThat(labelB1).isEqualTo(10L);
        assertThat(labelB2).isEqualTo(10L);
    }

    @Test
    void testIsolatedVertex() {
        // Vertex with no neighbors gets identity, then applies min
        long label = lp.apply(42L, lp.identity()); // min(42, MAX_VALUE) = 42
        assertThat(label).isEqualTo(42L);
    }
}
