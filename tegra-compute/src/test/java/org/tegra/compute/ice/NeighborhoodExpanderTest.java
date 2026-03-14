package org.tegra.compute.ice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.api.GraphSnapshot;
import org.tegra.compute.TestGraphBuilder;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NeighborhoodExpanderTest {

    private NeighborhoodExpander expander;
    private GraphSnapshot<Double, Double> snapshot;

    @BeforeEach
    void setUp() {
        expander = new NeighborhoodExpander();

        // Graph: 1 -> 2, 2 -> 3, 3 -> 4, 5 (isolated)
        snapshot = TestGraphBuilder.<Double, Double>create()
                .addVertex(1, 1.0)
                .addVertex(2, 2.0)
                .addVertex(3, 3.0)
                .addVertex(4, 4.0)
                .addVertex(5, 5.0)
                .addEdge(1, 2, 1.0)
                .addEdge(2, 3, 1.0)
                .addEdge(3, 4, 1.0)
                .build();
    }

    @Test
    void testExpandSingleVertex() {
        // Expand vertex 2: out-neighbor=3, in-neighbor=1
        Set<Long> result = expander.expandOneHop(snapshot, Set.of(2L));
        assertThat(result).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    void testExpandMultipleVertices() {
        // Expand {1, 3}: 1's out-neighbor=2, 3's in-neighbor=2, 3's out-neighbor=4
        Set<Long> result = expander.expandOneHop(snapshot, Set.of(1L, 3L));
        assertThat(result).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
    }

    @Test
    void testExpandEmptySet() {
        Set<Long> result = expander.expandOneHop(snapshot, Set.of());
        assertThat(result).isEmpty();
    }

    @Test
    void testExpandIsolatedVertex() {
        // Vertex 5 has no edges
        Set<Long> result = expander.expandOneHop(snapshot, Set.of(5L));
        assertThat(result).containsExactly(5L);
    }
}
