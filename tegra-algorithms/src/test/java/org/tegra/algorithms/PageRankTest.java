package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;
import org.tegra.api.TestGraphs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PageRankTest {

    @Test
    void testDefaultParameters() {
        var pr = new PageRank<>();
        assertThat(pr.name()).isEqualTo("PageRank");
        assertThat(pr.dampingFactor()).isEqualTo(0.85);
        assertThat(pr.tolerance()).isEqualTo(1e-6);
        assertThat(pr.maxIterations()).isEqualTo(20);
        assertThat(pr.gatherDirection()).isEqualTo(EdgeDirection.IN);
        assertThat(pr.scatterDirection()).isEqualTo(EdgeDirection.OUT);
    }

    @Test
    void testCustomParameters() {
        var pr = new PageRank<>(0.9, 1e-4, 50);
        assertThat(pr.dampingFactor()).isEqualTo(0.9);
        assertThat(pr.tolerance()).isEqualTo(1e-4);
        assertThat(pr.maxIterations()).isEqualTo(50);
    }

    @Test
    void testIdentity() {
        var pr = new PageRank<>();
        assertThat(pr.identity()).isEqualTo(0.0);
    }

    @Test
    void testGatherReturnsNeighborValue() {
        var pr = new PageRank<Double>();
        assertThat(pr.gather(0.5, 1.0, 0.3)).isEqualTo(0.3);
    }

    @Test
    void testSumAddsValues() {
        var pr = new PageRank<>();
        assertThat(pr.sum(0.3, 0.7)).isCloseTo(1.0, within(1e-12));
    }

    @Test
    void testApplyDampingFormula() {
        var pr = new PageRank<>(0.85, 1e-6, 20);
        // apply(current, gathered) = (1 - 0.85) + 0.85 * gathered
        double result = pr.apply(0.5, 1.0);
        assertThat(result).isCloseTo(0.15 + 0.85, within(1e-12));
        assertThat(result).isCloseTo(1.0, within(1e-12));
    }

    @Test
    void testScatterWhenChanged() {
        var pr = new PageRank<Double>(0.85, 0.01, 20);
        // Difference > tolerance -> scatter returns true
        assertThat(pr.scatter(1.0, 0.5, 1.0)).isTrue();
        // Difference < tolerance -> scatter returns false
        assertThat(pr.scatter(1.0, 1.005, 1.0)).isFalse(); // |1.0 - 1.005| = 0.005 < 0.01
        // Exactly equal -> no scatter
        assertThat(pr.scatter(1.0, 1.0, 1.0)).isFalse();
        // Difference > tolerance -> scatter
        assertThat(pr.scatter(1.0, 1.02, 1.0)).isTrue(); // |1.0 - 1.02| = 0.02 > 0.01
    }

    @Test
    void testHasConverged() {
        var pr = new PageRank<>(0.85, 0.01, 20);
        assertThat(pr.hasConverged(1.0, 1.005)).isTrue();  // diff = 0.005 < 0.01
        assertThat(pr.hasConverged(1.0, 1.02)).isFalse();  // diff = 0.02 > 0.01
        assertThat(pr.hasConverged(1.0, 1.0)).isTrue();
    }

    @Test
    void testUniformGraphAllEqualRank() {
        // In a symmetric triangle graph where each vertex has same in/out degree,
        // after convergence all vertices should have equal rank.
        var pr = new PageRank<Double>(0.85, 1e-6, 100);

        // Simulate one iteration on the triangle graph:
        // Each vertex gathers from 2 in-neighbors, each with rank 1.0
        // gathered = 1.0 + 1.0 = 2.0
        // apply = 0.15 + 0.85 * 2.0 = 1.85
        double gathered = pr.sum(pr.gather(1.0, 1.0, 1.0), pr.gather(1.0, 1.0, 1.0));
        double newRank = pr.apply(1.0, gathered);
        assertThat(newRank).isCloseTo(1.85, within(1e-12));
    }

    @Test
    void testStarGraphCenterHigherRank() {
        // In a star graph with edges pointing inward to center,
        // the center gathers from all spokes.
        var pr = new PageRank<Double>(0.85, 1e-6, 20);

        // Center gathers from 5 in-neighbors, each with rank 1.0
        double gatheredCenter = 0.0;
        for (int i = 0; i < 5; i++) {
            gatheredCenter = pr.sum(gatheredCenter, pr.gather(1.0, 1.0, 1.0));
        }
        double centerRank = pr.apply(1.0, gatheredCenter);

        // Spoke has 0 in-neighbors -> gathered = identity = 0.0
        double spokeRank = pr.apply(1.0, pr.identity());

        assertThat(centerRank).isGreaterThan(spokeRank);
        assertThat(centerRank).isCloseTo(0.15 + 0.85 * 5.0, within(1e-12));
        assertThat(spokeRank).isCloseTo(0.15, within(1e-12));
    }

    @Test
    void testLinearGraph() {
        // In 1 -> 2 -> 3, vertex 3 gets rank from 2, vertex 2 from 1, vertex 1 from nobody
        var pr = new PageRank<Double>(0.85, 1e-6, 20);

        // Vertex 1: no in-neighbors
        double rank1 = pr.apply(1.0, pr.identity());
        assertThat(rank1).isCloseTo(0.15, within(1e-12));

        // Vertex 2: gathers from vertex 1 (rank 1.0 initially)
        double rank2 = pr.apply(1.0, pr.gather(1.0, 1.0, 1.0));
        assertThat(rank2).isCloseTo(0.15 + 0.85, within(1e-12));

        // Vertex 3: gathers from vertex 2 (rank 1.0 initially)
        double rank3 = pr.apply(1.0, pr.gather(1.0, 1.0, 1.0));
        assertThat(rank3).isCloseTo(1.0, within(1e-12));
    }

    @Test
    void testConvergence() {
        // Verify that convergence detection works
        var pr = new PageRank<>(0.85, 1e-6, 20);
        assertThat(pr.hasConverged(1.0, 1.0 + 1e-7)).isTrue();
        assertThat(pr.hasConverged(1.0, 1.0 + 1e-5)).isFalse();
    }
}
