package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PageRankTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void starGraph_centerShouldHaveHighestRank() {
        GraphView graph = TestGraphs.star(3);
        PageRank pr = new PageRank(0.85, 1e-6, 20);

        // Initialize all vertices with rank 1.0
        Map<Long, Double> initial = new HashMap<>();
        for (int i = 0; i <= 3; i++) {
            initial.put((long) i, 1.0);
        }

        Map<Long, Double> result = engine.execute(graph, pr, initial, pr.maxIterations());

        // Center vertex (0) should have the highest rank because it receives
        // contributions from all leaf vertices
        double centerRank = result.get(0L);
        for (int i = 1; i <= 3; i++) {
            assertTrue(centerRank > result.get((long) i),
                    "Center rank " + centerRank + " should be > leaf rank " + result.get((long) i));
        }
    }

    @Test
    void cycleGraph_allVerticesShouldHaveEqualRank() {
        GraphView graph = TestGraphs.cycle(4);
        PageRank pr = new PageRank(0.85, 1e-6, 20);

        Map<Long, Double> initial = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            initial.put((long) i, 1.0);
        }

        Map<Long, Double> result = engine.execute(graph, pr, initial, pr.maxIterations());

        // All vertices in a symmetric cycle should have approximately equal rank
        double rank0 = result.get(0L);
        for (int i = 1; i < 4; i++) {
            assertEquals(rank0, result.get((long) i), 0.01,
                    "Vertex " + i + " rank should equal vertex 0 rank");
        }
    }
}
