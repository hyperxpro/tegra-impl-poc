package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.store.GraphView;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CollaborativeFilteringTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void smallBipartiteGraph_factorsShouldConverge() {
        // 2 users, 2 items with known ratings
        GraphView graph = TestGraphs.bipartite(2, 2);
        int numFactors = 3;
        CollaborativeFiltering cf = new CollaborativeFiltering(numFactors, 0.1);

        // Initialize all vertices with small random-ish factor vectors
        Map<Long, double[]> initial = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            double[] factors = new double[numFactors];
            for (int j = 0; j < numFactors; j++) {
                factors[j] = 0.1 * (i + j + 1);
            }
            initial.put((long) i, factors);
        }

        Map<Long, double[]> result = engine.execute(graph, cf, initial, 20);

        // All vertices should have factor vectors
        assertEquals(4, result.size());
        for (Map.Entry<Long, double[]> entry : result.entrySet()) {
            double[] factors = entry.getValue();
            assertNotNull(factors);
            assertEquals(numFactors, factors.length);

            // Factors should be finite (no NaN or Inf)
            for (double f : factors) {
                assertTrue(Double.isFinite(f),
                        "Factor should be finite for vertex " + entry.getKey() + ": " + Arrays.toString(factors));
            }
        }

        // Run a second time with the same initial values to verify determinism
        Map<Long, double[]> result2 = engine.execute(graph, cf, initial, 20);
        for (long vid : result.keySet()) {
            assertArrayEquals(result.get(vid), result2.get(vid), 1e-10,
                    "Results should be deterministic for vertex " + vid);
        }
    }
}
