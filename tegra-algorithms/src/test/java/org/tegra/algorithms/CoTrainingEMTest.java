package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CoTrainingEMTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void simpleGraph_distributionsShouldConverge() {
        GraphView graph = TestGraphs.cycle(3);
        int numTopics = 3;
        CoTrainingEM em = new CoTrainingEM(numTopics, 0.5, 1e-6);

        // Initialize with different distributions
        Map<Long, double[]> initial = new HashMap<>();
        initial.put(0L, new double[]{0.7, 0.2, 0.1});
        initial.put(1L, new double[]{0.1, 0.7, 0.2});
        initial.put(2L, new double[]{0.2, 0.1, 0.7});

        Map<Long, double[]> result = engine.execute(graph, em, initial, 30);

        assertEquals(3, result.size());

        // All distributions should be valid (sum to 1.0)
        for (Map.Entry<Long, double[]> entry : result.entrySet()) {
            double[] dist = entry.getValue();
            assertNotNull(dist);
            assertEquals(numTopics, dist.length);

            double sum = 0;
            for (double d : dist) {
                assertTrue(d >= 0, "Distribution values should be non-negative");
                sum += d;
            }
            assertEquals(1.0, sum, 0.01,
                    "Distribution for vertex " + entry.getKey() + " should sum to 1.0");
        }

        // In a symmetric cycle with EM mixing, distributions should converge
        // toward each other (become more similar over iterations).
        // After convergence, all vertices in a cycle should have similar distributions.
        double[] d0 = result.get(0L);
        double[] d1 = result.get(1L);
        double[] d2 = result.get(2L);

        // Check that distributions have converged toward each other
        for (int i = 0; i < numTopics; i++) {
            assertEquals(d0[i], d1[i], 0.15,
                    "Topic " + i + " should be similar for vertices 0 and 1");
            assertEquals(d1[i], d2[i], 0.15,
                    "Topic " + i + " should be similar for vertices 1 and 2");
        }
    }
}
