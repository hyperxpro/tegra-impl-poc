package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BreadthFirstSearchTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void linearChain_distancesShouldBeHopCounts() {
        GraphView graph = TestGraphs.linearChain(5);
        BreadthFirstSearch bfs = new BreadthFirstSearch(0L);

        // Initialize: source=0 gets distance 0, others get MAX_VALUE
        Map<Long, Integer> initial = new HashMap<>();
        initial.put(0L, 0);
        for (int i = 1; i < 5; i++) {
            initial.put((long) i, Integer.MAX_VALUE);
        }

        Map<Long, Integer> result = engine.execute(graph, bfs, initial, 10);

        assertEquals(5, result.size());
        assertEquals(0, result.get(0L));
        assertEquals(1, result.get(1L));
        assertEquals(2, result.get(2L));
        assertEquals(3, result.get(3L));
        assertEquals(4, result.get(4L));
    }
}
