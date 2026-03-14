package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ConnectedComponentsTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void twoTriangles_shouldFindTwoComponents() {
        GraphView graph = TestGraphs.twoTriangles();
        ConnectedComponents cc = new ConnectedComponents();

        // Initialize each vertex with its own ID as label
        Map<Long, Long> initial = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            initial.put((long) i, (long) i);
        }

        Map<Long, Long> result = engine.execute(graph, cc, initial, 20);

        assertEquals(6, result.size());

        // Vertices 0,1,2 should have the same label
        long label1 = result.get(0L);
        assertEquals(label1, result.get(1L));
        assertEquals(label1, result.get(2L));

        // Vertices 3,4,5 should have the same label
        long label2 = result.get(3L);
        assertEquals(label2, result.get(4L));
        assertEquals(label2, result.get(5L));

        // The two components should have different labels
        assertNotEquals(label1, label2);

        // Labels should be the minimum vertex ID in each component
        assertEquals(0L, label1);
        assertEquals(3L, label2);
    }

    @Test
    void completeGraph_shouldFindOneComponent() {
        GraphView graph = TestGraphs.complete(5);
        ConnectedComponents cc = new ConnectedComponents();

        Map<Long, Long> initial = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            initial.put((long) i, (long) i);
        }

        Map<Long, Long> result = engine.execute(graph, cc, initial, 20);

        assertEquals(5, result.size());

        // All vertices should have the same label (0, the minimum)
        Set<Long> uniqueLabels = result.values().stream().collect(Collectors.toSet());
        assertEquals(1, uniqueLabels.size());
        assertEquals(0L, uniqueLabels.iterator().next());
    }
}
