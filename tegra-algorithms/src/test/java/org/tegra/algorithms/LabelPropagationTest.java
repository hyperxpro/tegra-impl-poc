package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LabelPropagationTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void twoTriangles_verticesInSameTriangleShouldHaveSameLabel() {
        GraphView graph = TestGraphs.twoTriangles();
        LabelPropagation lp = new LabelPropagation();

        // Initialize each vertex with its own ID
        Map<Long, Long> initial = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            initial.put((long) i, (long) i);
        }

        Map<Long, Long> result = engine.execute(graph, lp, initial, 20);

        assertEquals(6, result.size());

        // Vertices in the same triangle should converge to the same label
        long label1 = result.get(0L);
        assertEquals(label1, result.get(1L));
        assertEquals(label1, result.get(2L));

        long label2 = result.get(3L);
        assertEquals(label2, result.get(4L));
        assertEquals(label2, result.get(5L));

        // Different triangles should have different labels
        assertNotEquals(label1, label2);
    }
}
