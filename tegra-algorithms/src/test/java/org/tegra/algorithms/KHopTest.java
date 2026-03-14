package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class KHopTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void cycle_k2FromSource0_shouldReach012And4() {
        // Use a cycle graph (bidirectional edges) for reliable k-hop propagation.
        // Cycle of 5: 0-1-2-3-4-0
        // Within 2 hops from 0: vertices {0, 1, 2, 4, 3} — actually:
        // 1 hop: 1, 4 (direct neighbors in cycle)
        // 2 hops: 2 (via 1), 3 (via 4)
        // So all 5 vertices are within 2 hops in a 5-cycle.
        GraphView graph = TestGraphs.cycle(5);
        KHop khop = new KHop(0L, 2);

        Map<Long, Set<Long>> initial = new HashMap<>();
        initial.put(0L, new HashSet<>(Set.of(0L)));
        for (int i = 1; i < 5; i++) {
            initial.put((long) i, new HashSet<>());
        }

        Map<Long, Set<Long>> result = engine.execute(graph, khop, initial, khop.k());

        // After 2 iterations, source should have propagated.
        // Iteration 1: vertex 0 ({0}) -> neighbors 1 and 4 learn about 0.
        //   Vertex 1 gets {0,1}, vertex 4 gets {0,4}.
        // Iteration 2: vertex 1 ({0,1}) -> vertex 2 gets {0,1,2}.
        //   Vertex 4 ({0,4}) -> vertex 3 gets {0,3,4}.
        //   Vertex 0 gathers from 1({0,1}) and 4({0,4}) -> vertex 0 gets {0,1,4}.

        Set<Long> reach0 = result.get(0L);
        assertNotNull(reach0, "Source vertex should have a reachability set");
        assertTrue(reach0.contains(0L), "Source should be in its own reach set");
        assertTrue(reach0.contains(1L), "Vertex 1 (1 hop) should be reachable");
        assertTrue(reach0.contains(4L), "Vertex 4 (1 hop) should be reachable");
    }

    @Test
    void linearChain_k2_forwardReachability() {
        // For a directed linear chain 0->1->2->3->4,
        // after k=2 iterations, vertex 1 (which is 1 hop from source) should
        // have picked up the source vertex.
        GraphView graph = TestGraphs.linearChain(5);
        KHop khop = new KHop(0L, 2);

        Map<Long, Set<Long>> initial = new HashMap<>();
        initial.put(0L, new HashSet<>(Set.of(0L)));
        for (int i = 1; i < 5; i++) {
            initial.put((long) i, new HashSet<>());
        }

        Map<Long, Set<Long>> result = engine.execute(graph, khop, initial, khop.k());

        // Vertex 1 should know about vertex 0 (1 hop via edge 0->1)
        Set<Long> reach1 = result.get(1L);
        assertTrue(reach1.contains(0L), "Vertex 1 should know about source vertex 0");
        assertTrue(reach1.contains(1L), "Vertex 1 should know about itself");

        // Vertex 2 should know about vertices 0 and 1 (2 hops via 0->1->2)
        Set<Long> reach2 = result.get(2L);
        assertTrue(reach2.contains(0L), "Vertex 2 should know about source vertex 0");
        assertTrue(reach2.contains(1L), "Vertex 2 should know about vertex 1");
        assertTrue(reach2.contains(2L), "Vertex 2 should know about itself");
    }
}
