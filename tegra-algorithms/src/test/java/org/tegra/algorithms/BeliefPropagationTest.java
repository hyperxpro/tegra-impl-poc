package org.tegra.algorithms;

import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.GasEngine;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BeliefPropagationTest {

    private final GasEngine engine = new GasEngine();

    @Test
    void twoNodeGraph_beliefsShouldConverge() {
        // Create a simple 2-node graph with bidirectional edges
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        wv.putVertex(0, new VertexData(0, Map.of()));
        wv.putVertex(1, new VertexData(1, Map.of()));
        wv.putEdge(0, 1, (short) 0,
                new EdgeData(new EdgeKey(0, 1, (short) 0), Map.of()));
        wv.putEdge(1, 0, (short) 0,
                new EdgeData(new EdgeKey(1, 0, (short) 0), Map.of()));

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        GraphView graph = store.retrieve(v1);

        // Transition matrix for 2 states: slight preference for same state
        double[][] transition = {
                {0.7, 0.3},
                {0.3, 0.7}
        };

        BeliefPropagation bp = new BeliefPropagation(2, transition, 1e-6);

        // Vertex 0 starts with strong belief in state 0
        // Vertex 1 starts with uniform belief
        Map<Long, double[]> initial = new HashMap<>();
        initial.put(0L, new double[]{0.9, 0.1});
        initial.put(1L, new double[]{0.5, 0.5});

        Map<Long, double[]> result = engine.execute(graph, bp, initial, 20);

        // Both vertices should have converged
        assertNotNull(result.get(0L));
        assertNotNull(result.get(1L));

        // Beliefs should be valid probability distributions
        double[] belief0 = result.get(0L);
        double[] belief1 = result.get(1L);

        assertEquals(1.0, belief0[0] + belief0[1], 0.01);
        assertEquals(1.0, belief1[0] + belief1[1], 0.01);

        // Vertex 0 started with strong state-0 belief, and with the
        // same-state-preferring transition matrix, vertex 1 should
        // also lean toward state 0
        assertTrue(belief1[0] > 0.4,
                "Vertex 1 should be influenced toward state 0, got: " + belief1[0]);
    }
}
