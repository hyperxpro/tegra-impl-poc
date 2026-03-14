package org.tegra.compute.ice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;
import org.tegra.api.GraphSnapshot;
import org.tegra.compute.TestGraphBuilder;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class IceEngineTest {

    private GasEngine gasEngine;
    private IceEngine iceEngine;

    /**
     * Simple program: each vertex's value becomes the sum of its in-neighbors' values.
     */
    private static VertexProgram<Double, Double, Double> sumNeighborsProgram() {
        return new VertexProgram<>() {
            @Override public String name() { return "SumNeighbors"; }
            @Override public EdgeDirection gatherDirection() { return EdgeDirection.IN; }
            @Override public EdgeDirection scatterDirection() { return EdgeDirection.OUT; }

            @Override
            public Double gather(Double vertexValue, Double edgeValue, Double neighborValue) {
                return neighborValue;
            }

            @Override public Double sum(Double a, Double b) { return a + b; }

            @Override
            public Double apply(Double currentValue, Double gathered) {
                return gathered;
            }

            @Override
            public boolean scatter(Double updatedValue, Double oldValue, Double edgeValue) {
                return !Objects.equals(updatedValue, oldValue);
            }

            @Override public Double identity() { return 0.0; }
            @Override public int maxIterations() { return 10; }
        };
    }

    @BeforeEach
    void setUp() {
        gasEngine = new GasEngine();
        iceEngine = new IceEngine(gasEngine, new DiffEngine(), new NeighborhoodExpander());
    }

    @Test
    void testIncrementalSameAsFullForSmallChange() {
        // Previous: 1->2, 2->3
        GraphSnapshot<Double, Double> previous = TestGraphBuilder.<Double, Double>create("v1")
                .addVertex(1, 10.0)
                .addVertex(2, 20.0)
                .addVertex(3, 30.0)
                .addEdge(1, 2, 1.0)
                .addEdge(2, 3, 1.0)
                .build();

        var program = sumNeighborsProgram();
        Map<Long, Double> previousResults = gasEngine.execute(previous, program);

        // New: same structure but vertex 1 value changed
        GraphSnapshot<Double, Double> newSnapshot = TestGraphBuilder.<Double, Double>create("v2")
                .addVertex(1, 99.0)
                .addVertex(2, 20.0)
                .addVertex(3, 30.0)
                .addEdge(1, 2, 1.0)
                .addEdge(2, 3, 1.0)
                .build();

        // Incremental result
        Map<Long, Double> incrementalResult = iceEngine.executeIncremental(
                newSnapshot, program, previous, previousResults);

        // Full recomputation result
        Map<Long, Double> fullResult = gasEngine.execute(newSnapshot, program);

        // They should agree on all vertices
        assertThat(incrementalResult).containsExactlyInAnyOrderEntriesOf(fullResult);
    }

    @Test
    void testNoChangesReturnsPreservesResults() {
        GraphSnapshot<Double, Double> snapshot = TestGraphBuilder.<Double, Double>create("v1")
                .addVertex(1, 10.0)
                .addVertex(2, 20.0)
                .addEdge(1, 2, 1.0)
                .build();

        var program = sumNeighborsProgram();
        Map<Long, Double> previousResults = gasEngine.execute(snapshot, program);

        // Same snapshot as "new" — no changes
        Map<Long, Double> incrementalResult = iceEngine.executeIncremental(
                snapshot, program, snapshot, previousResults);

        assertThat(incrementalResult).containsExactlyInAnyOrderEntriesOf(previousResults);
    }

    @Test
    void testAffectedSubgraphExpansion() {
        // Graph: 1->2->3->4->5
        GraphSnapshot<Double, Double> previous = TestGraphBuilder.<Double, Double>create("v1")
                .addVertex(1, 1.0)
                .addVertex(2, 2.0)
                .addVertex(3, 3.0)
                .addVertex(4, 4.0)
                .addVertex(5, 5.0)
                .addEdge(1, 2, 1.0)
                .addEdge(2, 3, 1.0)
                .addEdge(3, 4, 1.0)
                .addEdge(4, 5, 1.0)
                .build();

        var program = sumNeighborsProgram();
        Map<Long, Double> previousResults = gasEngine.execute(previous, program);

        // Change only vertex 3's value
        GraphSnapshot<Double, Double> newSnapshot = TestGraphBuilder.<Double, Double>create("v2")
                .addVertex(1, 1.0)
                .addVertex(2, 2.0)
                .addVertex(3, 99.0) // changed
                .addVertex(4, 4.0)
                .addVertex(5, 5.0)
                .addEdge(1, 2, 1.0)
                .addEdge(2, 3, 1.0)
                .addEdge(3, 4, 1.0)
                .addEdge(4, 5, 1.0)
                .build();

        Map<Long, Double> incrementalResult = iceEngine.executeIncremental(
                newSnapshot, program, previous, previousResults);

        // The result should cover all 5 vertices
        assertThat(incrementalResult).hasSize(5);

        // The affected region includes vertex 3 + 1-hop = {2, 3, 4}
        // Vertex 1 and 5 should preserve their values from previousResults
        // (vertex 1 has no in-edges -> 0.0, vertex 5 has in from 4)
        assertThat(incrementalResult.get(1L)).isEqualTo(previousResults.get(1L));
    }

    @Test
    void testAddedEdgeIsDetected() {
        // Previous: 1->2, 3 isolated
        GraphSnapshot<Double, Double> previous = TestGraphBuilder.<Double, Double>create("v1")
                .addVertex(1, 10.0)
                .addVertex(2, 20.0)
                .addVertex(3, 30.0)
                .addEdge(1, 2, 1.0)
                .build();

        var program = sumNeighborsProgram();
        Map<Long, Double> previousResults = gasEngine.execute(previous, program);

        // New: add edge 2->3
        GraphSnapshot<Double, Double> newSnapshot = TestGraphBuilder.<Double, Double>create("v2")
                .addVertex(1, 10.0)
                .addVertex(2, 20.0)
                .addVertex(3, 30.0)
                .addEdge(1, 2, 1.0)
                .addEdge(2, 3, 1.0) // new edge
                .build();

        Map<Long, Double> incrementalResult = iceEngine.executeIncremental(
                newSnapshot, program, previous, previousResults);
        Map<Long, Double> fullResult = gasEngine.execute(newSnapshot, program);

        assertThat(incrementalResult).containsExactlyInAnyOrderEntriesOf(fullResult);
    }
}
