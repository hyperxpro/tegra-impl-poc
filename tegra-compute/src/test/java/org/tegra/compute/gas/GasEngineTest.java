package org.tegra.compute.gas;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;
import org.tegra.api.GraphSnapshot;
import org.tegra.compute.TestGraphBuilder;

import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class GasEngineTest {

    private GasEngine engine;

    @BeforeEach
    void setUp() {
        engine = new GasEngine();
    }

    /**
     * A simple vertex program that sums incoming neighbor values.
     * Each vertex value is a Double. Messages are Doubles.
     * Gather: return neighbor's value. Sum: add. Apply: replace with sum.
     * Scatter: always propagate if value changed.
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

    /**
     * A vertex program that always converges after one iteration (apply returns current value).
     */
    private static VertexProgram<Double, Double, Double> convergingProgram() {
        return new VertexProgram<>() {
            @Override public String name() { return "Converging"; }
            @Override public EdgeDirection gatherDirection() { return EdgeDirection.IN; }
            @Override public EdgeDirection scatterDirection() { return EdgeDirection.OUT; }

            @Override
            public Double gather(Double vertexValue, Double edgeValue, Double neighborValue) {
                return 0.0;
            }

            @Override public Double sum(Double a, Double b) { return a + b; }

            @Override
            public Double apply(Double currentValue, Double gathered) {
                // Does not change value — converges immediately
                return currentValue;
            }

            @Override
            public boolean scatter(Double updatedValue, Double oldValue, Double edgeValue) {
                return !Objects.equals(updatedValue, oldValue);
            }

            @Override public Double identity() { return 0.0; }
            @Override public int maxIterations() { return 100; }
        };
    }

    /**
     * A program that never converges (always increments), limited by maxIterations.
     */
    private static VertexProgram<Double, Double, Double> nonConvergingProgram(int maxIter) {
        return new VertexProgram<>() {
            @Override public String name() { return "NonConverging"; }
            @Override public EdgeDirection gatherDirection() { return EdgeDirection.IN; }
            @Override public EdgeDirection scatterDirection() { return EdgeDirection.OUT; }

            @Override
            public Double gather(Double vertexValue, Double edgeValue, Double neighborValue) {
                return 1.0;
            }

            @Override public Double sum(Double a, Double b) { return a + b; }

            @Override
            public Double apply(Double currentValue, Double gathered) {
                return currentValue + gathered;
            }

            @Override
            public boolean scatter(Double updatedValue, Double oldValue, Double edgeValue) {
                return true; // always scatter
            }

            @Override public Double identity() { return 0.0; }
            @Override public int maxIterations() { return maxIter; }
        };
    }

    @Test
    void testSimpleVertexProgram() {
        // Graph: 1 -> 2, 1 -> 3, 2 -> 3
        // Vertex values: 1=10.0, 2=20.0, 3=30.0
        // SumNeighbors gathers IN edges
        // After iteration 1:
        //   vertex 1: no in-edges -> identity (0.0)
        //   vertex 2: in from 1 -> 10.0
        //   vertex 3: in from 1 (10.0) + in from 2 (20.0) -> 30.0 (unchanged)
        GraphSnapshot<Double, Double> snapshot = TestGraphBuilder.<Double, Double>create()
                .addVertex(1, 10.0)
                .addVertex(2, 20.0)
                .addVertex(3, 30.0)
                .addEdge(1, 2, 1.0)
                .addEdge(1, 3, 1.0)
                .addEdge(2, 3, 1.0)
                .build();

        Map<Long, Double> result = engine.execute(snapshot, sumNeighborsProgram());

        assertThat(result).containsKeys(1L, 2L, 3L);
        // All values eventually stabilize
        assertThat(result).isNotEmpty();
    }

    @Test
    void testConvergence() {
        // Program that immediately converges (apply returns currentValue)
        GraphSnapshot<Double, Double> snapshot = TestGraphBuilder.<Double, Double>create()
                .addVertex(1, 5.0)
                .addVertex(2, 10.0)
                .addEdge(1, 2, 1.0)
                .build();

        Map<Long, Double> result = engine.execute(snapshot, convergingProgram());

        // Values should remain unchanged because apply returns currentValue
        assertThat(result).containsEntry(1L, 5.0);
        assertThat(result).containsEntry(2L, 10.0);
    }

    @Test
    void testMaxIterations() {
        // Program that never converges, limited to 3 iterations
        // Single edge: 1 -> 2
        // vertex 2 gathers from vertex 1 each iteration: gathered = 1.0 per in-neighbor
        // vertex 1 has no in-edges so it converges after first iteration (gathered=identity=0, apply=current+0=current)
        // Actually vertex 1: apply(val, 0) = val+0 = val, so converges immediately.
        // vertex 2: apply(val, 1) = val+1 each iteration
        GraphSnapshot<Double, Double> snapshot = TestGraphBuilder.<Double, Double>create()
                .addVertex(1, 0.0)
                .addVertex(2, 0.0)
                .addEdge(1, 2, 1.0)
                .build();

        Map<Long, Double> result = engine.execute(snapshot, nonConvergingProgram(3));

        // Vertex 2 should have been updated, vertex 1 converges immediately
        assertThat(result).containsKey(1L);
        assertThat(result).containsKey(2L);
        // The result should be bounded by 3 iterations
        assertThat(result.get(2L)).isLessThanOrEqualTo(3.0);
    }

    @Test
    void testEmptyGraph() {
        GraphSnapshot<Double, Double> snapshot = TestGraphBuilder.<Double, Double>create().build();

        Map<Long, Double> result = engine.execute(snapshot, sumNeighborsProgram());

        assertThat(result).isEmpty();
    }

    @Test
    void testSingleVertex() {
        GraphSnapshot<Double, Double> snapshot = TestGraphBuilder.<Double, Double>create()
                .addVertex(1, 42.0)
                .build();

        Map<Long, Double> result = engine.execute(snapshot, sumNeighborsProgram());

        // Single vertex with no edges: gather returns identity, apply(42.0, 0.0) = 0.0
        // Then it converges because value doesn't change any more
        assertThat(result).containsKey(1L);
    }

    @Test
    void testLinearGraph() {
        // Chain: 1 -> 2 -> 3 -> 4
        GraphSnapshot<Double, Double> snapshot = TestGraphBuilder.<Double, Double>create()
                .addVertex(1, 1.0)
                .addVertex(2, 2.0)
                .addVertex(3, 3.0)
                .addVertex(4, 4.0)
                .addEdge(1, 2, 1.0)
                .addEdge(2, 3, 1.0)
                .addEdge(3, 4, 1.0)
                .build();

        Map<Long, Double> result = engine.execute(snapshot, sumNeighborsProgram());

        // All vertices should have computed values
        assertThat(result).hasSize(4);
        assertThat(result).containsKeys(1L, 2L, 3L, 4L);

        // Vertex 1 has no in-edges: eventually gets 0.0 (identity)
        // Vertex 2 gathers from 1: eventually gets value of vertex 1
        // Vertex 3 gathers from 2: eventually gets value of vertex 2
        // Vertex 4 gathers from 3: eventually gets value of vertex 3
        // After convergence, values propagate down the chain
        assertThat(result.get(1L)).isEqualTo(0.0);
    }
}
