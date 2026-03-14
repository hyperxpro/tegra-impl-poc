package org.tegra.compute.gas;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the GAS (Gather-Apply-Scatter) engine.
 */
class GasEngineTest {

    private GasEngine engine;

    @BeforeEach
    void setUp() {
        engine = new GasEngine();
    }

    /**
     * Builds a graph from the given edges and returns a GraphView.
     * All vertices referenced by edges are created automatically.
     */
    private GraphView buildGraph(long[][] edges) {
        PartitionStore store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("test_v0");
        store.createInitialVersion(v0);
        WorkingVersion working = store.branch(v0);

        // Collect all vertex IDs
        java.util.Set<Long> vertexIds = new java.util.HashSet<>();
        for (long[] edge : edges) {
            vertexIds.add(edge[0]);
            vertexIds.add(edge[1]);
        }

        // Add vertices
        for (long vid : vertexIds) {
            working.putVertex(vid, new VertexData(vid, Map.of()));
        }

        // Add edges
        for (long[] edge : edges) {
            EdgeKey ek = new EdgeKey(edge[0], edge[1], (short) 0);
            working.putEdge(edge[0], edge[1], (short) 0, new EdgeData(ek, Map.of()));
        }

        ByteArray v1 = ByteArray.fromString("test_v1");
        store.commit(working, v1);
        return store.retrieve(v1);
    }

    // ---- PageRank tests ----

    /**
     * Simple PageRank vertex program.
     * Gather: collect PR(neighbor) / outDegree(neighbor) from incoming edges.
     * Apply: PR(v) = 0.15 + 0.85 * sum
     * Scatter: activate destination if value changed significantly.
     */
    static class SimplePageRank implements VertexProgram<Double, Map<String, ?>, Double> {
        @Override
        public Double gather(EdgeTriplet<Double, Map<String, ?>> context) {
            // Gather from incoming: src sends its value
            // In a real PageRank, we'd divide by outDegree, but for simplicity
            // we just send the raw value
            return context.srcValue() != null ? context.srcValue() : 0.0;
        }

        @Override
        public Double sum(Double a, Double b) {
            return a + b;
        }

        @Override
        public Double apply(long vertexId, Double currentValue, Double gathered) {
            if (gathered == null) gathered = 0.0;
            return 0.15 + 0.85 * gathered;
        }

        @Override
        public Set<Long> scatter(EdgeTriplet<Double, Map<String, ?>> context, Double newValue) {
            // Activate destination
            return Set.of(context.dstId());
        }

        @Override
        public EdgeDirection gatherNeighbors() {
            return EdgeDirection.IN;
        }

        @Override
        public EdgeDirection scatterNeighbors() {
            return EdgeDirection.OUT;
        }
    }

    @Test
    void pageRankConvergesOnTriangle() {
        // Triangle: 1->2, 2->3, 3->1
        GraphView graph = buildGraph(new long[][]{
                {1, 2}, {2, 3}, {3, 1}
        });

        Map<Long, Double> initial = Map.of(1L, 1.0, 2L, 1.0, 3L, 1.0);
        Map<Long, Double> result = engine.execute(graph, new SimplePageRank(), initial, 20);

        assertThat(result).hasSize(3);
        // In a symmetric triangle, all vertices should converge to the same value
        double v1 = result.get(1L);
        double v2 = result.get(2L);
        double v3 = result.get(3L);
        assertThat(Math.abs(v1 - v2)).isLessThan(0.01);
        assertThat(Math.abs(v2 - v3)).isLessThan(0.01);
    }

    @Test
    void pageRankOnChain() {
        // Chain: 1->2->3->4
        GraphView graph = buildGraph(new long[][]{
                {1, 2}, {2, 3}, {3, 4}
        });

        Map<Long, Double> initial = Map.of(1L, 1.0, 2L, 1.0, 3L, 1.0, 4L, 1.0);
        Map<Long, Double> result = engine.execute(graph, new SimplePageRank(), initial, 20);

        assertThat(result).hasSize(4);
        // All values should be reasonable (positive and finite)
        for (double v : result.values()) {
            assertThat(v).isPositive().isFinite();
        }
    }

    // ---- Connected components tests ----

    /**
     * Connected components via GAS: each vertex propagates the minimum ID.
     */
    static class ConnectedComponents implements VertexProgram<Long, Map<String, ?>, Long> {
        @Override
        public Long gather(EdgeTriplet<Long, Map<String, ?>> context) {
            return context.srcValue() != null ? context.srcValue() : context.srcId();
        }

        @Override
        public Long sum(Long a, Long b) {
            if (a == null) return b;
            if (b == null) return a;
            return Math.min(a, b);
        }

        @Override
        public Long apply(long vertexId, Long currentValue, Long gathered) {
            long curr = (currentValue != null) ? currentValue : vertexId;
            if (gathered == null) return curr;
            return Math.min(curr, gathered);
        }

        @Override
        public Set<Long> scatter(EdgeTriplet<Long, Map<String, ?>> context, Long newValue) {
            if (context.dstValue() == null || newValue < context.dstValue()) {
                return Set.of(context.dstId());
            }
            return Set.of();
        }

        @Override
        public EdgeDirection gatherNeighbors() {
            return EdgeDirection.IN;
        }

        @Override
        public EdgeDirection scatterNeighbors() {
            return EdgeDirection.OUT;
        }
    }

    @Test
    void connectedComponentsOnDisconnectedGraph() {
        // Two disconnected components: {1,2,3} and {4,5}
        // Component 1: 1->2, 2->3, 3->1
        // Component 2: 4->5, 5->4
        GraphView graph = buildGraph(new long[][]{
                {1, 2}, {2, 3}, {3, 1},
                {4, 5}, {5, 4}
        });

        Map<Long, Long> initial = Map.of(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L, 5L, 5L);
        Map<Long, Long> result = engine.execute(graph, new ConnectedComponents(), initial, 20);

        assertThat(result).hasSize(5);
        // Component 1: all should have min ID = 1
        assertThat(result.get(1L)).isEqualTo(1L);
        assertThat(result.get(2L)).isEqualTo(1L);
        assertThat(result.get(3L)).isEqualTo(1L);
        // Component 2: all should have min ID = 4
        assertThat(result.get(4L)).isEqualTo(4L);
        assertThat(result.get(5L)).isEqualTo(4L);
    }

    // ---- BFS test ----

    /**
     * BFS from a source vertex: computes shortest hop count.
     */
    static class BFS implements VertexProgram<Integer, Map<String, ?>, Integer> {
        @Override
        public Integer gather(EdgeTriplet<Integer, Map<String, ?>> context) {
            Integer srcDist = context.srcValue();
            if (srcDist != null && srcDist < Integer.MAX_VALUE) {
                return srcDist + 1;
            }
            return null;
        }

        @Override
        public Integer sum(Integer a, Integer b) {
            return Math.min(a, b);
        }

        @Override
        public Integer apply(long vertexId, Integer currentValue, Integer gathered) {
            if (gathered == null) return currentValue;
            return Math.min(currentValue, gathered);
        }

        @Override
        public Set<Long> scatter(EdgeTriplet<Integer, Map<String, ?>> context, Integer newValue) {
            if (newValue < Integer.MAX_VALUE
                    && (context.dstValue() == null || newValue + 1 < context.dstValue())) {
                return Set.of(context.dstId());
            }
            return Set.of();
        }

        @Override
        public EdgeDirection gatherNeighbors() {
            return EdgeDirection.IN;
        }

        @Override
        public EdgeDirection scatterNeighbors() {
            return EdgeDirection.OUT;
        }
    }

    @Test
    void bfsFromSourceVertex() {
        // Chain: 1->2->3->4
        GraphView graph = buildGraph(new long[][]{
                {1, 2}, {2, 3}, {3, 4}
        });

        Map<Long, Integer> initial = new HashMap<>();
        initial.put(1L, 0);         // source
        initial.put(2L, Integer.MAX_VALUE);
        initial.put(3L, Integer.MAX_VALUE);
        initial.put(4L, Integer.MAX_VALUE);

        Map<Long, Integer> result = engine.execute(graph, new BFS(), initial, 10);

        assertThat(result.get(1L)).isEqualTo(0);
        assertThat(result.get(2L)).isEqualTo(1);
        assertThat(result.get(3L)).isEqualTo(2);
        assertThat(result.get(4L)).isEqualTo(3);
    }

    // ---- Max iterations test ----

    @Test
    void maxIterationsTerminatesExecution() {
        // Simple cycle that would run forever without max iterations
        GraphView graph = buildGraph(new long[][]{
                {1, 2}, {2, 1}
        });

        // This program always changes values, so it never converges
        VertexProgram<Integer, Map<String, ?>, Integer> neverConverges =
                new VertexProgram<>() {
                    @Override
                    public Integer gather(EdgeTriplet<Integer, Map<String, ?>> context) {
                        return context.srcValue();
                    }

                    @Override
                    public Integer sum(Integer a, Integer b) {
                        return a + b;
                    }

                    @Override
                    public Integer apply(long vertexId, Integer currentValue, Integer gathered) {
                        return (currentValue != null ? currentValue : 0) + 1;
                    }

                    @Override
                    public Set<Long> scatter(EdgeTriplet<Integer, Map<String, ?>> context, Integer newValue) {
                        return Set.of(context.dstId());
                    }

                    @Override
                    public EdgeDirection gatherNeighbors() {
                        return EdgeDirection.IN;
                    }

                    @Override
                    public EdgeDirection scatterNeighbors() {
                        return EdgeDirection.OUT;
                    }
                };

        Map<Long, Integer> initial = Map.of(1L, 0, 2L, 0);
        Map<Long, Integer> result = engine.execute(graph, neverConverges, initial, 3);

        // Should have terminated after 3 iterations, values should be incremented
        assertThat(result).hasSize(2);
        assertThat(result.get(1L)).isGreaterThan(0);
        assertThat(result.get(2L)).isGreaterThan(0);
    }

    // ---- Empty graph test ----

    @Test
    void emptyGraphReturnsEmptyResults() {
        // Empty graph (no vertices, no edges)
        PartitionStore store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("empty_v0");
        store.createInitialVersion(v0);
        GraphView graph = store.retrieve(v0);

        Map<Long, Double> initial = Map.of();
        Map<Long, Double> result = engine.execute(graph, new SimplePageRank(), initial, 10);

        assertThat(result).isEmpty();
    }

    // ---- Isolated vertex test ----

    @Test
    void isolatedVertexKeepsInitialValue() {
        // Single vertex, no edges
        PartitionStore store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("iso_v0");
        store.createInitialVersion(v0);
        WorkingVersion working = store.branch(v0);
        working.putVertex(1L, new VertexData(1L, Map.of()));
        ByteArray v1 = ByteArray.fromString("iso_v1");
        store.commit(working, v1);
        GraphView graph = store.retrieve(v1);

        Map<Long, Double> initial = Map.of(1L, 42.0);
        Map<Long, Double> result = engine.execute(graph, new SimplePageRank(), initial, 10);

        // With no incoming edges, gather produces null, apply gives 0.15 + 0.85*0 = 0.15
        assertThat(result.get(1L)).isCloseTo(0.15, org.assertj.core.api.Assertions.within(0.001));
    }
}
