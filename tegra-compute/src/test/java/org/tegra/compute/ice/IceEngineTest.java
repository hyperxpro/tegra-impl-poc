package org.tegra.compute.ice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;
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
 * Tests for the ICE (Incremental Computation by Entity expansion) engine.
 */
class IceEngineTest {

    private GasEngine gasEngine;
    private IceEngine iceEngine;
    private PartitionStore store;

    @BeforeEach
    void setUp() {
        gasEngine = new GasEngine();
        iceEngine = new IceEngine(gasEngine, new HeuristicSwitchOracle(0.5));
        store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("ice_v0");
        store.createInitialVersion(v0);
    }

    /**
     * Connected components program used across tests.
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

    private GraphView buildGraph(long[][] edges) {
        ByteArray v0 = ByteArray.fromString("ice_v0");
        WorkingVersion working = store.branch(v0);

        java.util.Set<Long> vertexIds = new java.util.HashSet<>();
        for (long[] edge : edges) {
            vertexIds.add(edge[0]);
            vertexIds.add(edge[1]);
        }
        for (long vid : vertexIds) {
            working.putVertex(vid, new VertexData(vid, Map.of()));
        }
        for (long[] edge : edges) {
            EdgeKey ek = new EdgeKey(edge[0], edge[1], (short) 0);
            working.putEdge(edge[0], edge[1], (short) 0, new EdgeData(ek, Map.of()));
        }

        ByteArray vId = ByteArray.fromString("ice_graph_" + System.nanoTime());
        store.commit(working, vId);
        return store.retrieve(vId);
    }

    private GraphView buildGraphFrom(ByteArray baseVersionId, long[][] addEdges, long[] addVertices) {
        WorkingVersion working = store.branch(baseVersionId);

        for (long vid : addVertices) {
            working.putVertex(vid, new VertexData(vid, Map.of()));
        }
        for (long[] edge : addEdges) {
            working.putVertex(edge[0], new VertexData(edge[0], Map.of()));
            working.putVertex(edge[1], new VertexData(edge[1], Map.of()));
            EdgeKey ek = new EdgeKey(edge[0], edge[1], (short) 0);
            working.putEdge(edge[0], edge[1], (short) 0, new EdgeData(ek, Map.of()));
        }

        ByteArray vId = ByteArray.fromString("ice_graph_" + System.nanoTime());
        store.commit(working, vId);
        return store.retrieve(vId);
    }

    @Test
    void incrementalResultMatchesFullRecomputation() {
        // Build initial graph: 1->2->3 (all bidirectional for CC)
        GraphView graph1 = buildGraph(new long[][]{
                {1, 2}, {2, 1}, {2, 3}, {3, 2}
        });

        // Full computation on graph1
        Map<Long, Long> initial1 = Map.of(1L, 1L, 2L, 2L, 3L, 3L);
        Map<Long, Long> result1 = gasEngine.execute(graph1, new ConnectedComponents(), initial1, 20);

        // Build modified graph: add vertex 4 connected to 3
        GraphView graph2 = buildGraphFrom(graph1.versionId(), new long[][]{
                {3, 4}, {4, 3}
        }, new long[]{4L});

        // Full computation on graph2
        Map<Long, Long> initial2 = new HashMap<>(Map.of(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L));
        Map<Long, Long> fullResult = gasEngine.execute(graph2, new ConnectedComponents(), initial2, 20);

        // ICE incremental computation on graph2 using result1 as previous
        Map<Long, Long> iceResult = iceEngine.incPregel(
                graph2, result1, graph1, new ConnectedComponents(), 20);

        // Both should agree: all vertices should have component ID = 1
        assertThat(fullResult.get(1L)).isEqualTo(1L);
        assertThat(fullResult.get(2L)).isEqualTo(1L);
        assertThat(fullResult.get(3L)).isEqualTo(1L);
        assertThat(fullResult.get(4L)).isEqualTo(1L);

        assertThat(iceResult.get(1L)).isEqualTo(1L);
        assertThat(iceResult.get(2L)).isEqualTo(1L);
        assertThat(iceResult.get(3L)).isEqualTo(1L);
        assertThat(iceResult.get(4L)).isEqualTo(1L);
    }

    @Test
    void noChangesReturnsPreviousResults() {
        // Same graph for both "snapshots"
        GraphView graph = buildGraph(new long[][]{
                {1, 2}, {2, 1}, {2, 3}, {3, 2}
        });

        Map<Long, Long> initial = Map.of(1L, 1L, 2L, 2L, 3L, 3L);
        Map<Long, Long> result = gasEngine.execute(graph, new ConnectedComponents(), initial, 20);

        // ICE with same graph for current and previous
        Map<Long, Long> iceResult = iceEngine.incPregel(
                graph, result, graph, new ConnectedComponents(), 20);

        // Should return same results
        assertThat(iceResult).containsAllEntriesOf(result);
    }

    @Test
    void smallChangeRecomputesOnlyAffectedVertices() {
        // Build graph with two components: {1,2,3} and {4,5}
        GraphView graph1 = buildGraph(new long[][]{
                {1, 2}, {2, 1}, {2, 3}, {3, 2},
                {4, 5}, {5, 4}
        });

        Map<Long, Long> initial1 = Map.of(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L, 5L, 5L);
        Map<Long, Long> result1 = gasEngine.execute(graph1, new ConnectedComponents(), initial1, 20);

        // Verify initial result: two components
        assertThat(result1.get(1L)).isEqualTo(1L);
        assertThat(result1.get(4L)).isEqualTo(4L);

        // Connect the two components: add edge 3->4 and 4->3
        GraphView graph2 = buildGraphFrom(graph1.versionId(), new long[][]{
                {3, 4}, {4, 3}
        }, new long[]{});

        // ICE should detect the edge change and recompute affected vertices
        Map<Long, Long> iceResult = iceEngine.incPregel(
                graph2, result1, graph1, new ConnectedComponents(), 20);

        // Now all should be in the same component
        assertThat(iceResult.get(1L)).isEqualTo(1L);
        assertThat(iceResult.get(2L)).isEqualTo(1L);
        assertThat(iceResult.get(3L)).isEqualTo(1L);
        assertThat(iceResult.get(4L)).isEqualTo(1L);
        assertThat(iceResult.get(5L)).isEqualTo(1L);
    }

    @Test
    void switchOracleTriggersFallbackToFullComputation() {
        // Use a switch oracle with very low threshold so it always switches
        IceEngine alwaysSwitchEngine = new IceEngine(gasEngine, new HeuristicSwitchOracle(0.0));

        GraphView graph1 = buildGraph(new long[][]{
                {1, 2}, {2, 1}
        });

        Map<Long, Long> initial = Map.of(1L, 1L, 2L, 2L);
        Map<Long, Long> result1 = gasEngine.execute(graph1, new ConnectedComponents(), initial, 20);

        // Modify graph slightly
        GraphView graph2 = buildGraphFrom(graph1.versionId(), new long[][]{
                {2, 3}, {3, 2}
        }, new long[]{3L});

        // With threshold=0.0, any non-zero affected count triggers switch
        Map<Long, Long> result = alwaysSwitchEngine.incPregel(
                graph2, result1, graph1, new ConnectedComponents(), 20);

        // Should still produce correct results (just via full computation)
        assertThat(result.get(1L)).isEqualTo(1L);
        assertThat(result.get(2L)).isEqualTo(1L);
        assertThat(result.get(3L)).isEqualTo(1L);
    }

    @Test
    void handlesVertexDeletion() {
        GraphView graph1 = buildGraph(new long[][]{
                {1, 2}, {2, 1}, {2, 3}, {3, 2}
        });

        Map<Long, Long> initial = Map.of(1L, 1L, 2L, 2L, 3L, 3L);
        Map<Long, Long> result1 = gasEngine.execute(graph1, new ConnectedComponents(), initial, 20);

        // Remove vertex 3 and its edges
        WorkingVersion working = store.branch(graph1.versionId());
        working.removeVertex(3L);
        working.removeEdge(2L, 3L, (short) 0);
        working.removeEdge(3L, 2L, (short) 0);
        ByteArray v2Id = ByteArray.fromString("ice_graph_del_" + System.nanoTime());
        store.commit(working, v2Id);
        GraphView graph2 = store.retrieve(v2Id);

        Map<Long, Long> iceResult = iceEngine.incPregel(
                graph2, result1, graph1, new ConnectedComponents(), 20);

        // Vertex 3 should be removed from results
        assertThat(iceResult).doesNotContainKey(3L);
        // Vertices 1 and 2 should still be in the same component
        assertThat(iceResult.get(1L)).isEqualTo(1L);
        assertThat(iceResult.get(2L)).isEqualTo(1L);
    }
}
