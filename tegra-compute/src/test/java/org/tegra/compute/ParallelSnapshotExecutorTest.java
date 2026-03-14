package org.tegra.compute;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.compute.ice.HeuristicSwitchOracle;
import org.tegra.compute.ice.IceEngine;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the ParallelSnapshotExecutor.
 */
class ParallelSnapshotExecutorTest {

    private GasEngine gasEngine;
    private IceEngine iceEngine;
    private ParallelSnapshotExecutor executor;
    private PartitionStore store;

    @BeforeEach
    void setUp() {
        gasEngine = new GasEngine();
        iceEngine = new IceEngine(gasEngine, new HeuristicSwitchOracle(0.5));
        executor = new ParallelSnapshotExecutor(gasEngine, iceEngine);
        store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("par_v0");
        store.createInitialVersion(v0);
    }

    /**
     * Connected components program.
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
        ByteArray v0 = ByteArray.fromString("par_v0");
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

        ByteArray vId = ByteArray.fromString("par_graph_" + System.nanoTime());
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

        ByteArray vId = ByteArray.fromString("par_graph_" + System.nanoTime());
        store.commit(working, vId);
        return store.retrieve(vId);
    }

    @Test
    void parallelExecutionOnThreeSnapshots() {
        // Snapshot 1: triangle 1-2-3
        GraphView g1 = buildGraph(new long[][]{
                {1, 2}, {2, 1}, {2, 3}, {3, 2}, {1, 3}, {3, 1}
        });

        // Snapshot 2: add vertex 4 connected to 3
        GraphView g2 = buildGraphFrom(g1.versionId(), new long[][]{
                {3, 4}, {4, 3}
        }, new long[]{4L});

        // Snapshot 3: add vertex 5 connected to 4
        GraphView g3 = buildGraphFrom(g2.versionId(), new long[][]{
                {4, 5}, {5, 4}
        }, new long[]{5L});

        List<GraphView> graphs = List.of(g1, g2, g3);
        Map<Long, Long> initial = new HashMap<>();
        initial.put(1L, 1L);
        initial.put(2L, 2L);
        initial.put(3L, 3L);

        Map<Integer, Map<Long, Long>> results = executor.executeParallel(
                graphs, new ConnectedComponents(), initial, 20);

        assertThat(results).hasSize(3);

        // Snapshot 1: all in one component
        Map<Long, Long> r1 = results.get(0);
        assertThat(r1.get(1L)).isEqualTo(1L);
        assertThat(r1.get(2L)).isEqualTo(1L);
        assertThat(r1.get(3L)).isEqualTo(1L);

        // Snapshot 2: vertex 4 joins the component
        Map<Long, Long> r2 = results.get(1);
        assertThat(r2.get(1L)).isEqualTo(1L);
        assertThat(r2.get(4L)).isEqualTo(1L);

        // Snapshot 3: vertex 5 joins the component
        Map<Long, Long> r3 = results.get(2);
        assertThat(r3.get(1L)).isEqualTo(1L);
        assertThat(r3.get(5L)).isEqualTo(1L);
    }

    @Test
    void singleSnapshotWorks() {
        GraphView g = buildGraph(new long[][]{
                {1, 2}, {2, 1}
        });

        Map<Long, Long> initial = Map.of(1L, 1L, 2L, 2L);

        Map<Integer, Map<Long, Long>> results = executor.executeParallel(
                List.of(g), new ConnectedComponents(), initial, 20);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).get(1L)).isEqualTo(1L);
        assertThat(results.get(0).get(2L)).isEqualTo(1L);
    }

    @Test
    void emptyGraphListReturnsEmpty() {
        Map<Integer, Map<Long, Long>> results = executor.executeParallel(
                List.of(), new ConnectedComponents(), Map.of(), 20);

        assertThat(results).isEmpty();
    }

    @Test
    void parallelResultsMatchSerial() {
        // Build 3 evolving snapshots
        GraphView g1 = buildGraph(new long[][]{
                {1, 2}, {2, 1}
        });

        GraphView g2 = buildGraphFrom(g1.versionId(), new long[][]{
                {2, 3}, {3, 2}
        }, new long[]{3L});

        GraphView g3 = buildGraphFrom(g2.versionId(), new long[][]{
                {3, 4}, {4, 3}
        }, new long[]{4L});

        Map<Long, Long> initial = new HashMap<>();
        initial.put(1L, 1L);
        initial.put(2L, 2L);

        // Parallel execution
        Map<Integer, Map<Long, Long>> parallelResults = executor.executeParallel(
                List.of(g1, g2, g3), new ConnectedComponents(), initial, 20);

        // Serial execution for comparison
        Map<Long, Long> serial1 = gasEngine.execute(g1, new ConnectedComponents(), initial, 20);

        Map<Long, Long> init2 = new HashMap<>(initial);
        init2.put(3L, 3L);
        Map<Long, Long> serial2 = gasEngine.execute(g2, new ConnectedComponents(), init2, 20);

        Map<Long, Long> init3 = new HashMap<>(initial);
        init3.put(3L, 3L);
        init3.put(4L, 4L);
        Map<Long, Long> serial3 = gasEngine.execute(g3, new ConnectedComponents(), init3, 20);

        // Verify all produce the same connected component structure
        // Snapshot 1: {1, 2} in one component
        assertThat(parallelResults.get(0).get(1L)).isEqualTo(serial1.get(1L));
        assertThat(parallelResults.get(0).get(2L)).isEqualTo(serial1.get(2L));

        // All vertices should be in component 1 for all snapshots
        for (var entry : parallelResults.get(2).entrySet()) {
            assertThat(entry.getValue()).isEqualTo(1L);
        }
    }
}
