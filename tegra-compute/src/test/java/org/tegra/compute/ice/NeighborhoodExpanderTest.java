package org.tegra.compute.ice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.api.SubgraphView;
import org.tegra.compute.gas.EdgeDirection;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the NeighborhoodExpander (1-hop expansion).
 */
class NeighborhoodExpanderTest {

    private NeighborhoodExpander expander;

    @BeforeEach
    void setUp() {
        expander = new NeighborhoodExpander();
    }

    /**
     * Builds a graph: 1->2->3->4, 2->4
     */
    private GraphView buildTestGraph() {
        PartitionStore store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("exp_v0");
        store.createInitialVersion(v0);
        WorkingVersion working = store.branch(v0);

        for (long id = 1; id <= 4; id++) {
            working.putVertex(id, new VertexData(id, Map.of()));
        }

        long[][] edges = {{1, 2}, {2, 3}, {3, 4}, {2, 4}};
        for (long[] e : edges) {
            EdgeKey ek = new EdgeKey(e[0], e[1], (short) 0);
            working.putEdge(e[0], e[1], (short) 0, new EdgeData(ek, Map.of()));
        }

        ByteArray v1 = ByteArray.fromString("exp_v1");
        store.commit(working, v1);
        return store.retrieve(v1);
    }

    @Test
    void expandOutDirection() {
        GraphView graph = buildTestGraph();

        // Expand vertex 2 outward
        SubgraphView view = expander.expand(Set.of(2L), graph, EdgeDirection.OUT);

        assertThat(view.activeVertexIds()).containsExactly(2L);
        // 2->3 and 2->4 are out-edges, so 3 and 4 are boundary
        assertThat(view.boundaryVertexIds()).containsExactlyInAnyOrder(3L, 4L);
    }

    @Test
    void expandInDirection() {
        GraphView graph = buildTestGraph();

        // Expand vertex 3 inward (who points to 3? vertex 2)
        SubgraphView view = expander.expand(Set.of(3L), graph, EdgeDirection.IN);

        assertThat(view.activeVertexIds()).containsExactly(3L);
        // 2->3 is the only edge pointing to 3, so 2 is boundary
        assertThat(view.boundaryVertexIds()).containsExactly(2L);
    }

    @Test
    void expandBothDirections() {
        GraphView graph = buildTestGraph();

        // Expand vertex 2 in both directions
        SubgraphView view = expander.expand(Set.of(2L), graph, EdgeDirection.BOTH);

        assertThat(view.activeVertexIds()).containsExactly(2L);
        // OUT: 2->3, 2->4 => boundary includes 3, 4
        // IN: 1->2 => boundary includes 1
        assertThat(view.boundaryVertexIds()).containsExactlyInAnyOrder(1L, 3L, 4L);
    }

    @Test
    void expandIsolatedVertex() {
        // Graph with isolated vertex 5
        PartitionStore store = new PartitionStore();
        ByteArray v0 = ByteArray.fromString("iso_v0");
        store.createInitialVersion(v0);
        WorkingVersion working = store.branch(v0);
        working.putVertex(5L, new VertexData(5L, Map.of()));
        ByteArray v1 = ByteArray.fromString("iso_v1");
        store.commit(working, v1);
        GraphView graph = store.retrieve(v1);

        SubgraphView view = expander.expand(Set.of(5L), graph, EdgeDirection.BOTH);

        assertThat(view.activeVertexIds()).containsExactly(5L);
        assertThat(view.boundaryVertexIds()).isEmpty();
    }

    @Test
    void expandMultipleCandidates() {
        GraphView graph = buildTestGraph();

        // Expand vertices 1 and 3 outward
        SubgraphView view = expander.expand(Set.of(1L, 3L), graph, EdgeDirection.OUT);

        assertThat(view.activeVertexIds()).containsExactlyInAnyOrder(1L, 3L);
        // 1->2 and 3->4 => boundary includes 2 and 4
        assertThat(view.boundaryVertexIds()).containsExactlyInAnyOrder(2L, 4L);
    }

    @Test
    void expandDoesNotDuplicateActiveAsBoundary() {
        GraphView graph = buildTestGraph();

        // If candidate set includes both 2 and 3, and there's edge 2->3,
        // vertex 3 should NOT appear in boundary since it's active
        SubgraphView view = expander.expand(Set.of(2L, 3L), graph, EdgeDirection.OUT);

        assertThat(view.activeVertexIds()).containsExactlyInAnyOrder(2L, 3L);
        // 2->3 has dst=3 which is active, so not boundary
        // 2->4 has dst=4 which is boundary
        // 3->4 has dst=4 which is boundary
        assertThat(view.boundaryVertexIds()).containsExactly(4L);
        assertThat(view.boundaryVertexIds()).doesNotContain(2L, 3L);
    }
}
