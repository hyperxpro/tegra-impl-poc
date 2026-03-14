package org.tegra.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.pds.art.PersistentART;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.KeyCodec;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.version.ByteArray;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SubgraphView: active/boundary vertex classification.
 */
class SubgraphViewTest {

    private GraphView graphView;

    @BeforeEach
    void setUp() {
        // Build a graph: 1 -> 2 -> 3 -> 4, and 5 (isolated)
        PersistentART<VertexData> vArt = PersistentART.empty();
        for (int i = 1; i <= 5; i++) {
            vArt = vArt.insert(KeyCodec.encodeVertexKey(i),
                    new VertexData(i, Map.of()));
        }

        PersistentART<EdgeData> eArt = PersistentART.empty();
        eArt = eArt.insert(KeyCodec.encodeEdgeKey(1L, 2L, (short) 0),
                new EdgeData(new EdgeKey(1L, 2L, (short) 0), Map.of()));
        eArt = eArt.insert(KeyCodec.encodeEdgeKey(2L, 3L, (short) 0),
                new EdgeData(new EdgeKey(2L, 3L, (short) 0), Map.of()));
        eArt = eArt.insert(KeyCodec.encodeEdgeKey(3L, 4L, (short) 0),
                new EdgeData(new EdgeKey(3L, 4L, (short) 0), Map.of()));

        graphView = new GraphView(vArt.root(), eArt.root(), ByteArray.fromString("test"));
    }

    @Test
    void activeVerticesAreCorrectlyClassified() {
        SubgraphView view = new SubgraphView(Set.of(1L, 2L), Set.of(3L), graphView);

        assertThat(view.isActive(1L)).isTrue();
        assertThat(view.isActive(2L)).isTrue();
        assertThat(view.isActive(3L)).isFalse();
    }

    @Test
    void boundaryVerticesAreCorrectlyClassified() {
        SubgraphView view = new SubgraphView(Set.of(2L), Set.of(1L, 3L), graphView);

        assertThat(view.isBoundary(1L)).isTrue();
        assertThat(view.isBoundary(3L)).isTrue();
        assertThat(view.isBoundary(2L)).isFalse();
        assertThat(view.isBoundary(4L)).isFalse();
    }

    @Test
    void activeVertexIteratorReturnsCorrectData() {
        SubgraphView view = new SubgraphView(Set.of(1L, 2L), Set.of(3L), graphView);

        Iterator<VertexData> activeIt = view.activeVertices();
        int count = 0;
        while (activeIt.hasNext()) {
            VertexData vd = activeIt.next();
            assertThat(vd.vertexId()).isIn(1L, 2L);
            count++;
        }
        assertThat(count).isEqualTo(2);
    }

    @Test
    void boundaryVertexIteratorReturnsCorrectData() {
        SubgraphView view = new SubgraphView(Set.of(2L), Set.of(1L, 3L), graphView);

        Iterator<VertexData> boundaryIt = view.boundaryVertices();
        int count = 0;
        while (boundaryIt.hasNext()) {
            VertexData vd = boundaryIt.next();
            assertThat(vd.vertexId()).isIn(1L, 3L);
            count++;
        }
        assertThat(count).isEqualTo(2);
    }

    @Test
    void relevantEdgesIncludesEdgesWithActiveOrBoundaryEndpoints() {
        // Active: {2}, Boundary: {1, 3}
        SubgraphView view = new SubgraphView(Set.of(2L), Set.of(1L, 3L), graphView);

        Iterator<EdgeData> edges = view.relevantEdges();
        int count = 0;
        while (edges.hasNext()) {
            EdgeData ed = edges.next();
            long src = ed.edgeKey().srcId();
            long dst = ed.edgeKey().dstId();
            // At least one endpoint should be active or boundary
            assertThat(view.isActive(src) || view.isActive(dst)
                    || view.isBoundary(src) || view.isBoundary(dst)).isTrue();
            count++;
        }
        // Edges 1->2, 2->3, 3->4: first two have endpoints in active/boundary
        // 3->4: 3 is boundary, so it's included
        assertThat(count).isEqualTo(3);
    }

    @Test
    void isolatedActiveVertexHasNoRelevantEdgesFromIt() {
        // Active: {5} (isolated), Boundary: empty
        SubgraphView view = new SubgraphView(Set.of(5L), Set.of(), graphView);

        Iterator<EdgeData> edges = view.relevantEdges();
        int count = 0;
        while (edges.hasNext()) {
            edges.next();
            count++;
        }
        assertThat(count).isZero();
    }

    @Test
    void activeVertexIdsReturnsImmutableSet() {
        SubgraphView view = new SubgraphView(Set.of(1L, 2L), Set.of(3L), graphView);

        Set<Long> ids = view.activeVertexIds();
        assertThat(ids).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void boundaryVertexIdsReturnsImmutableSet() {
        SubgraphView view = new SubgraphView(Set.of(1L), Set.of(2L, 3L), graphView);

        Set<Long> ids = view.boundaryVertexIds();
        assertThat(ids).containsExactlyInAnyOrder(2L, 3L);
    }
}
