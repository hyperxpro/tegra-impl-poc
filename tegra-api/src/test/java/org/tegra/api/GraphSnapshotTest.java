package org.tegra.api;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GraphSnapshotTest {

    private Timelapse<String, Double> createTimelapse() {
        return Timelapse.create("test-graph");
    }

    @Test
    void testEmptySnapshot() {
        var tl = createTimelapse();
        GraphSnapshot<String, Double> snap = tl.emptySnapshot();

        assertThat(snap.vertexCount()).isEqualTo(0);
        assertThat(snap.edgeCount()).isEqualTo(0);
        assertThat(snap.vertices().count()).isEqualTo(0);
        assertThat(snap.edges().count()).isEqualTo(0);
    }

    @Test
    void testAddVertexAndRetrieve() {
        var tl = createTimelapse();
        GraphSnapshot<String, Double> empty = tl.emptySnapshot();

        MutableGraphView<String, Double> mut = empty.asMutable();
        mut.addVertex(1L, "Alice");
        mut.addVertex(2L, "Bob");

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("v1"));

        assertThat(snap.vertexCount()).isEqualTo(2);
        assertThat(snap.vertex(1L)).isPresent();
        assertThat(snap.vertex(1L).get().properties()).isEqualTo("Alice");
        assertThat(snap.vertex(2L)).isPresent();
        assertThat(snap.vertex(2L).get().properties()).isEqualTo("Bob");
        assertThat(snap.vertex(99L)).isEmpty();
    }

    @Test
    void testAddEdgeAndRetrieve() {
        var tl = createTimelapse();
        GraphSnapshot<String, Double> empty = tl.emptySnapshot();

        MutableGraphView<String, Double> mut = empty.asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        mut.addEdge(1L, 2L, 1.5);

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("v1"));

        assertThat(snap.edgeCount()).isEqualTo(1);
        List<Edge<Double>> outEdges = snap.outEdges(1L).toList();
        assertThat(outEdges).hasSize(1);
        assertThat(outEdges.get(0).src()).isEqualTo(1L);
        assertThat(outEdges.get(0).dst()).isEqualTo(2L);
        assertThat(outEdges.get(0).properties()).isEqualTo(1.5);
    }

    @Test
    void testVertexStream() {
        var tl = createTimelapse();
        MutableGraphView<String, Double> mut = tl.emptySnapshot().asMutable();
        mut.addVertex(10L, "X");
        mut.addVertex(20L, "Y");
        mut.addVertex(30L, "Z");

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("v1"));

        List<Vertex<String>> vertices = snap.vertices().toList();
        assertThat(vertices).hasSize(3);
        assertThat(vertices).extracting(Vertex::id).containsExactlyInAnyOrder(10L, 20L, 30L);
    }

    @Test
    void testEdgeStream() {
        var tl = createTimelapse();
        MutableGraphView<String, Double> mut = tl.emptySnapshot().asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        mut.addVertex(3L, "C");
        mut.addEdge(1L, 2L, 1.0);
        mut.addEdge(2L, 3L, 2.0);

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("v1"));

        List<Edge<Double>> allEdges = snap.edges().toList();
        assertThat(allEdges).hasSize(2);
    }

    @Test
    void testOutEdges() {
        var tl = createTimelapse();
        MutableGraphView<String, Double> mut = tl.emptySnapshot().asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        mut.addVertex(3L, "C");
        mut.addEdge(1L, 2L, 1.0);
        mut.addEdge(1L, 3L, 2.0);
        mut.addEdge(2L, 3L, 3.0);

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("v1"));

        assertThat(snap.outEdges(1L).toList()).hasSize(2);
        assertThat(snap.outEdges(2L).toList()).hasSize(1);
        assertThat(snap.outEdges(3L).toList()).isEmpty();
        assertThat(snap.outEdges(99L).toList()).isEmpty();
    }

    @Test
    void testInEdges() {
        var tl = createTimelapse();
        MutableGraphView<String, Double> mut = tl.emptySnapshot().asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        mut.addVertex(3L, "C");
        mut.addEdge(1L, 2L, 1.0);
        mut.addEdge(1L, 3L, 2.0);
        mut.addEdge(2L, 3L, 3.0);

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("v1"));

        assertThat(snap.inEdges(1L).toList()).isEmpty();
        assertThat(snap.inEdges(2L).toList()).hasSize(1);
        assertThat(snap.inEdges(3L).toList()).hasSize(2);
    }

    @Test
    void testImmutability() {
        var tl = createTimelapse();
        MutableGraphView<String, Double> mut = tl.emptySnapshot().asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        mut.addEdge(1L, 2L, 1.0);

        GraphSnapshot<String, Double> original = mut.toSnapshot(SnapshotId.of("v1"));

        // Create a mutable copy and modify it
        MutableGraphView<String, Double> branch = original.asMutable();
        branch.addVertex(3L, "C");
        branch.addEdge(2L, 3L, 2.0);
        branch.removeVertex(1L);

        // Original snapshot should be unchanged
        assertThat(original.vertexCount()).isEqualTo(2);
        assertThat(original.edgeCount()).isEqualTo(1);
        assertThat(original.vertex(1L)).isPresent();
        assertThat(original.vertex(3L)).isEmpty();
    }
}
