package org.tegra.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimelapseTest {

    private Timelapse<String, Double> timelapse;

    @BeforeEach
    void setUp() {
        timelapse = Timelapse.create("test-graph");
    }

    @Test
    void testCreateAndSave() {
        GraphSnapshot<String, Double> empty = timelapse.emptySnapshot();
        MutableGraphView<String, Double> mut = empty.asMutable();
        mut.addVertex(1L, "A");

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("v1"));
        SnapshotId savedId = timelapse.save(snap, "snapshot-1");

        assertThat(timelapse.snapshotCount()).isEqualTo(1);
        assertThat(timelapse.hasSnapshot(savedId)).isTrue();
    }

    @Test
    void testRetrieve() {
        MutableGraphView<String, Double> mut = timelapse.emptySnapshot().asMutable();
        mut.addVertex(1L, "Alice");
        mut.addVertex(2L, "Bob");
        mut.addEdge(1L, 2L, 1.0);

        GraphSnapshot<String, Double> snap = mut.toSnapshot(SnapshotId.of("temp"));
        SnapshotId id = timelapse.save(snap, "snap-1");

        GraphSnapshot<String, Double> retrieved = timelapse.retrieve(id);
        assertThat(retrieved.vertexCount()).isEqualTo(2);
        assertThat(retrieved.edgeCount()).isEqualTo(1);
        assertThat(retrieved.id()).isEqualTo(id);
    }

    @Test
    void testRetrieveNonExistent() {
        assertThatThrownBy(() -> timelapse.retrieve(SnapshotId.of("nonexistent")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRetrieveByPrefix() {
        MutableGraphView<String, Double> mut1 = timelapse.emptySnapshot().asMutable();
        mut1.addVertex(1L, "A");
        timelapse.save(mut1.toSnapshot(SnapshotId.of("temp")), "graph_v1");

        MutableGraphView<String, Double> mut2 = timelapse.emptySnapshot().asMutable();
        mut2.addVertex(2L, "B");
        timelapse.save(mut2.toSnapshot(SnapshotId.of("temp")), "graph_v2");

        MutableGraphView<String, Double> mut3 = timelapse.emptySnapshot().asMutable();
        mut3.addVertex(3L, "C");
        timelapse.save(mut3.toSnapshot(SnapshotId.of("temp")), "other_v1");

        List<GraphSnapshot<String, Double>> graphSnapshots = timelapse.retrieveByPrefix("graph");
        assertThat(graphSnapshots).hasSize(2);

        List<GraphSnapshot<String, Double>> otherSnapshots = timelapse.retrieveByPrefix("other");
        assertThat(otherSnapshots).hasSize(1);
    }

    @Test
    void testBranch() {
        MutableGraphView<String, Double> mut = timelapse.emptySnapshot().asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        mut.addEdge(1L, 2L, 1.0);

        SnapshotId id = timelapse.save(mut.toSnapshot(SnapshotId.of("temp")), "base");

        MutableGraphView<String, Double> branch = timelapse.branch(id);
        branch.addVertex(3L, "C");
        branch.addEdge(2L, 3L, 2.0);

        // Branch should have the new vertex
        assertThat(branch.vertexCount()).isEqualTo(3);
        assertThat(branch.edgeCount()).isEqualTo(2);

        // Original snapshot should be unchanged
        GraphSnapshot<String, Double> original = timelapse.retrieve(id);
        assertThat(original.vertexCount()).isEqualTo(2);
        assertThat(original.edgeCount()).isEqualTo(1);
    }

    @Test
    void testDiff() {
        // Create first snapshot
        MutableGraphView<String, Double> mut1 = timelapse.emptySnapshot().asMutable();
        mut1.addVertex(1L, "A");
        mut1.addVertex(2L, "B");
        mut1.addEdge(1L, 2L, 1.0);
        SnapshotId id1 = timelapse.save(mut1.toSnapshot(SnapshotId.of("temp")), "v1");

        // Create second snapshot with changes
        MutableGraphView<String, Double> mut2 = timelapse.branch(id1);
        mut2.addVertex(3L, "C");
        mut2.setVertexProperty(2L, "B-modified");
        mut2.addEdge(2L, 3L, 2.0);
        SnapshotId id2 = timelapse.save(mut2.toSnapshot(SnapshotId.of("temp")), "v2");

        GraphDelta<String, Double> delta = timelapse.diff(id1, id2);

        // Vertex changes: vertex 3 added, vertex 2 modified
        assertThat(delta.vertexChanges()).hasSize(2);
        assertThat(delta.changedVertexIds()).containsExactlyInAnyOrder(2L, 3L);

        // Edge changes: edge 2->3 added
        assertThat(delta.edgeChanges()).hasSize(1);

        // Affected vertex IDs should include both vertex and edge changes
        assertThat(delta.affectedVertexIds()).contains(2L, 3L);
    }

    @Test
    void testRunAlgorithm() {
        MutableGraphView<String, Double> mut = timelapse.emptySnapshot().asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        mut.addVertex(3L, "C");
        SnapshotId id = timelapse.save(mut.toSnapshot(SnapshotId.of("temp")), "snap");

        // Simple algorithm: count vertices
        long count = timelapse.run(id, snapshot -> snapshot.vertexCount());
        assertThat(count).isEqualTo(3);
    }

    @Test
    void testSnapshotImmutability() {
        MutableGraphView<String, Double> mut = timelapse.emptySnapshot().asMutable();
        mut.addVertex(1L, "A");
        mut.addVertex(2L, "B");
        SnapshotId id = timelapse.save(mut.toSnapshot(SnapshotId.of("temp")), "immutable");

        GraphSnapshot<String, Double> snap = timelapse.retrieve(id);
        long originalCount = snap.vertexCount();

        // Mutating a branch should not affect the stored snapshot
        MutableGraphView<String, Double> branch = snap.asMutable();
        branch.addVertex(3L, "C");
        branch.addVertex(4L, "D");

        // Re-retrieve and verify unchanged
        GraphSnapshot<String, Double> snapAgain = timelapse.retrieve(id);
        assertThat(snapAgain.vertexCount()).isEqualTo(originalCount);
    }
}
