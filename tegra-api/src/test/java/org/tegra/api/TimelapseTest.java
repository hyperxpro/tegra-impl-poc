package org.tegra.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for save/retrieve/diff/expand/merge.
 */
class TimelapseTest {

    private PartitionStore store;
    private Timelapse timelapse;

    @BeforeEach
    void setUp() {
        store = new PartitionStore();
        timelapse = new Timelapse(store, "test_graph");

        // Create initial empty version
        ByteArray v0 = ByteArray.fromString("test_graph_0");
        store.createInitialVersion(v0);
    }

    @Test
    void saveAndRetrieve() {
        // Branch, add data, save
        ByteArray v0 = ByteArray.fromString("test_graph_0");
        WorkingVersion working = store.branch(v0);
        working.putVertex(1L, new VertexData(1L, Map.of("name", new PropertyValue.StringProperty("Alice"))));
        working.putVertex(2L, new VertexData(2L, Map.of("name", new PropertyValue.StringProperty("Bob"))));

        EdgeKey ek = new EdgeKey(1L, 2L, (short) 0);
        working.putEdge(1L, 2L, (short) 0, new EdgeData(ek, Map.of("type", new PropertyValue.StringProperty("KNOWS"))));

        timelapse.setCurrentWorking(working, v0);
        ByteArray v1 = ByteArray.fromString("test_graph_1");
        timelapse.save(v1);

        // Retrieve
        Snapshot snapshot = timelapse.retrieve(v1);
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.id()).isEqualTo(v1);
        assertThat(snapshot.vertexCount()).isEqualTo(2);
        assertThat(snapshot.edgeCount()).isEqualTo(1);

        VertexData alice = snapshot.vertex(1L);
        assertThat(alice).isNotNull();
        assertThat(alice.properties().get("name")).isEqualTo(new PropertyValue.StringProperty("Alice"));
    }

    @Test
    void diffDetectsAddedVertices() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        // Create snapshot with vertex 1
        WorkingVersion w1 = store.branch(v0);
        w1.putVertex(1L, new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(1))));
        ByteArray id1 = ByteArray.fromString("test_graph_1");
        store.commit(w1, id1);

        // Create snapshot with vertices 1 and 2
        WorkingVersion w2 = store.branch(id1);
        w2.putVertex(2L, new VertexData(2L, Map.of("v", new PropertyValue.LongProperty(2))));
        ByteArray id2 = ByteArray.fromString("test_graph_2");
        store.commit(w2, id2);

        Snapshot s1 = timelapse.retrieve(id1);
        Snapshot s2 = timelapse.retrieve(id2);

        Delta delta = timelapse.diff(s1, s2);

        assertThat(delta.addedVertices()).containsExactly(2L);
        assertThat(delta.removedVertices()).isEmpty();
        assertThat(delta.modifiedVertices()).isEmpty();
    }

    @Test
    void diffDetectsRemovedVertices() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        // Create snapshot with vertices 1 and 2
        WorkingVersion w1 = store.branch(v0);
        w1.putVertex(1L, new VertexData(1L, Map.of()));
        w1.putVertex(2L, new VertexData(2L, Map.of()));
        ByteArray id1 = ByteArray.fromString("test_graph_1");
        store.commit(w1, id1);

        // Create snapshot with only vertex 1
        WorkingVersion w2 = store.branch(id1);
        w2.removeVertex(2L);
        ByteArray id2 = ByteArray.fromString("test_graph_2");
        store.commit(w2, id2);

        Snapshot s1 = timelapse.retrieve(id1);
        Snapshot s2 = timelapse.retrieve(id2);

        Delta delta = timelapse.diff(s1, s2);

        assertThat(delta.removedVertices()).containsExactly(2L);
        assertThat(delta.addedVertices()).isEmpty();
    }

    @Test
    void diffDetectsModifiedVertices() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        WorkingVersion w1 = store.branch(v0);
        w1.putVertex(1L, new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(10))));
        ByteArray id1 = ByteArray.fromString("test_graph_1");
        store.commit(w1, id1);

        WorkingVersion w2 = store.branch(id1);
        w2.putVertex(1L, new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(20))));
        ByteArray id2 = ByteArray.fromString("test_graph_2");
        store.commit(w2, id2);

        Snapshot s1 = timelapse.retrieve(id1);
        Snapshot s2 = timelapse.retrieve(id2);

        Delta delta = timelapse.diff(s1, s2);

        assertThat(delta.modifiedVertices()).containsExactly(1L);
    }

    @Test
    void diffDetectsEdgeChanges() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        WorkingVersion w1 = store.branch(v0);
        w1.putVertex(1L, new VertexData(1L, Map.of()));
        w1.putVertex(2L, new VertexData(2L, Map.of()));
        EdgeKey ek12 = new EdgeKey(1L, 2L, (short) 0);
        w1.putEdge(1L, 2L, (short) 0, new EdgeData(ek12, Map.of()));
        ByteArray id1 = ByteArray.fromString("test_graph_1");
        store.commit(w1, id1);

        WorkingVersion w2 = store.branch(id1);
        w2.putVertex(3L, new VertexData(3L, Map.of()));
        EdgeKey ek23 = new EdgeKey(2L, 3L, (short) 0);
        w2.putEdge(2L, 3L, (short) 0, new EdgeData(ek23, Map.of()));
        ByteArray id2 = ByteArray.fromString("test_graph_2");
        store.commit(w2, id2);

        Snapshot s1 = timelapse.retrieve(id1);
        Snapshot s2 = timelapse.retrieve(id2);

        Delta delta = timelapse.diff(s1, s2);

        assertThat(delta.addedEdges()).containsExactly(ek23);
    }

    @Test
    void expandIncludesNeighbors() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        WorkingVersion w = store.branch(v0);
        w.putVertex(1L, new VertexData(1L, Map.of()));
        w.putVertex(2L, new VertexData(2L, Map.of()));
        w.putVertex(3L, new VertexData(3L, Map.of()));
        w.putVertex(4L, new VertexData(4L, Map.of()));

        w.putEdge(1L, 2L, (short) 0, new EdgeData(new EdgeKey(1L, 2L, (short) 0), Map.of()));
        w.putEdge(2L, 3L, (short) 0, new EdgeData(new EdgeKey(2L, 3L, (short) 0), Map.of()));
        w.putEdge(3L, 4L, (short) 0, new EdgeData(new EdgeKey(3L, 4L, (short) 0), Map.of()));

        ByteArray id = ByteArray.fromString("test_graph_1");
        store.commit(w, id);

        Snapshot snapshot = timelapse.retrieve(id);

        // Expand from vertex 2 — should include neighbors 1 and 3
        SubgraphView subgraph = timelapse.expand(Set.of(2L), snapshot);

        assertThat(subgraph.isActive(2L)).isTrue();
        assertThat(subgraph.isBoundary(3L)).isTrue(); // outgoing neighbor
        // vertex 1 won't be boundary via outEdges from vertex 2 (only 2->3 exists)
        // but vertex 1 would be boundary via inEdges to vertex 2 (1->2 exists)
        assertThat(subgraph.isBoundary(1L)).isTrue();
        assertThat(subgraph.isActive(4L)).isFalse();
        assertThat(subgraph.isBoundary(4L)).isFalse();
    }

    @Test
    void mergeUnionsTwoSnapshots() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        // Snapshot A: vertices 1, 2
        WorkingVersion wA = store.branch(v0);
        wA.putVertex(1L, new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(10))));
        wA.putVertex(2L, new VertexData(2L, Map.of("v", new PropertyValue.LongProperty(20))));
        ByteArray idA = ByteArray.fromString("test_graph_A");
        store.commit(wA, idA);

        // Snapshot B: vertices 2, 3 (vertex 2 has different value)
        WorkingVersion wB = store.branch(v0);
        wB.putVertex(2L, new VertexData(2L, Map.of("v", new PropertyValue.LongProperty(200))));
        wB.putVertex(3L, new VertexData(3L, Map.of("v", new PropertyValue.LongProperty(30))));
        ByteArray idB = ByteArray.fromString("test_graph_B");
        store.commit(wB, idB);

        Snapshot snA = timelapse.retrieve(idA);
        Snapshot snB = timelapse.retrieve(idB);

        // Merge: for common vertex 2, take the value from B
        Snapshot merged = timelapse.merge(snA, snB, (a, b) -> b);

        assertThat(merged.vertexCount()).isEqualTo(3);
        assertThat(merged.vertex(1L)).isNotNull();
        assertThat(merged.vertex(2L)).isNotNull();
        assertThat(merged.vertex(3L)).isNotNull();

        // Vertex 2 should have value from B (200)
        VertexData v2 = merged.vertex(2L);
        assertThat(v2.properties().get("v")).isEqualTo(new PropertyValue.LongProperty(200));
    }

    @Test
    void retrieveByPrefix() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        // Create several versions
        WorkingVersion w1 = store.branch(v0);
        w1.putVertex(1L, new VertexData(1L, Map.of()));
        store.commit(w1, ByteArray.fromString("test_graph_100"));

        WorkingVersion w2 = store.branch(v0);
        w2.putVertex(2L, new VertexData(2L, Map.of()));
        store.commit(w2, ByteArray.fromString("test_graph_200"));

        var snapshots = timelapse.retrieveByPrefix(ByteArray.fromString("test_graph_"));
        // Should include v0, and the two committed versions: test_graph_0, test_graph_100, test_graph_200
        assertThat(snapshots.size()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void diffOnIdenticalSnapshotsIsEmpty() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");
        WorkingVersion w = store.branch(v0);
        w.putVertex(1L, new VertexData(1L, Map.of()));
        ByteArray id = ByteArray.fromString("test_graph_1");
        store.commit(w, id);

        Snapshot s = timelapse.retrieve(id);
        Delta delta = timelapse.diff(s, s);

        assertThat(delta.isEmpty()).isTrue();
    }

    @Test
    void deltaAffectedVerticesIncludesEdgeEndpoints() {
        ByteArray v0 = ByteArray.fromString("test_graph_0");

        WorkingVersion w1 = store.branch(v0);
        w1.putVertex(1L, new VertexData(1L, Map.of()));
        w1.putVertex(2L, new VertexData(2L, Map.of()));
        ByteArray id1 = ByteArray.fromString("test_graph_1");
        store.commit(w1, id1);

        WorkingVersion w2 = store.branch(id1);
        EdgeKey ek = new EdgeKey(1L, 2L, (short) 0);
        w2.putEdge(1L, 2L, (short) 0, new EdgeData(ek, Map.of()));
        ByteArray id2 = ByteArray.fromString("test_graph_2");
        store.commit(w2, id2);

        Snapshot s1 = timelapse.retrieve(id1);
        Snapshot s2 = timelapse.retrieve(id2);

        Delta delta = timelapse.diff(s1, s2);

        Set<Long> affected = delta.affectedVertices();
        assertThat(affected).contains(1L, 2L);
    }
}
