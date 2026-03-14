package org.tegra.store;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for PartitionStore: branch/commit/retrieve lifecycle, multiple versions, structural sharing.
 */
class PartitionStoreTest {

    private PartitionStore store;

    @BeforeEach
    void setUp() {
        store = new PartitionStore();
    }

    @Test
    void branchCommitRetrieveLifecycle() {
        // Create an initial empty version
        ByteArray v0 = ByteArray.fromString("graph_0");
        store.createInitialVersion(v0);

        // Branch from the initial version
        WorkingVersion working = store.branch(v0);
        assertThat(working).isNotNull();

        // Add a vertex
        VertexData vertex = new VertexData(1L, Map.of("name", new PropertyValue.StringProperty("Alice")));
        working.putVertex(1L, vertex);

        // Add an edge
        EdgeKey edgeKey = new EdgeKey(1L, 2L, (short) 0);
        EdgeData edge = new EdgeData(edgeKey, Map.of("weight", new PropertyValue.DoubleProperty(1.5)));
        working.putEdge(1L, 2L, (short) 0, edge);

        // Commit
        ByteArray v1 = ByteArray.fromString("graph_1");
        store.commit(working, v1);

        // Retrieve and verify
        GraphView view = store.retrieve(v1);
        assertThat(view).isNotNull();

        VertexData retrieved = view.vertex(1L);
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.vertexId()).isEqualTo(1L);
        assertThat(retrieved.properties().get("name"))
                .isEqualTo(new PropertyValue.StringProperty("Alice"));

        EdgeData retrievedEdge = view.edge(1L, 2L, (short) 0);
        assertThat(retrievedEdge).isNotNull();
        assertThat(retrievedEdge.edgeKey()).isEqualTo(edgeKey);
    }

    @Test
    void multipleVersionsAreIsolated() {
        ByteArray v0 = ByteArray.fromString("graph_0");
        store.createInitialVersion(v0);

        // Create version 1 with vertex 1
        WorkingVersion w1 = store.branch(v0);
        w1.putVertex(1L, new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(1))));
        ByteArray v1 = ByteArray.fromString("graph_1");
        store.commit(w1, v1);

        // Create version 2 from version 1, add vertex 2
        WorkingVersion w2 = store.branch(v1);
        w2.putVertex(2L, new VertexData(2L, Map.of("v", new PropertyValue.LongProperty(2))));
        ByteArray v2 = ByteArray.fromString("graph_2");
        store.commit(w2, v2);

        // Version 1 should only have vertex 1
        GraphView view1 = store.retrieve(v1);
        assertThat(view1.vertex(1L)).isNotNull();
        assertThat(view1.vertex(2L)).isNull();
        assertThat(view1.vertexCount()).isEqualTo(1);

        // Version 2 should have both vertices
        GraphView view2 = store.retrieve(v2);
        assertThat(view2.vertex(1L)).isNotNull();
        assertThat(view2.vertex(2L)).isNotNull();
        assertThat(view2.vertexCount()).isEqualTo(2);
    }

    @Test
    void structuralSharingBetweenVersions() {
        ByteArray v0 = ByteArray.fromString("graph_0");
        store.createInitialVersion(v0);

        // Create version 1 with many vertices
        WorkingVersion w1 = store.branch(v0);
        for (int i = 0; i < 100; i++) {
            w1.putVertex(i, new VertexData(i, Map.of("v", new PropertyValue.LongProperty(i))));
        }
        ByteArray v1 = ByteArray.fromString("graph_1");
        store.commit(w1, v1);

        // Create version 2 from version 1, only change one vertex
        WorkingVersion w2 = store.branch(v1);
        w2.putVertex(50L, new VertexData(50L, Map.of("v", new PropertyValue.LongProperty(999))));
        ByteArray v2 = ByteArray.fromString("graph_2");
        store.commit(w2, v2);

        // Both versions should work correctly
        GraphView view1 = store.retrieve(v1);
        GraphView view2 = store.retrieve(v2);

        assertThat(view1.vertexCount()).isEqualTo(100);
        assertThat(view2.vertexCount()).isEqualTo(100);

        // Version 1 should have old value for vertex 50
        VertexData v1_50 = view1.vertex(50L);
        assertThat(v1_50.properties().get("v")).isEqualTo(new PropertyValue.LongProperty(50));

        // Version 2 should have new value for vertex 50
        VertexData v2_50 = view2.vertex(50L);
        assertThat(v2_50.properties().get("v")).isEqualTo(new PropertyValue.LongProperty(999));

        // Structural sharing: the vertex roots should be different objects
        // but unchanged subtrees are shared
        assertThat(view1.vertexRoot()).isNotSameAs(view2.vertexRoot());
    }

    @Test
    void branchNonExistentVersionThrows() {
        assertThatThrownBy(() -> store.branch(ByteArray.fromString("nonexistent")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retrieveNonExistentVersionThrows() {
        assertThatThrownBy(() -> store.retrieve(ByteArray.fromString("nonexistent")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void evictRemovesVersion() {
        ByteArray v0 = ByteArray.fromString("graph_0");
        store.createInitialVersion(v0);

        store.evict(v0);

        assertThatThrownBy(() -> store.retrieve(v0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void workingVersionOperations() {
        ByteArray v0 = ByteArray.fromString("graph_0");
        store.createInitialVersion(v0);

        WorkingVersion working = store.branch(v0);

        // Put vertex
        working.putVertex(1L, new VertexData(1L, Map.of("name", new PropertyValue.StringProperty("Node1"))));
        assertThat(working.getVertex(1L)).isNotNull();

        // Put edge
        EdgeKey ek = new EdgeKey(1L, 2L, (short) 0);
        working.putEdge(1L, 2L, (short) 0, new EdgeData(ek, Map.of()));
        assertThat(working.getEdge(1L, 2L, (short) 0)).isNotNull();

        // Out edges
        Iterator<EdgeData> outEdges = working.outEdges(1L);
        assertThat(outEdges.hasNext()).isTrue();
        assertThat(outEdges.next().edgeKey()).isEqualTo(ek);

        // Remove vertex
        working.removeVertex(1L);
        assertThat(working.getVertex(1L)).isNull();

        // Remove edge
        working.removeEdge(1L, 2L, (short) 0);
        assertThat(working.getEdge(1L, 2L, (short) 0)).isNull();
    }

    @Test
    void graphViewVerticesAndEdgesIterators() {
        ByteArray v0 = ByteArray.fromString("graph_0");
        store.createInitialVersion(v0);

        WorkingVersion w = store.branch(v0);
        w.putVertex(1L, new VertexData(1L, Map.of()));
        w.putVertex(2L, new VertexData(2L, Map.of()));
        w.putVertex(3L, new VertexData(3L, Map.of()));

        EdgeKey e12 = new EdgeKey(1L, 2L, (short) 0);
        EdgeKey e23 = new EdgeKey(2L, 3L, (short) 0);
        w.putEdge(1L, 2L, (short) 0, new EdgeData(e12, Map.of()));
        w.putEdge(2L, 3L, (short) 0, new EdgeData(e23, Map.of()));

        ByteArray v1 = ByteArray.fromString("graph_1");
        store.commit(w, v1);

        GraphView view = store.retrieve(v1);
        assertThat(view.vertexCount()).isEqualTo(3);
        assertThat(view.edgeCount()).isEqualTo(2);

        // Verify out edges
        Iterator<EdgeData> outEdges = view.outEdges(1L);
        assertThat(outEdges.hasNext()).isTrue();
        EdgeData outEdge = outEdges.next();
        assertThat(outEdge.edgeKey().dstId()).isEqualTo(2L);

        // Verify in edges
        Iterator<EdgeData> inEdges = view.inEdges(3L);
        assertThat(inEdges.hasNext()).isTrue();
        EdgeData inEdge = inEdges.next();
        assertThat(inEdge.edgeKey().srcId()).isEqualTo(2L);
    }
}
