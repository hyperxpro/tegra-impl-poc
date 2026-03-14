package org.tegra.api;

import org.junit.jupiter.api.Test;
import org.tegra.pds.art.PersistentART;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.KeyCodec;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.version.ByteArray;

import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Snapshot: immutability, graph operators.
 */
class SnapshotTest {

    @Test
    void snapshotExposesGraphView() {
        PersistentART<VertexData> vArt = PersistentART.empty();
        vArt = vArt.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of("name", new PropertyValue.StringProperty("Alice"))));
        vArt = vArt.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of("name", new PropertyValue.StringProperty("Bob"))));

        PersistentART<EdgeData> eArt = PersistentART.empty();
        EdgeKey ek = new EdgeKey(1L, 2L, (short) 0);
        eArt = eArt.insert(KeyCodec.encodeEdgeKey(1L, 2L, (short) 0),
                new EdgeData(ek, Map.of()));

        ByteArray vId = ByteArray.fromString("test_1");
        GraphView view = new GraphView(vArt.root(), eArt.root(), vId);
        Snapshot snapshot = new Snapshot(view, vId);

        assertThat(snapshot.id()).isEqualTo(vId);
        assertThat(snapshot.graph()).isSameAs(view);
        assertThat(snapshot.vertexCount()).isEqualTo(2);
        assertThat(snapshot.edgeCount()).isEqualTo(1);
    }

    @Test
    void snapshotVertexLookup() {
        PersistentART<VertexData> vArt = PersistentART.empty();
        vArt = vArt.insert(KeyCodec.encodeVertexKey(42L),
                new VertexData(42L, Map.of("score", new PropertyValue.DoubleProperty(3.14))));

        ByteArray vId = ByteArray.fromString("test_1");
        GraphView view = new GraphView(vArt.root(), null, vId);
        Snapshot snapshot = new Snapshot(view, vId);

        VertexData v = snapshot.vertex(42L);
        assertThat(v).isNotNull();
        assertThat(v.vertexId()).isEqualTo(42L);
        assertThat(v.properties().get("score")).isEqualTo(new PropertyValue.DoubleProperty(3.14));

        assertThat(snapshot.vertex(99L)).isNull();
    }

    @Test
    void snapshotVerticesIterator() {
        PersistentART<VertexData> vArt = PersistentART.empty();
        for (int i = 1; i <= 5; i++) {
            vArt = vArt.insert(KeyCodec.encodeVertexKey(i),
                    new VertexData(i, Map.of()));
        }

        ByteArray vId = ByteArray.fromString("test_1");
        GraphView view = new GraphView(vArt.root(), null, vId);
        Snapshot snapshot = new Snapshot(view, vId);

        int count = 0;
        Iterator<VertexData> it = snapshot.vertices();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertThat(count).isEqualTo(5);
    }

    @Test
    void snapshotEdgesIterator() {
        PersistentART<EdgeData> eArt = PersistentART.empty();
        for (int i = 1; i <= 3; i++) {
            EdgeKey ek = new EdgeKey((long) i, (long) (i + 1), (short) 0);
            eArt = eArt.insert(KeyCodec.encodeEdgeKey(i, i + 1, (short) 0),
                    new EdgeData(ek, Map.of()));
        }

        ByteArray vId = ByteArray.fromString("test_1");
        GraphView view = new GraphView(null, eArt.root(), vId);
        Snapshot snapshot = new Snapshot(view, vId);

        int count = 0;
        Iterator<EdgeData> it = snapshot.edges();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertThat(count).isEqualTo(3);
    }

    @Test
    void snapshotOutEdges() {
        PersistentART<EdgeData> eArt = PersistentART.empty();
        // Vertex 1 has edges to 2 and 3
        eArt = eArt.insert(KeyCodec.encodeEdgeKey(1L, 2L, (short) 0),
                new EdgeData(new EdgeKey(1L, 2L, (short) 0), Map.of()));
        eArt = eArt.insert(KeyCodec.encodeEdgeKey(1L, 3L, (short) 0),
                new EdgeData(new EdgeKey(1L, 3L, (short) 0), Map.of()));
        // Vertex 2 has edge to 3
        eArt = eArt.insert(KeyCodec.encodeEdgeKey(2L, 3L, (short) 0),
                new EdgeData(new EdgeKey(2L, 3L, (short) 0), Map.of()));

        ByteArray vId = ByteArray.fromString("test_1");
        GraphView view = new GraphView(null, eArt.root(), vId);
        Snapshot snapshot = new Snapshot(view, vId);

        Iterator<EdgeData> outEdges = snapshot.outEdges(1L);
        int count = 0;
        while (outEdges.hasNext()) {
            outEdges.next();
            count++;
        }
        assertThat(count).isEqualTo(2);
    }

    @Test
    void snapshotIsImmutable() {
        PersistentART<VertexData> vArt = PersistentART.empty();
        vArt = vArt.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of()));

        ByteArray vId = ByteArray.fromString("test_1");
        GraphView view = new GraphView(vArt.root(), null, vId);
        Snapshot snapshot = new Snapshot(view, vId);

        // The snapshot's vertex count should not change if we create new art versions
        long countBefore = snapshot.vertexCount();

        // Create a new ART — this should not affect the snapshot
        PersistentART<VertexData> vArt2 = vArt.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of()));

        assertThat(snapshot.vertexCount()).isEqualTo(countBefore);
    }
}
