package org.tegra.examples;

import org.tegra.api.Delta;
import org.tegra.api.Snapshot;
import org.tegra.api.Timelapse;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.Iterator;
import java.util.Map;

/**
 * Demonstrates the basic Timelapse API: create a graph, save snapshots,
 * retrieve them, and compute diffs between versions.
 */
public final class BasicTimelapseExample {

    public static void main(String[] args) {
        System.out.println("=== Basic Timelapse Example ===");
        System.out.println();

        // 1. Create timelapse
        PartitionStore store = new PartitionStore();
        Timelapse timelapse = new Timelapse(store, "social-network");

        // Bootstrap with an initial empty version
        ByteArray initialVersion = ByteArray.fromString("v0");
        store.createInitialVersion(initialVersion);

        // 2. Build initial social network graph (Alice, Bob, Charlie)
        WorkingVersion wv = store.branch(initialVersion);
        timelapse.setCurrentWorking(wv, initialVersion);

        wv.putVertex(1, new VertexData(1, Map.of(
                "name", new PropertyValue.StringProperty("Alice"),
                "age", new PropertyValue.LongProperty(30))));
        wv.putVertex(2, new VertexData(2, Map.of(
                "name", new PropertyValue.StringProperty("Bob"),
                "age", new PropertyValue.LongProperty(25))));
        wv.putVertex(3, new VertexData(3, Map.of(
                "name", new PropertyValue.StringProperty("Charlie"),
                "age", new PropertyValue.LongProperty(35))));

        // Alice -> Bob (friends)
        wv.putEdge(1, 2, (short) 0, new EdgeData(
                new EdgeKey(1, 2, (short) 0),
                Map.of("relation", new PropertyValue.StringProperty("friends"))));
        // Bob -> Charlie (colleagues)
        wv.putEdge(2, 3, (short) 0, new EdgeData(
                new EdgeKey(2, 3, (short) 0),
                Map.of("relation", new PropertyValue.StringProperty("colleagues"))));

        // 3. Save snapshot 1
        ByteArray snap1Id = ByteArray.fromString("snapshot_1");
        timelapse.save(snap1Id);
        System.out.println("Saved snapshot 1: " + snap1Id);

        // 4. Add new vertices and edges
        WorkingVersion wv2 = store.branch(snap1Id);
        timelapse.setCurrentWorking(wv2, snap1Id);

        // Add Diana
        wv2.putVertex(4, new VertexData(4, Map.of(
                "name", new PropertyValue.StringProperty("Diana"),
                "age", new PropertyValue.LongProperty(28))));
        // Alice -> Diana (friends)
        wv2.putEdge(1, 4, (short) 0, new EdgeData(
                new EdgeKey(1, 4, (short) 0),
                Map.of("relation", new PropertyValue.StringProperty("friends"))));
        // Charlie -> Diana (mentors)
        wv2.putEdge(3, 4, (short) 0, new EdgeData(
                new EdgeKey(3, 4, (short) 0),
                Map.of("relation", new PropertyValue.StringProperty("mentors"))));

        // 5. Save snapshot 2
        ByteArray snap2Id = ByteArray.fromString("snapshot_2");
        timelapse.save(snap2Id);
        System.out.println("Saved snapshot 2: " + snap2Id);

        // 6. Retrieve and display snapshots
        Snapshot s1 = timelapse.retrieve(snap1Id);
        Snapshot s2 = timelapse.retrieve(snap2Id);

        System.out.println();
        System.out.println("Snapshot 1: " + s1.vertexCount() + " vertices, " + s1.edgeCount() + " edges");
        System.out.println("Snapshot 2: " + s2.vertexCount() + " vertices, " + s2.edgeCount() + " edges");

        // Print vertices in snapshot 2
        System.out.println();
        System.out.println("Vertices in snapshot 2:");
        Iterator<VertexData> vertices = s2.vertices();
        while (vertices.hasNext()) {
            VertexData vd = vertices.next();
            PropertyValue nameProp = vd.properties().get("name");
            String name = (nameProp instanceof PropertyValue.StringProperty sp) ? sp.value() : "unknown";
            System.out.println("  Vertex " + vd.vertexId() + ": " + name);
        }

        // 7. Diff snapshots
        Delta delta = timelapse.diff(s1, s2);
        System.out.println();
        System.out.println("Diff between snapshot 1 and snapshot 2:");
        System.out.println("  Added vertices:    " + delta.addedVertices());
        System.out.println("  Removed vertices:  " + delta.removedVertices());
        System.out.println("  Modified vertices: " + delta.modifiedVertices());
        System.out.println("  Added edges:       " + delta.addedEdges().size());
        System.out.println("  Removed edges:     " + delta.removedEdges().size());
        System.out.println("  Affected vertices: " + delta.affectedVertices());
        System.out.println();
        System.out.println("Done.");
    }
}
