package org.tegra.algorithms;

import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.Map;

/**
 * Helper to build test graphs for algorithm unit tests.
 * Each method creates a PartitionStore, adds vertices/edges, commits, and returns a GraphView.
 */
public final class TestGraphs {

    private TestGraphs() {}

    /**
     * Linear chain: 0 -> 1 -> 2 -> ... -> (n-1)
     * Directed edges only.
     */
    public static GraphView linearChain(int n) {
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        for (int i = 0; i < n; i++) {
            wv.putVertex(i, new VertexData(i, Map.of()));
        }
        for (int i = 0; i < n - 1; i++) {
            wv.putEdge(i, i + 1, (short) 0,
                    new EdgeData(new EdgeKey(i, i + 1, (short) 0), Map.of()));
        }

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        return store.retrieve(v1);
    }

    /**
     * Star graph: center vertex 0 connected to vertices 1..n with bidirectional edges.
     */
    public static GraphView star(int n) {
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        for (int i = 0; i <= n; i++) {
            wv.putVertex(i, new VertexData(i, Map.of()));
        }
        for (int i = 1; i <= n; i++) {
            wv.putEdge(0, i, (short) 0,
                    new EdgeData(new EdgeKey(0, i, (short) 0), Map.of()));
            wv.putEdge(i, 0, (short) 0,
                    new EdgeData(new EdgeKey(i, 0, (short) 0), Map.of()));
        }

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        return store.retrieve(v1);
    }

    /**
     * Two disjoint triangles: {0,1,2} and {3,4,5} with bidirectional edges.
     */
    public static GraphView twoTriangles() {
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        for (int i = 0; i < 6; i++) {
            wv.putVertex(i, new VertexData(i, Map.of()));
        }

        // Triangle 1: 0-1-2
        addBidirectionalEdge(wv, 0, 1);
        addBidirectionalEdge(wv, 1, 2);
        addBidirectionalEdge(wv, 0, 2);

        // Triangle 2: 3-4-5
        addBidirectionalEdge(wv, 3, 4);
        addBidirectionalEdge(wv, 4, 5);
        addBidirectionalEdge(wv, 3, 5);

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        return store.retrieve(v1);
    }

    /**
     * Complete graph K_n with bidirectional edges.
     */
    public static GraphView complete(int n) {
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        for (int i = 0; i < n; i++) {
            wv.putVertex(i, new VertexData(i, Map.of()));
        }
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                addBidirectionalEdge(wv, i, j);
            }
        }

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        return store.retrieve(v1);
    }

    /**
     * Cycle: 0 -> 1 -> 2 -> ... -> (n-1) -> 0 with bidirectional edges.
     */
    public static GraphView cycle(int n) {
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        for (int i = 0; i < n; i++) {
            wv.putVertex(i, new VertexData(i, Map.of()));
        }
        for (int i = 0; i < n; i++) {
            int next = (i + 1) % n;
            addBidirectionalEdge(wv, i, next);
        }

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        return store.retrieve(v1);
    }

    /**
     * Bipartite graph: m user vertices (IDs 0..m-1) connected to n item vertices (IDs m..m+n-1).
     * Each user is connected to all items with edges carrying a "rating" property.
     * Ratings are assigned as (userId + itemId + 1.0) for deterministic test values.
     */
    public static GraphView bipartite(int m, int n) {
        PartitionStore store = new PartitionStore();
        ByteArray base = ByteArray.fromString("base");
        store.createInitialVersion(base);
        WorkingVersion wv = store.branch(base);

        // User vertices 0..m-1
        for (int i = 0; i < m; i++) {
            wv.putVertex(i, new VertexData(i, Map.of()));
        }
        // Item vertices m..m+n-1
        for (int j = 0; j < n; j++) {
            wv.putVertex(m + j, new VertexData(m + j, Map.of()));
        }

        // Edges from users to items with ratings
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                double rating = (i + j + 1.0);
                Map<String, PropertyValue> props = Map.of(
                        "rating", new PropertyValue.DoubleProperty(rating)
                );
                long itemId = m + j;
                wv.putEdge(i, itemId, (short) 0,
                        new EdgeData(new EdgeKey(i, itemId, (short) 0), props));
                wv.putEdge(itemId, i, (short) 0,
                        new EdgeData(new EdgeKey(itemId, i, (short) 0), props));
            }
        }

        ByteArray v1 = ByteArray.fromString("v1");
        store.commit(wv, v1);
        return store.retrieve(v1);
    }

    private static void addBidirectionalEdge(WorkingVersion wv, long src, long dst) {
        wv.putEdge(src, dst, (short) 0,
                new EdgeData(new EdgeKey(src, dst, (short) 0), Map.of()));
        wv.putEdge(dst, src, (short) 0,
                new EdgeData(new EdgeKey(dst, src, (short) 0), Map.of()));
    }
}
