package org.tegra.api;

import org.tegra.pds.hamt.PersistentHAMT;

/**
 * Test utility for constructing {@link GraphSnapshot} instances.
 * <p>
 * Uses the public {@link GraphSnapshot#create} factory and
 * {@link MutableGraphView} to build test graphs.
 */
public final class TestGraphs {

    private TestGraphs() {
        // utility class
    }

    /**
     * Creates an empty mutable graph view via an empty snapshot.
     */
    private static <V, E> MutableGraphView<V, E> emptyMutable() {
        GraphSnapshot<V, E> empty = GraphSnapshot.create(
                PersistentHAMT.empty(),
                PersistentHAMT.empty(),
                PersistentHAMT.empty(),
                SnapshotId.of("empty"));
        return empty.asMutable();
    }

    /**
     * Triangle graph (undirected): 1--2, 2--3, 3--1.
     * Represented as directed edges in both directions.
     */
    public static GraphSnapshot<Double, Double> triangleGraph() {
        MutableGraphView<Double, Double> g = emptyMutable();
        g.addVertex(1L, 1.0);
        g.addVertex(2L, 1.0);
        g.addVertex(3L, 1.0);
        g.addEdge(1L, 2L, 1.0);
        g.addEdge(2L, 1L, 1.0);
        g.addEdge(2L, 3L, 1.0);
        g.addEdge(3L, 2L, 1.0);
        g.addEdge(3L, 1L, 1.0);
        g.addEdge(1L, 3L, 1.0);
        return g.toSnapshot(SnapshotId.of("triangle"));
    }

    /**
     * Linear directed graph: 1 -> 2 -> 3 -> 4 -> 5.
     * Edge weights are all 1.0.
     */
    public static GraphSnapshot<Double, Double> linearGraph() {
        MutableGraphView<Double, Double> g = emptyMutable();
        for (long i = 1; i <= 5; i++) {
            g.addVertex(i, 1.0);
        }
        for (long i = 1; i < 5; i++) {
            g.addEdge(i, i + 1, 1.0);
        }
        return g.toSnapshot(SnapshotId.of("linear"));
    }

    /**
     * Star graph (directed outward from center): center 0, spokes 1-5.
     * Edges: 0 -> 1, 0 -> 2, ..., 0 -> 5. Weight 1.0.
     */
    public static GraphSnapshot<Double, Double> starGraph() {
        MutableGraphView<Double, Double> g = emptyMutable();
        g.addVertex(0L, 1.0);
        for (long i = 1; i <= 5; i++) {
            g.addVertex(i, 1.0);
            g.addEdge(0L, i, 1.0);
        }
        return g.toSnapshot(SnapshotId.of("star"));
    }

    /**
     * Star graph with bidirectional edges: center 0, spokes 1-5.
     */
    public static GraphSnapshot<Double, Double> biStarGraph() {
        MutableGraphView<Double, Double> g = emptyMutable();
        g.addVertex(0L, 1.0);
        for (long i = 1; i <= 5; i++) {
            g.addVertex(i, 1.0);
            g.addEdge(0L, i, 1.0);
            g.addEdge(i, 0L, 1.0);
        }
        return g.toSnapshot(SnapshotId.of("bistar"));
    }

    /**
     * Disconnected graph with two components:
     * Component A: 1 -- 2 -- 3 (undirected)
     * Component B: 4 -- 5 -- 6 (undirected)
     * Vertex values are the vertex IDs (for CC initialization).
     */
    public static GraphSnapshot<Long, Double> disconnectedGraph() {
        MutableGraphView<Long, Double> g = emptyMutable();
        for (long i = 1; i <= 6; i++) {
            g.addVertex(i, i);
        }
        g.addEdge(1L, 2L, 1.0);
        g.addEdge(2L, 1L, 1.0);
        g.addEdge(2L, 3L, 1.0);
        g.addEdge(3L, 2L, 1.0);
        g.addEdge(4L, 5L, 1.0);
        g.addEdge(5L, 4L, 1.0);
        g.addEdge(5L, 6L, 1.0);
        g.addEdge(6L, 5L, 1.0);
        return g.toSnapshot(SnapshotId.of("disconnected"));
    }

    /**
     * Single vertex graph with no edges.
     */
    public static GraphSnapshot<Long, Double> singleVertexGraph() {
        MutableGraphView<Long, Double> g = emptyMutable();
        g.addVertex(1L, 1L);
        return g.toSnapshot(SnapshotId.of("single"));
    }

    /**
     * Complete directed graph K4 (all pairs connected both ways).
     * Vertices: 1, 2, 3, 4 with value 1.0.
     */
    public static GraphSnapshot<Double, Double> completeGraph4() {
        MutableGraphView<Double, Double> g = emptyMutable();
        for (long i = 1; i <= 4; i++) {
            g.addVertex(i, 1.0);
        }
        for (long i = 1; i <= 4; i++) {
            for (long j = 1; j <= 4; j++) {
                if (i != j) {
                    g.addEdge(i, j, 1.0);
                }
            }
        }
        return g.toSnapshot(SnapshotId.of("k4"));
    }

    /**
     * Linear directed graph with Long vertex values: 1 -- 2 -- 3 -- 4 (bidirectional).
     */
    public static GraphSnapshot<Long, Double> linearUndirectedLongGraph() {
        MutableGraphView<Long, Double> g = emptyMutable();
        for (long i = 1; i <= 4; i++) {
            g.addVertex(i, i);
        }
        for (long i = 1; i < 4; i++) {
            g.addEdge(i, i + 1, 1.0);
            g.addEdge(i + 1, i, 1.0);
        }
        return g.toSnapshot(SnapshotId.of("linear-long"));
    }

    /**
     * Linear directed graph with weighted edges: 1 --(2)--> 2 --(3)--> 3 --(1)--> 4 --(4)--> 5.
     */
    public static GraphSnapshot<Double, Double> weightedLinearGraph() {
        MutableGraphView<Double, Double> g = emptyMutable();
        for (long i = 1; i <= 5; i++) {
            g.addVertex(i, Double.MAX_VALUE);
        }
        g.addEdge(1L, 2L, 2.0);
        g.addEdge(2L, 3L, 3.0);
        g.addEdge(3L, 4L, 1.0);
        g.addEdge(4L, 5L, 4.0);
        return g.toSnapshot(SnapshotId.of("weighted-linear"));
    }

    /**
     * Graph with a disconnected vertex for reachability tests.
     * Connected: 1 -> 2 -> 3. Isolated: 4.
     */
    public static GraphSnapshot<Double, Double> graphWithIsolatedVertex() {
        MutableGraphView<Double, Double> g = emptyMutable();
        g.addVertex(1L, 0.0);
        g.addVertex(2L, Double.MAX_VALUE);
        g.addVertex(3L, Double.MAX_VALUE);
        g.addVertex(4L, Double.MAX_VALUE);
        g.addEdge(1L, 2L, 1.0);
        g.addEdge(2L, 3L, 1.0);
        return g.toSnapshot(SnapshotId.of("with-isolated"));
    }
}
