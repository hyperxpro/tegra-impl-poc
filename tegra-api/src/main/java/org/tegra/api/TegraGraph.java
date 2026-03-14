package org.tegra.api;

import org.tegra.serde.EdgeData;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.version.ByteArray;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Snapshot-aware graph operators supporting both single and multi-snapshot operations.
 * Extends the standard graph API to operate on user-specified snapshots.
 */
public final class TegraGraph {

    private final TimelapseManager manager;

    public TegraGraph(TimelapseManager manager) {
        this.manager = manager;
    }

    /**
     * Returns an iterator over vertices in the given snapshot.
     */
    public Iterator<VertexData> vertices(ByteArray snapshotId) {
        GraphView view = manager.store().retrieve(snapshotId);
        return view.vertices();
    }

    /**
     * Returns an iterator over edges in the given snapshot.
     */
    public Iterator<EdgeData> edges(ByteArray snapshotId) {
        GraphView view = manager.store().retrieve(snapshotId);
        return view.edges();
    }

    /**
     * Applies a function to all vertices across multiple snapshots.
     * Returns a map from snapshot ID to the function result.
     *
     * @param snapshotIds the snapshot IDs to process
     * @param fn          the function to apply to each vertex
     * @param <R>         the result type
     * @return map from snapshot ID to list of results
     */
    public <R> Map<ByteArray, List<R>> mapVertices(
            List<ByteArray> snapshotIds, Function<VertexData, R> fn) {
        Map<ByteArray, List<R>> results = new HashMap<>();
        for (ByteArray snapshotId : snapshotIds) {
            GraphView view = manager.store().retrieve(snapshotId);
            java.util.List<R> snapshotResults = new java.util.ArrayList<>();
            Iterator<VertexData> vertices = view.vertices();
            while (vertices.hasNext()) {
                snapshotResults.add(fn.apply(vertices.next()));
            }
            results.put(snapshotId, snapshotResults);
        }
        return results;
    }

    /**
     * Returns the underlying timelapse manager.
     */
    public TimelapseManager manager() {
        return manager;
    }
}
