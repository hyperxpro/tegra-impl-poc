package org.tegra.compute.ice;

import org.tegra.api.Edge;
import org.tegra.api.GraphDelta;
import org.tegra.api.GraphSnapshot;
import org.tegra.api.Vertex;
import org.tegra.pds.common.DiffEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Computes structural and value differences between two {@link GraphSnapshot}s,
 * producing a {@link GraphDelta} that captures added, removed, and modified vertices and edges.
 */
public final class DiffEngine {

    /**
     * Compute the delta between snapshot {@code a} (old) and snapshot {@code b} (new).
     *
     * @param a the previous snapshot
     * @param b the current snapshot
     * @return a delta describing all changes from {@code a} to {@code b}
     */
    public <V, E> GraphDelta<V, E> diff(GraphSnapshot<V, E> a, GraphSnapshot<V, E> b) {
        // Collect vertices from both snapshots
        Map<Long, V> verticesA = new HashMap<>();
        a.vertices().forEach(v -> verticesA.put(v.id(), v.properties()));

        Map<Long, V> verticesB = new HashMap<>();
        b.vertices().forEach(v -> verticesB.put(v.id(), v.properties()));

        List<DiffEntry<Long, V>> vertexChanges = new ArrayList<>();

        // Find added and modified vertices
        for (var entry : verticesB.entrySet()) {
            long id = entry.getKey();
            V newVal = entry.getValue();
            if (!verticesA.containsKey(id)) {
                vertexChanges.add(new DiffEntry<>(id, null, newVal, DiffEntry.ChangeType.ADDED));
            } else if (!Objects.equals(verticesA.get(id), newVal)) {
                vertexChanges.add(new DiffEntry<>(id, verticesA.get(id), newVal, DiffEntry.ChangeType.MODIFIED));
            }
        }

        // Find removed vertices
        for (var entry : verticesA.entrySet()) {
            long id = entry.getKey();
            if (!verticesB.containsKey(id)) {
                vertexChanges.add(new DiffEntry<>(id, entry.getValue(), null, DiffEntry.ChangeType.REMOVED));
            }
        }

        // Collect edges from both snapshots
        record EdgeKey(long src, long dst) {}

        Map<EdgeKey, E> edgesA = new HashMap<>();
        a.edges().forEach(e -> edgesA.put(new EdgeKey(e.src(), e.dst()), e.properties()));

        Map<EdgeKey, E> edgesB = new HashMap<>();
        b.edges().forEach(e -> edgesB.put(new EdgeKey(e.src(), e.dst()), e.properties()));

        // Edge changes are keyed by source vertex ID in GraphDelta
        List<DiffEntry<Long, E>> edgeChanges = new ArrayList<>();

        for (var entry : edgesB.entrySet()) {
            EdgeKey key = entry.getKey();
            E newVal = entry.getValue();
            if (!edgesA.containsKey(key)) {
                edgeChanges.add(new DiffEntry<>(key.src(), null, newVal, DiffEntry.ChangeType.ADDED));
            } else if (!Objects.equals(edgesA.get(key), newVal)) {
                edgeChanges.add(new DiffEntry<>(key.src(), edgesA.get(key), newVal, DiffEntry.ChangeType.MODIFIED));
            }
        }

        for (var entry : edgesA.entrySet()) {
            EdgeKey key = entry.getKey();
            if (!edgesB.containsKey(key)) {
                edgeChanges.add(new DiffEntry<>(key.src(), entry.getValue(), null, DiffEntry.ChangeType.REMOVED));
            }
        }

        return new GraphDelta<>(vertexChanges, edgeChanges);
    }
}
