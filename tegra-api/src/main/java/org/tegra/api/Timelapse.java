package org.tegra.api;

import org.tegra.pds.common.DiffEntry;
import org.tegra.pds.hamt.PersistentHAMT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The primary user-facing abstraction for managing evolving graph snapshots.
 * <p>
 * A Timelapse maintains a collection of named, immutable graph snapshots that
 * share structure via persistent data structures. It supports branching,
 * diffing, and running algorithms against any snapshot.
 *
 * @param <V> the vertex property type
 * @param <E> the edge property type
 */
public final class Timelapse<V, E> {

    private final String graphId;
    private final Map<SnapshotId, GraphSnapshot<V, E>> snapshots;

    private Timelapse(String graphId) {
        this.graphId = Objects.requireNonNull(graphId, "graphId");
        this.snapshots = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new empty Timelapse for the given graph identifier.
     */
    public static <V, E> Timelapse<V, E> create(String graphId) {
        return new Timelapse<>(graphId);
    }

    /**
     * Saves a snapshot under the given string identifier.
     *
     * @param snapshot the snapshot to save
     * @param id       the string identifier for the snapshot
     * @return the {@link SnapshotId} under which the snapshot was stored
     */
    public SnapshotId save(GraphSnapshot<V, E> snapshot, String id) {
        SnapshotId snapshotId = SnapshotId.of(id);
        GraphSnapshot<V, E> stored = new GraphSnapshot<>(
                snapshot.vertexData(), snapshot.outEdgeIndex(), snapshot.inEdgeIndex(), snapshotId);
        snapshots.put(snapshotId, stored);
        return snapshotId;
    }

    /**
     * Retrieves a snapshot by its identifier.
     *
     * @param id the snapshot identifier
     * @return the snapshot
     * @throws IllegalArgumentException if no snapshot exists with the given ID
     */
    public GraphSnapshot<V, E> retrieve(SnapshotId id) {
        GraphSnapshot<V, E> snapshot = snapshots.get(id);
        if (snapshot == null) {
            throw new IllegalArgumentException("Snapshot not found: " + id);
        }
        return snapshot;
    }

    /**
     * Retrieves all snapshots whose IDs start with the given prefix.
     */
    public List<GraphSnapshot<V, E>> retrieveByPrefix(String prefix) {
        SnapshotId prefixId = SnapshotId.of(prefix);
        List<GraphSnapshot<V, E>> result = new ArrayList<>();
        for (var entry : snapshots.entrySet()) {
            if (entry.getKey().hasPrefix(prefixId)) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    /**
     * Computes the difference between two snapshots.
     */
    public GraphDelta<V, E> diff(SnapshotId a, SnapshotId b) {
        GraphSnapshot<V, E> snapA = retrieve(a);
        GraphSnapshot<V, E> snapB = retrieve(b);

        List<DiffEntry<Long, V>> vertexChanges = computeVertexDiff(snapA, snapB);
        List<DiffEntry<Long, E>> edgeChanges = computeEdgeDiff(snapA, snapB);

        return new GraphDelta<>(vertexChanges, edgeChanges);
    }

    /**
     * Creates a mutable working copy branched from the given snapshot.
     */
    public MutableGraphView<V, E> branch(SnapshotId source) {
        return retrieve(source).asMutable();
    }

    /**
     * Runs a graph algorithm against the snapshot with the given ID.
     */
    public <R> R run(SnapshotId id, GraphAlgorithm<V, E, R> algorithm) {
        return algorithm.execute(retrieve(id));
    }

    /**
     * Returns the number of stored snapshots.
     */
    public int snapshotCount() {
        return snapshots.size();
    }

    /**
     * Returns {@code true} if a snapshot with the given ID exists.
     */
    public boolean hasSnapshot(SnapshotId id) {
        return snapshots.containsKey(id);
    }

    /**
     * Creates an initial empty snapshot (no vertices, no edges).
     */
    public GraphSnapshot<V, E> emptySnapshot() {
        return new GraphSnapshot<>(
                PersistentHAMT.empty(),
                PersistentHAMT.empty(),
                PersistentHAMT.empty(),
                SnapshotId.of(graphId + "_empty"));
    }

    private List<DiffEntry<Long, V>> computeVertexDiff(GraphSnapshot<V, E> snapA, GraphSnapshot<V, E> snapB) {
        List<DiffEntry<Long, V>> changes = new ArrayList<>();

        Set<Long> allIds = new HashSet<>();
        snapA.vertexData().forEach((id, v) -> allIds.add(id));
        snapB.vertexData().forEach((id, v) -> allIds.add(id));

        for (Long id : allIds) {
            V oldVal = snapA.vertexData().get(id);
            V newVal = snapB.vertexData().get(id);

            if (oldVal == null && newVal != null) {
                changes.add(new DiffEntry<>(id, null, newVal, DiffEntry.ChangeType.ADDED));
            } else if (oldVal != null && newVal == null) {
                changes.add(new DiffEntry<>(id, oldVal, null, DiffEntry.ChangeType.REMOVED));
            } else if (oldVal != null && !Objects.equals(oldVal, newVal)) {
                changes.add(new DiffEntry<>(id, oldVal, newVal, DiffEntry.ChangeType.MODIFIED));
            }
        }
        return changes;
    }

    private List<DiffEntry<Long, E>> computeEdgeDiff(GraphSnapshot<V, E> snapA, GraphSnapshot<V, E> snapB) {
        List<DiffEntry<Long, E>> changes = new ArrayList<>();

        Map<Long, Map<Long, E>> edgesA = new HashMap<>();
        snapA.edges().forEach(e -> edgesA.computeIfAbsent(e.src(), k -> new HashMap<>()).put(e.dst(), e.properties()));

        Map<Long, Map<Long, E>> edgesB = new HashMap<>();
        snapB.edges().forEach(e -> edgesB.computeIfAbsent(e.src(), k -> new HashMap<>()).put(e.dst(), e.properties()));

        Set<Long> allSrcs = new HashSet<>();
        allSrcs.addAll(edgesA.keySet());
        allSrcs.addAll(edgesB.keySet());

        for (Long src : allSrcs) {
            Map<Long, E> aEdges = edgesA.getOrDefault(src, Map.of());
            Map<Long, E> bEdges = edgesB.getOrDefault(src, Map.of());

            Set<Long> allDsts = new HashSet<>();
            allDsts.addAll(aEdges.keySet());
            allDsts.addAll(bEdges.keySet());

            for (Long dst : allDsts) {
                E oldEdgeVal = aEdges.get(dst);
                E newEdgeVal = bEdges.get(dst);

                if (oldEdgeVal == null && newEdgeVal != null) {
                    changes.add(new DiffEntry<>(src, null, newEdgeVal, DiffEntry.ChangeType.ADDED));
                } else if (oldEdgeVal != null && newEdgeVal == null) {
                    changes.add(new DiffEntry<>(src, oldEdgeVal, null, DiffEntry.ChangeType.REMOVED));
                } else if (oldEdgeVal != null && !Objects.equals(oldEdgeVal, newEdgeVal)) {
                    changes.add(new DiffEntry<>(src, oldEdgeVal, newEdgeVal, DiffEntry.ChangeType.MODIFIED));
                }
            }
        }
        return changes;
    }
}
