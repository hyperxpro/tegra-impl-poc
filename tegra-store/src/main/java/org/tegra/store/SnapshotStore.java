package org.tegra.store;

import org.tegra.api.GraphSnapshot;
import org.tegra.api.SnapshotId;

import java.util.List;

/**
 * High-level store managing graph snapshots with eviction support.
 * <p>
 * Combines a {@link VersionMap} for version storage with an
 * {@link EvictionManager} to enforce capacity limits via pluggable
 * {@link EvictionPolicy} implementations.
 *
 * @param <V> vertex property type
 * @param <E> edge property type
 */
public final class SnapshotStore<V, E> {

    private static final int DEFAULT_MAX_SNAPSHOTS = 1000;

    private final VersionMap<V, E> versionMap;
    private final EvictionManager<V, E> evictionManager;

    /**
     * Creates a snapshot store with default LRU eviction and 1000 max snapshots.
     */
    public SnapshotStore() {
        this(EvictionPolicy.lru(), DEFAULT_MAX_SNAPSHOTS);
    }

    /**
     * Creates a snapshot store with the given eviction policy and capacity.
     */
    public SnapshotStore(EvictionPolicy policy, int maxSnapshots) {
        this.versionMap = new VersionMap<>();
        this.evictionManager = new EvictionManager<>(policy, maxSnapshots);
    }

    /**
     * Creates and stores a new snapshot from the given version root.
     *
     * @param id   the snapshot identifier
     * @param root the version root containing the graph data
     * @return the created graph snapshot
     */
    public GraphSnapshot<V, E> createSnapshot(SnapshotId id, VersionRoot<V, E> root) {
        versionMap.commit(id, root);
        evictionManager.onStore(id);
        evictIfNeeded();
        return toGraphSnapshot(id, root);
    }

    /**
     * Retrieves a snapshot by its identifier.
     *
     * @param id the snapshot identifier
     * @return the graph snapshot, or {@code null} if not found
     */
    public GraphSnapshot<V, E> getSnapshot(SnapshotId id) {
        VersionRoot<V, E> root = versionMap.get(id);
        if (root == null) {
            return null;
        }
        evictionManager.onAccess(id);
        return toGraphSnapshot(id, root);
    }

    /**
     * Finds all snapshot IDs matching the given prefix.
     */
    public List<SnapshotId> findSnapshots(String prefix) {
        return versionMap.findByPrefix(prefix);
    }

    /**
     * Checks capacity and evicts snapshots if the limit is exceeded.
     */
    public void evictIfNeeded() {
        List<SnapshotId> toEvict = evictionManager.checkEviction();
        for (SnapshotId id : toEvict) {
            versionMap.remove(id);
        }
    }

    /**
     * Returns the current number of stored snapshots.
     */
    public int snapshotCount() {
        return versionMap.size();
    }

    private GraphSnapshot<V, E> toGraphSnapshot(SnapshotId id, VersionRoot<V, E> root) {
        return GraphSnapshot.create(root.vertexData(), root.outEdges(), root.inEdges(), id);
    }
}
