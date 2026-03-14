package org.tegra.store.partition;

import org.tegra.store.GraphView;
import org.tegra.store.version.ByteArray;
import org.tegra.store.version.VersionEntry;
import org.tegra.store.version.VersionMap;

/**
 * Core partition store implementing the DGSI per-partition graph store.
 * <p>
 * Provides branch/commit/retrieve lifecycle for versioned graph snapshots.
 * Each partition maintains a version map of committed snapshots.
 */
public final class PartitionStore {

    private final VersionMap versionMap;

    public PartitionStore() {
        this.versionMap = new VersionMap();
    }

    /**
     * Returns the version map for direct access.
     */
    public VersionMap versionMap() {
        return versionMap;
    }

    /**
     * Creates a mutable working version branched from the given version.
     * The working version starts with the same tree roots as the source version.
     *
     * @param versionId the source version to branch from
     * @return a new mutable WorkingVersion
     * @throws IllegalArgumentException if the version does not exist
     */
    public WorkingVersion branch(ByteArray versionId) {
        VersionEntry entry = versionMap.get(versionId);
        if (entry == null) {
            throw new IllegalArgumentException("Version not found: " + versionId);
        }
        // Update access timestamp
        versionMap.put(versionId, entry.withAccessTimestamp(System.currentTimeMillis()));
        // Create working version with same roots
        return new WorkingVersion(entry.vertexRoot(), entry.edgeRoot(), versionId);
    }

    /**
     * Commits a working version as a new immutable snapshot.
     * Freezes the working version by recording its current tree roots in the version map.
     *
     * @param working      the working version to commit
     * @param newVersionId the ID for the new version
     * @return the version ID of the committed snapshot
     */
    public ByteArray commit(WorkingVersion working, ByteArray newVersionId) {
        VersionEntry entry = new VersionEntry(
                working.vertexRoot(),
                working.edgeRoot(),
                System.currentTimeMillis(),
                null
        );
        versionMap.put(newVersionId, entry);
        return newVersionId;
    }

    /**
     * Retrieves a read-only view of the given version.
     *
     * @param versionId the version to retrieve
     * @return a read-only GraphView
     * @throws IllegalArgumentException if the version does not exist
     */
    public GraphView retrieve(ByteArray versionId) {
        VersionEntry entry = versionMap.get(versionId);
        if (entry == null) {
            throw new IllegalArgumentException("Version not found: " + versionId);
        }
        // Update access timestamp
        versionMap.put(versionId, entry.withAccessTimestamp(System.currentTimeMillis()));
        return new GraphView(entry.vertexRoot(), entry.edgeRoot(), versionId);
    }

    /**
     * Evicts a version from the version map.
     *
     * @param versionId the version to evict
     */
    public void evict(ByteArray versionId) {
        versionMap.remove(versionId);
    }

    /**
     * Creates an initial empty version with the given ID.
     * Used to bootstrap a new graph.
     */
    public void createInitialVersion(ByteArray versionId) {
        VersionEntry entry = new VersionEntry(null, null, System.currentTimeMillis(), null);
        versionMap.put(versionId, entry);
    }
}
