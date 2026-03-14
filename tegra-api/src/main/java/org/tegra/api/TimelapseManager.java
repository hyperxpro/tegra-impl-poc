package org.tegra.api;

import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;
import org.tegra.store.version.VersionIdGenerator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lifecycle manager for Timelapse instances.
 * Manages creation, retrieval, and branching of timelapses.
 */
public final class TimelapseManager {

    private final PartitionStore store;
    private final Map<String, Timelapse> timelapses;
    private final LineageGraph lineage;

    public TimelapseManager(PartitionStore store) {
        this.store = store;
        this.timelapses = new ConcurrentHashMap<>();
        this.lineage = new LineageGraph();
    }

    /**
     * Creates a new timelapse for the given graph ID.
     * Also creates an initial empty version in the store.
     *
     * @param graphId the graph identifier
     * @return the new Timelapse
     */
    public Timelapse create(String graphId) {
        if (timelapses.containsKey(graphId)) {
            throw new IllegalArgumentException("Timelapse already exists: " + graphId);
        }

        Timelapse timelapse = new Timelapse(store, graphId);

        // Create an initial empty version
        ByteArray initialVersionId = VersionIdGenerator.graphSnapshot(graphId, 0);
        store.createInitialVersion(initialVersionId);

        // Create a working version so the user can add data
        WorkingVersion working = store.branch(initialVersionId);
        timelapse.setCurrentWorking(working, initialVersionId);

        timelapses.put(graphId, timelapse);
        return timelapse;
    }

    /**
     * Retrieves an existing timelapse by graph ID.
     *
     * @param graphId the graph identifier
     * @return the Timelapse, or null if not found
     */
    public Timelapse get(String graphId) {
        return timelapses.get(graphId);
    }

    /**
     * Creates a new timelapse by branching from an existing snapshot.
     *
     * @param snapshotId the snapshot to branch from
     * @param branchName the name for the new branch
     * @return the new Timelapse
     */
    public Timelapse branch(ByteArray snapshotId, String branchName) {
        ByteArray branchVersionId = VersionIdGenerator.branch(snapshotId, branchName);
        Timelapse timelapse = new Timelapse(store, branchName);

        // Branch from the source snapshot
        WorkingVersion working = store.branch(snapshotId);

        // Commit the branch as a new version
        store.commit(working, branchVersionId);

        // Track lineage
        lineage.addRelationship(snapshotId, branchVersionId);

        timelapses.put(branchName, timelapse);
        return timelapse;
    }

    /**
     * Returns the lineage graph tracking snapshot relationships.
     */
    public LineageGraph lineage() {
        return lineage;
    }

    /**
     * Returns the underlying partition store.
     */
    public PartitionStore store() {
        return store;
    }
}
