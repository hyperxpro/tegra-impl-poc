package org.tegra.cluster.barrier;

import org.tegra.store.version.ByteArray;

/**
 * Interface for a partition node that participates in distributed snapshot commits.
 * Each partition node can prepare and commit a snapshot at a given version.
 */
public interface PartitionNode {

    /**
     * Prepare this partition for a commit at the given version.
     * Flushes pending mutations and prepares the working version for commit.
     *
     * @param versionId the version ID to prepare for
     */
    void prepareCommit(ByteArray versionId);

    /**
     * Finalize the commit at the given version.
     * After this call, the version is visible for reads.
     *
     * @param versionId the version ID to commit
     */
    void commit(ByteArray versionId);
}
