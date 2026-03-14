package org.tegra.store.version;

import java.nio.charset.StandardCharsets;

/**
 * Generates version IDs for the DGSI system.
 * <p>
 * Version ID scheme (from the paper, section 5.3.1):
 * <ul>
 *   <li>{GRAPH_ID}_{UNIX_TIMESTAMP} - graph snapshot</li>
 *   <li>{GRAPH_ID}_{UNIX_TIMESTAMP}_{ALGO_ID}_{ITERATION} - computation state</li>
 *   <li>{SOURCE_ID}_branch_{BRANCH_NAME} - branch</li>
 * </ul>
 */
public final class VersionIdGenerator {

    private VersionIdGenerator() {
        // utility class
    }

    /**
     * Generates a version ID for a graph snapshot.
     *
     * @param graphId   the graph identifier
     * @param timestamp the UNIX timestamp
     * @return the version ID as ByteArray
     */
    public static ByteArray graphSnapshot(String graphId, long timestamp) {
        String id = graphId + "_" + timestamp;
        return new ByteArray(id.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generates a version ID for a computation iteration.
     *
     * @param snapshotId  the parent snapshot's version ID
     * @param algorithmId the algorithm identifier
     * @param iteration   the iteration number
     * @return the version ID as ByteArray
     */
    public static ByteArray computationIteration(ByteArray snapshotId, String algorithmId, int iteration) {
        String base = new String(snapshotId.data(), StandardCharsets.UTF_8);
        String id = base + "_" + algorithmId + "_" + iteration;
        return new ByteArray(id.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generates a version ID for a branch.
     *
     * @param sourceId   the source version's ID
     * @param branchName the branch name
     * @return the version ID as ByteArray
     */
    public static ByteArray branch(ByteArray sourceId, String branchName) {
        String base = new String(sourceId.data(), StandardCharsets.UTF_8);
        String id = base + "_branch_" + branchName;
        return new ByteArray(id.getBytes(StandardCharsets.UTF_8));
    }
}
