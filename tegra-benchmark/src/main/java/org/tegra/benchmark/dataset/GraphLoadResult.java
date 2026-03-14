package org.tegra.benchmark.dataset;

import org.tegra.store.version.ByteArray;

/**
 * Result of loading a graph into a PartitionStore.
 *
 * @param versionId   the version ID under which the graph was committed
 * @param vertexCount number of vertices loaded
 * @param edgeCount   number of edges loaded
 * @param loadTimeMs  wall-clock time to load in milliseconds
 */
public record GraphLoadResult(
        ByteArray versionId,
        long vertexCount,
        long edgeCount,
        long loadTimeMs
) {}
