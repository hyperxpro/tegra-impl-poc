package org.tegra.benchmark.dataset;

import org.tegra.store.partition.PartitionStore;
import org.tegra.store.version.ByteArray;

/**
 * Interface for loading graph datasets into a PartitionStore.
 */
public interface DatasetLoader {

    /**
     * Loads a graph into the given store and commits it under the given version ID.
     *
     * @param store     the partition store to load into
     * @param versionId the version ID to commit the loaded graph under
     * @return the load result with vertex/edge counts and timing
     */
    GraphLoadResult load(PartitionStore store, ByteArray versionId);
}
