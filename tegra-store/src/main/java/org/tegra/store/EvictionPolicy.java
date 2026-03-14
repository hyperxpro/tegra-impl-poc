package org.tegra.store;

import org.tegra.api.SnapshotId;

import java.util.List;
import java.util.Map;

/**
 * SPI interface for snapshot eviction strategies.
 * <p>
 * Implementations decide which snapshots to remove when the store exceeds
 * its capacity limit.
 */
public interface EvictionPolicy {

    /**
     * Selects snapshot IDs for eviction based on the provided metadata.
     *
     * @param candidates   metadata for all candidate snapshots
     * @param currentCount the current number of snapshots
     * @param maxCount     the maximum allowed snapshots
     * @return ordered list of snapshot IDs to evict
     */
    List<SnapshotId> selectForEviction(
            Map<SnapshotId, EvictionMetadata> candidates,
            int currentCount, int maxCount);

    /**
     * Returns an LRU (least-recently-used) eviction policy.
     */
    static EvictionPolicy lru() {
        return new LruEvictionPolicy();
    }
}
