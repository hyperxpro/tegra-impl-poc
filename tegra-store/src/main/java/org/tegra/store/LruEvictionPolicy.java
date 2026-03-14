package org.tegra.store;

import org.tegra.api.SnapshotId;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Least-recently-used eviction policy.
 * <p>
 * When the snapshot count exceeds the maximum, selects the snapshots
 * with the oldest {@link EvictionMetadata#lastAccessed()} timestamps.
 */
public final class LruEvictionPolicy implements EvictionPolicy {

    @Override
    public List<SnapshotId> selectForEviction(
            Map<SnapshotId, EvictionMetadata> candidates,
            int currentCount, int maxCount) {
        if (currentCount <= maxCount) {
            return List.of();
        }
        int toEvict = currentCount - maxCount;
        return candidates.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getValue().lastAccessed()))
                .limit(toEvict)
                .map(Map.Entry::getKey)
                .toList();
    }
}
