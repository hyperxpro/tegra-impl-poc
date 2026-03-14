package org.tegra.store;

import org.tegra.api.SnapshotId;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages eviction metadata tracking and delegates eviction decisions
 * to a pluggable {@link EvictionPolicy}.
 *
 * @param <V> vertex property type
 * @param <E> edge property type
 */
public final class EvictionManager<V, E> {

    private final EvictionPolicy policy;
    private final int maxSnapshots;
    private final Map<SnapshotId, EvictionMetadata> metadata;

    public EvictionManager(EvictionPolicy policy, int maxSnapshots) {
        this.policy = policy;
        this.maxSnapshots = maxSnapshots;
        this.metadata = new LinkedHashMap<>();
    }

    /**
     * Records an access to the given snapshot, updating last-accessed time
     * and incrementing the access counter.
     */
    public void onAccess(SnapshotId id) {
        metadata.computeIfPresent(id, (k, existing) ->
                new EvictionMetadata(Instant.now(), existing.sizeBytes(), existing.accessCount() + 1));
    }

    /**
     * Records that a new snapshot has been stored.
     */
    public void onStore(SnapshotId id) {
        metadata.put(id, new EvictionMetadata(Instant.now(), 0L, 0));
    }

    /**
     * Checks whether eviction is needed and returns the list of snapshot IDs
     * that should be evicted according to the configured policy.
     */
    public List<SnapshotId> checkEviction() {
        int currentCount = metadata.size();
        if (currentCount <= maxSnapshots) {
            return List.of();
        }
        List<SnapshotId> toEvict = policy.selectForEviction(
                Collections.unmodifiableMap(metadata), currentCount, maxSnapshots);
        for (SnapshotId id : toEvict) {
            metadata.remove(id);
        }
        return toEvict;
    }

    /**
     * Returns the current number of tracked snapshots.
     */
    public int trackedCount() {
        return metadata.size();
    }
}
