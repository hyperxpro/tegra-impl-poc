package org.tegra.store;

import java.time.Instant;

/**
 * Tracking metadata for snapshot eviction decisions.
 *
 * @param lastAccessed the instant this snapshot was last accessed
 * @param sizeBytes    estimated size in bytes
 * @param accessCount  total number of accesses
 */
public record EvictionMetadata(
        Instant lastAccessed,
        long sizeBytes,
        int accessCount
) {}
