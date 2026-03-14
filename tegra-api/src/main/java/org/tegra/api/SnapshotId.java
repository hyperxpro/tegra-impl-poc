package org.tegra.api;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

/**
 * Snapshot identifier using hierarchical naming with byte-level comparison.
 * <p>
 * Naming convention: {@code graphId_epochSeconds} for time-based snapshots,
 * with {@code _algoId_iteration} suffixes for algorithm iterations.
 */
public record SnapshotId(byte[] raw) implements Comparable<SnapshotId> {

    /**
     * Creates a snapshot ID from a graph identifier and timestamp.
     */
    public static SnapshotId of(String graphId, Instant timestamp) {
        return new SnapshotId((graphId + "_" + timestamp.getEpochSecond()).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates a snapshot ID from a plain string identifier.
     */
    public static SnapshotId of(String id) {
        return new SnapshotId(id.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates a snapshot ID for an algorithm iteration, derived from a base snapshot.
     */
    public static SnapshotId ofIteration(SnapshotId base, String algoId, int iter) {
        return new SnapshotId(
                (new String(base.raw, StandardCharsets.UTF_8) + "_" + algoId + "_" + iter)
                        .getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns {@code true} if this snapshot ID starts with the given prefix.
     */
    public boolean hasPrefix(SnapshotId prefix) {
        if (prefix.raw.length > raw.length) return false;
        return Arrays.mismatch(raw, 0, prefix.raw.length, prefix.raw, 0, prefix.raw.length) < 0;
    }

    /**
     * Returns this snapshot ID as a UTF-8 string.
     */
    public String asString() {
        return new String(raw, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(SnapshotId other) {
        return Arrays.compare(raw, other.raw);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SnapshotId that)) return false;
        return Arrays.equals(raw, that.raw);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(raw);
    }

    @Override
    public String toString() {
        return asString();
    }
}
