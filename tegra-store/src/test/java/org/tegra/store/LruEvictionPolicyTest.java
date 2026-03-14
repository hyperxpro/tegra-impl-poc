package org.tegra.store;

import org.junit.jupiter.api.Test;
import org.tegra.api.SnapshotId;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LruEvictionPolicyTest {

    private final LruEvictionPolicy policy = new LruEvictionPolicy();

    @Test
    void testNoEvictionWhenUnderLimit() {
        Map<SnapshotId, EvictionMetadata> candidates = new LinkedHashMap<>();
        candidates.put(SnapshotId.of("snap1"),
                new EvictionMetadata(Instant.ofEpochSecond(100), 0L, 1));
        candidates.put(SnapshotId.of("snap2"),
                new EvictionMetadata(Instant.ofEpochSecond(200), 0L, 1));

        List<SnapshotId> result = policy.selectForEviction(candidates, 2, 5);
        assertThat(result).isEmpty();
    }

    @Test
    void testEvictsOldest() {
        Map<SnapshotId, EvictionMetadata> candidates = new LinkedHashMap<>();
        candidates.put(SnapshotId.of("oldest"),
                new EvictionMetadata(Instant.ofEpochSecond(100), 0L, 1));
        candidates.put(SnapshotId.of("newest"),
                new EvictionMetadata(Instant.ofEpochSecond(300), 0L, 1));
        candidates.put(SnapshotId.of("middle"),
                new EvictionMetadata(Instant.ofEpochSecond(200), 0L, 1));

        List<SnapshotId> result = policy.selectForEviction(candidates, 3, 2);
        assertThat(result).hasSize(1);
        assertThat(result.getFirst().asString()).isEqualTo("oldest");
    }

    @Test
    void testEvictsCorrectCount() {
        Map<SnapshotId, EvictionMetadata> candidates = new LinkedHashMap<>();
        candidates.put(SnapshotId.of("s1"),
                new EvictionMetadata(Instant.ofEpochSecond(100), 0L, 1));
        candidates.put(SnapshotId.of("s2"),
                new EvictionMetadata(Instant.ofEpochSecond(200), 0L, 1));
        candidates.put(SnapshotId.of("s3"),
                new EvictionMetadata(Instant.ofEpochSecond(300), 0L, 1));
        candidates.put(SnapshotId.of("s4"),
                new EvictionMetadata(Instant.ofEpochSecond(400), 0L, 1));
        candidates.put(SnapshotId.of("s5"),
                new EvictionMetadata(Instant.ofEpochSecond(500), 0L, 1));

        // Need to go from 5 to 2, so evict 3
        List<SnapshotId> result = policy.selectForEviction(candidates, 5, 2);
        assertThat(result).hasSize(3);
        assertThat(result).extracting(SnapshotId::asString)
                .containsExactly("s1", "s2", "s3");
    }
}
