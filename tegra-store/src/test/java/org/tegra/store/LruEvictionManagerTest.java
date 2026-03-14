package org.tegra.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.store.eviction.LruEvictionManager;
import org.tegra.store.version.ByteArray;
import org.tegra.store.version.VersionEntry;
import org.tegra.store.version.VersionMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for LruEvictionManager: access patterns, eviction ordering.
 */
class LruEvictionManagerTest {

    private VersionMap versionMap;
    private LruEvictionManager evictionManager;

    @BeforeEach
    void setUp() {
        versionMap = new VersionMap();
        // 100ms scan interval, 500ms access threshold
        evictionManager = new LruEvictionManager(100, 500, versionMap);
    }

    @AfterEach
    void tearDown() {
        evictionManager.stop();
    }

    @Test
    void touchVersionUpdatesAccessTime() {
        ByteArray v1 = ByteArray.fromString("v1");
        evictionManager.touchVersion(v1);

        assertThat(evictionManager.lastAccessTime(v1)).isGreaterThan(0);
    }

    @Test
    void untrackedVersionReturnsNegativeOne() {
        ByteArray v1 = ByteArray.fromString("v1");
        assertThat(evictionManager.lastAccessTime(v1)).isEqualTo(-1);
    }

    @Test
    void evictionScanRemovesOldVersions() {
        ByteArray v1 = ByteArray.fromString("v1");
        ByteArray v2 = ByteArray.fromString("v2");

        versionMap.put(v1, new VersionEntry(null, null, 1L, null));
        versionMap.put(v2, new VersionEntry(null, null, 2L, null));

        // Touch v1 with an old timestamp (simulate old access)
        evictionManager.touchVersion(v1);

        // Touch v2 with current timestamp
        evictionManager.touchVersion(v2);

        // Manually set v1's access time to be old enough for eviction
        // We do this by creating a new eviction manager with 0ms threshold
        LruEvictionManager strictManager = new LruEvictionManager(100, 0, versionMap);
        strictManager.touchVersion(v1);

        // Wait a tiny bit so the timestamp is definitely in the past
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        strictManager.runEvictionScan();

        // v1 should be evicted (access time is older than 0ms ago)
        assertThat(versionMap.containsKey(v1)).isFalse();
    }

    @Test
    void recentlyAccessedVersionsAreNotEvicted() {
        ByteArray v1 = ByteArray.fromString("v1");
        versionMap.put(v1, new VersionEntry(null, null, 1L, null));

        // Touch it now — it should be recent enough
        evictionManager.touchVersion(v1);

        // Run eviction scan (threshold is 500ms, so recent touch should survive)
        evictionManager.runEvictionScan();

        assertThat(versionMap.containsKey(v1)).isTrue();
    }

    @Test
    void startAndStopDoNotThrow() {
        evictionManager.start();
        assertThat(evictionManager.isRunning()).isTrue();

        evictionManager.stop();
        // Give the thread a moment to terminate
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(evictionManager.isRunning()).isFalse();
    }

    @Test
    void evictSpecificVersion() {
        ByteArray v1 = ByteArray.fromString("v1");
        versionMap.put(v1, new VersionEntry(null, null, 1L, null));
        evictionManager.touchVersion(v1);

        evictionManager.evictVersion(v1);

        assertThat(versionMap.containsKey(v1)).isFalse();
        assertThat(evictionManager.lastAccessTime(v1)).isEqualTo(-1);
    }
}
