package org.tegra.store.eviction;

import org.tegra.store.version.ByteArray;
import org.tegra.store.version.VersionEntry;
import org.tegra.store.version.VersionMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background virtual thread for LRU eviction.
 * Tracks access timestamps and evicts versions whose last access time
 * falls below a configurable threshold.
 */
public final class LruEvictionManager {

    private final long evictionIntervalMs;
    private final long accessThresholdMs;
    private final VersionMap versionMap;
    private final ConcurrentHashMap<ByteArray, Long> accessTimes;
    private final AtomicBoolean running;
    private volatile Thread evictionThread;

    /**
     * Creates a new LRU eviction manager.
     *
     * @param evictionIntervalMs the interval between eviction scans in milliseconds
     * @param accessThresholdMs  the access age threshold for eviction in milliseconds
     * @param versionMap         the version map to evict from
     */
    public LruEvictionManager(long evictionIntervalMs, long accessThresholdMs, VersionMap versionMap) {
        this.evictionIntervalMs = evictionIntervalMs;
        this.accessThresholdMs = accessThresholdMs;
        this.versionMap = versionMap;
        this.accessTimes = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
    }

    /**
     * Starts the background eviction thread (virtual thread).
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            evictionThread = Thread.ofVirtual()
                    .name("lru-eviction")
                    .start(this::evictionLoop);
        }
    }

    /**
     * Stops the background eviction thread.
     */
    public void stop() {
        running.set(false);
        Thread t = evictionThread;
        if (t != null) {
            t.interrupt();
        }
    }

    /**
     * Records an access to a version, updating its timestamp.
     */
    public void touchVersion(ByteArray versionId) {
        accessTimes.put(versionId, System.currentTimeMillis());
    }

    /**
     * Returns the last access time for a version, or -1 if not tracked.
     */
    public long lastAccessTime(ByteArray versionId) {
        Long time = accessTimes.get(versionId);
        return time != null ? time : -1;
    }

    /**
     * Evicts a specific version from the version map.
     */
    public void evictVersion(ByteArray versionId) {
        versionMap.remove(versionId);
        accessTimes.remove(versionId);
    }

    /**
     * Runs one eviction scan, evicting all versions older than the threshold.
     */
    public void runEvictionScan() {
        long now = System.currentTimeMillis();
        long cutoff = now - accessThresholdMs;

        for (var entry : accessTimes.entrySet()) {
            if (entry.getValue() < cutoff) {
                evictVersion(entry.getKey());
            }
        }
    }

    /**
     * Returns true if the eviction thread is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    private void evictionLoop() {
        while (running.get()) {
            try {
                Thread.sleep(evictionIntervalMs);
                runEvictionScan();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
