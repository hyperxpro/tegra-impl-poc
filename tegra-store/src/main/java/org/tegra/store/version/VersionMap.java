package org.tegra.store.version;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe version map implementation backed by ConcurrentHashMap.
 * Provides the version index operations required by DGSI.
 */
public final class VersionMap implements VersionIndex {

    private final ConcurrentHashMap<ByteArray, VersionEntry> map;

    public VersionMap() {
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public VersionEntry get(ByteArray id) {
        return map.get(id);
    }

    @Override
    public List<ByteArray> matchPrefix(ByteArray prefix) {
        List<ByteArray> result = new ArrayList<>();
        for (ByteArray key : map.keySet()) {
            if (key.startsWith(prefix)) {
                result.add(key);
            }
        }
        result.sort(ByteArray::compareTo);
        return result;
    }

    @Override
    public List<ByteArray> matchRange(ByteArray start, ByteArray end) {
        List<ByteArray> result = new ArrayList<>();
        for (ByteArray key : map.keySet()) {
            if (key.compareTo(start) >= 0 && key.compareTo(end) < 0) {
                result.add(key);
            }
        }
        result.sort(ByteArray::compareTo);
        return result;
    }

    @Override
    public void put(ByteArray id, VersionEntry entry) {
        map.put(id, entry);
    }

    @Override
    public void remove(ByteArray id) {
        map.remove(id);
    }

    /**
     * Returns the number of versions in the map.
     */
    public int size() {
        return map.size();
    }

    /**
     * Returns true if the map contains the given version ID.
     */
    public boolean containsKey(ByteArray id) {
        return map.containsKey(id);
    }

    /**
     * Returns all version IDs in the map.
     */
    public List<ByteArray> allKeys() {
        List<ByteArray> keys = new ArrayList<>(map.keySet());
        keys.sort(ByteArray::compareTo);
        return keys;
    }
}
