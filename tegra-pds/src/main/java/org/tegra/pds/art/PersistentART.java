package org.tegra.pds.art;

import org.tegra.pds.common.DiffEntry;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * A persistent (immutable) Adaptive Radix Tree for byte-key lookups with
 * prefix compression and structural sharing.
 * <p>
 * Keys are byte arrays. The tree supports efficient prefix scans,
 * critical for edge retrieval in graph workloads.
 *
 * @param <V> the value type
 */
public final class PersistentART<V> {

    private final ArtNode<V> root;
    private final int size;

    private PersistentART(ArtNode<V> root, int size) {
        this.root = root;
        this.size = size;
    }

    /**
     * Returns an empty PersistentART.
     */
    public static <V> PersistentART<V> empty() {
        return new PersistentART<>(null, 0);
    }

    /**
     * Returns a new ART with the given key-value mapping added or updated.
     */
    public PersistentART<V> put(byte[] key, V value) {
        Objects.requireNonNull(key, "key must not be null");
        if (root == null) {
            return new PersistentART<>(new Leaf<>(key.clone(), value), 1);
        }

        ArtNode<V> newRoot = root.put(key, 0, value);
        if (newRoot == root) {
            return this;
        }
        return new PersistentART<>(newRoot, newRoot.size());
    }

    /**
     * Returns the value associated with the given key, or null if not found.
     */
    public V get(byte[] key) {
        if (root == null) {
            return null;
        }
        return root.get(key, 0);
    }

    /**
     * Returns a new ART with the given key removed.
     */
    public PersistentART<V> remove(byte[] key) {
        if (root == null) {
            return this;
        }
        ArtNode<V> newRoot = root.remove(key, 0);
        if (newRoot == root) {
            return this;
        }
        if (newRoot == null) {
            return empty();
        }
        return new PersistentART<>(newRoot, newRoot.size());
    }

    /**
     * Returns the number of key-value pairs.
     */
    public int size() {
        return size;
    }

    /**
     * Applies the given action to each key-value pair.
     */
    public void forEach(BiConsumer<byte[], V> action) {
        if (root != null) {
            root.forEach(action);
        }
    }

    /**
     * Returns all entries whose keys start with the given prefix.
     */
    public List<Map.Entry<byte[], V>> prefixScan(byte[] prefix) {
        List<Map.Entry<byte[], V>> results = new ArrayList<>();
        if (root == null) {
            return results;
        }
        // Collect all entries and filter by prefix
        root.forEach((key, value) -> {
            if (startsWith(key, prefix)) {
                results.add(Map.entry(key, value));
            }
        });
        return results;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    /**
     * Computes the diff between this ART and another.
     */
    public List<DiffEntry<byte[], V>> diff(PersistentART<V> other) {
        if (this.root == other.root) {
            return List.of();
        }

        List<DiffEntry<byte[], V>> diffs = new ArrayList<>();

        // Collect entries from both trees, keyed by byte array wrapper
        Map<ByteArrayKey, V> thisEntries = new LinkedHashMap<>();
        this.forEach((k, v) -> thisEntries.put(new ByteArrayKey(k), v));

        Map<ByteArrayKey, V> otherEntries = new LinkedHashMap<>();
        other.forEach((k, v) -> otherEntries.put(new ByteArrayKey(k), v));

        // Find removed and modified
        for (var e : thisEntries.entrySet()) {
            ByteArrayKey bak = e.getKey();
            V thisVal = e.getValue();
            if (!otherEntries.containsKey(bak)) {
                diffs.add(new DiffEntry<>(bak.data(), thisVal, null, DiffEntry.ChangeType.REMOVED));
            } else {
                V otherVal = otherEntries.get(bak);
                if (!Objects.equals(thisVal, otherVal)) {
                    diffs.add(new DiffEntry<>(bak.data(), thisVal, otherVal, DiffEntry.ChangeType.MODIFIED));
                }
            }
        }

        // Find added
        for (var e : otherEntries.entrySet()) {
            if (!thisEntries.containsKey(e.getKey())) {
                diffs.add(new DiffEntry<>(e.getKey().data(), null, e.getValue(), DiffEntry.ChangeType.ADDED));
            }
        }

        return diffs;
    }

    /**
     * Wrapper for byte[] to use as map keys with proper equals/hashCode.
     */
    private record ByteArrayKey(byte[] data) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteArrayKey other)) return false;
            return Arrays.equals(data, other.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}
