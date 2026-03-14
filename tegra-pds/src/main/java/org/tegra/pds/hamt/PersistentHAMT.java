package org.tegra.pds.hamt;

import org.tegra.pds.common.DiffEntry;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * A persistent (immutable) Hash Array Mapped Trie providing efficient
 * structural sharing between versions.
 * <p>
 * All mutation operations return new instances; the original is never modified.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class PersistentHAMT<K, V> {

    private final HamtNode<K, V> root;
    private final int size;

    PersistentHAMT(HamtNode<K, V> root, int size) {
        this.root = root;
        this.size = size;
    }

    /**
     * Returns an empty PersistentHAMT.
     */
    public static <K, V> PersistentHAMT<K, V> empty() {
        return new PersistentHAMT<>(EmptyNode.instance(), 0);
    }

    /**
     * Returns a new HAMT with the given key-value mapping added or updated.
     */
    public PersistentHAMT<K, V> put(K key, V value) {
        int hash = Objects.hashCode(key);
        HamtNode<K, V> newRoot = root.put(key, value, hash, 0, null);
        if (newRoot == root) {
            return this;
        }
        int newSize = newRoot.size();
        return new PersistentHAMT<>(newRoot, newSize);
    }

    /**
     * Returns the value associated with the given key, or null if not found.
     */
    public V get(K key) {
        int hash = Objects.hashCode(key);
        return root.get(key, hash, 0);
    }

    /**
     * Returns a new HAMT with the given key removed.
     */
    public PersistentHAMT<K, V> remove(K key) {
        int hash = Objects.hashCode(key);
        HamtNode<K, V> newRoot = root.remove(key, hash, 0, null);
        if (newRoot == root) {
            return this;
        }
        int newSize = newRoot.size();
        return new PersistentHAMT<>(newRoot, newSize);
    }

    /**
     * Returns the number of key-value pairs.
     */
    public int size() {
        return size;
    }

    /**
     * Returns true if this map contains a mapping for the given key.
     */
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    /**
     * Applies the given action to each key-value pair.
     */
    public void forEach(BiConsumer<K, V> action) {
        root.forEach(action);
    }

    /**
     * Returns a transient (mutable) version for batch operations.
     */
    public TransientHAMT<K, V> asTransient() {
        return new TransientHAMT<>(root, size);
    }

    /**
     * Computes the diff between this HAMT and another.
     * Returns entries that differ: ADDED (in other but not this),
     * REMOVED (in this but not other), MODIFIED (different values).
     */
    public List<DiffEntry<K, V>> diff(PersistentHAMT<K, V> other) {
        if (this.root == other.root) {
            return List.of();
        }

        List<DiffEntry<K, V>> diffs = new ArrayList<>();

        // Collect all entries from both
        Map<K, V> thisEntries = new LinkedHashMap<>();
        this.forEach(thisEntries::put);

        Map<K, V> otherEntries = new LinkedHashMap<>();
        other.forEach(otherEntries::put);

        // Find removed and modified
        for (Map.Entry<K, V> e : thisEntries.entrySet()) {
            K key = e.getKey();
            V thisVal = e.getValue();
            if (!otherEntries.containsKey(key)) {
                diffs.add(new DiffEntry<>(key, thisVal, null, DiffEntry.ChangeType.REMOVED));
            } else {
                V otherVal = otherEntries.get(key);
                if (!Objects.equals(thisVal, otherVal)) {
                    diffs.add(new DiffEntry<>(key, thisVal, otherVal, DiffEntry.ChangeType.MODIFIED));
                }
            }
        }

        // Find added
        for (Map.Entry<K, V> e : otherEntries.entrySet()) {
            if (!thisEntries.containsKey(e.getKey())) {
                diffs.add(new DiffEntry<>(e.getKey(), null, e.getValue(), DiffEntry.ChangeType.ADDED));
            }
        }

        return diffs;
    }

    // Package-private access to root for TransientHAMT
    HamtNode<K, V> root() {
        return root;
    }
}
