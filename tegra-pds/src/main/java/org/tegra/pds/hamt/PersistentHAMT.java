package org.tegra.pds.hamt;

import org.tegra.pds.common.ChangeType;
import org.tegra.pds.common.DiffEntry;
import org.tegra.pds.common.MutationContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Persistent Hash Array Mapped Trie (HAMT) — a persistent (immutable) hash map
 * with structural sharing via path-copying.
 * <p>
 * Each mutation (put/remove) returns a new HAMT instance that shares most of its
 * structure with the old one. This enables efficient versioning: old versions
 * remain valid and unchanged.
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
     * Returns an empty HAMT.
     */
    public static <K, V> PersistentHAMT<K, V> empty() {
        return new PersistentHAMT<>(EmptyNode.instance(), 0);
    }

    /**
     * Looks up the value for the given key.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V get(K key) {
        return root.get(key, key.hashCode(), 0);
    }

    /**
     * Returns a new HAMT with the given key-value pair inserted or updated.
     */
    public PersistentHAMT<K, V> put(K key, V value) {
        int hash = key.hashCode();
        HamtNode<K, V> newRoot = root.put(key, value, hash, 0, null);
        if (newRoot == root) {
            return this;
        }
        return new PersistentHAMT<>(newRoot, newRoot.size());
    }

    /**
     * Returns a new HAMT with the given key removed.
     */
    public PersistentHAMT<K, V> remove(K key) {
        int hash = key.hashCode();
        HamtNode<K, V> newRoot = root.remove(key, hash, 0, null);
        if (newRoot == root) {
            return this;
        }
        return new PersistentHAMT<>(newRoot, newRoot.size());
    }

    /**
     * Returns true if this HAMT contains the given key.
     */
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    /**
     * Returns the number of key-value pairs.
     */
    public int size() {
        return size;
    }

    /**
     * Returns true if empty.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Applies the given action to each key-value pair.
     */
    public void forEach(BiConsumer<K, V> action) {
        root.forEach(action);
    }

    /**
     * Computes the diff between this HAMT and another.
     * Returns a list of DiffEntry objects describing the changes
     * needed to go from {@code this} to {@code other}.
     */
    public List<DiffEntry<K, V>> diff(PersistentHAMT<K, V> other) {
        if (this.root == other.root) {
            return List.of();
        }

        List<DiffEntry<K, V>> diffs = new ArrayList<>();
        Map<K, V> thisMap = new HashMap<>();
        Map<K, V> otherMap = new HashMap<>();
        this.forEach(thisMap::put);
        other.forEach(otherMap::put);

        // Find removed and modified
        for (Map.Entry<K, V> entry : thisMap.entrySet()) {
            K key = entry.getKey();
            V thisValue = entry.getValue();
            V otherValue = otherMap.get(key);
            if (otherValue == null && !otherMap.containsKey(key)) {
                diffs.add(DiffEntry.removed(key, thisValue));
            } else if (thisValue != null && !thisValue.equals(otherValue)) {
                diffs.add(DiffEntry.modified(key, thisValue, otherValue));
            }
        }

        // Find added
        for (Map.Entry<K, V> entry : otherMap.entrySet()) {
            K key = entry.getKey();
            if (!thisMap.containsKey(key)) {
                diffs.add(DiffEntry.added(key, entry.getValue()));
            }
        }

        return diffs;
    }

    /**
     * Returns the root node. Package-private for serialization.
     */
    HamtNode<K, V> root() {
        return root;
    }
}
