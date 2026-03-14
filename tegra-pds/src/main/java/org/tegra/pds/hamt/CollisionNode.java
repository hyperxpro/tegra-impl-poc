package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Handles hash collisions in the HAMT via a linear list of key-value pairs.
 * All entries in a collision node have the same hash code.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class CollisionNode<K, V> implements HamtNode<K, V> {

    private final int hash;
    // Flat array: [k0, v0, k1, v1, ...]
    private final Object[] entries;

    CollisionNode(int hash, Object[] entries) {
        this.hash = hash;
        this.entries = entries;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key, int hash, int shift) {
        for (int i = 0; i < entries.length; i += 2) {
            if (Objects.equals(key, (K) entries[i])) {
                return (V) entries[i + 1];
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx) {
        if (hash != this.hash) {
            // Different hash — need to create a BitmapIndexedNode that branches
            // This collision node becomes a child, and we insert the new key separately
            HamtNode<K, V> bmNode = BitmapIndexedNode.single(key, value, hash, shift);
            // We need to re-insert all our collision entries into a common parent
            // Build a fresh bitmap node at this shift level that holds both
            return mergeCollisionWithEntry(key, value, hash, shift);
        }

        // Same hash — check for existing key
        for (int i = 0; i < entries.length; i += 2) {
            if (Objects.equals(key, (K) entries[i])) {
                if (Objects.equals(value, (V) entries[i + 1])) {
                    return this;
                }
                // Update existing
                Object[] newEntries = entries.clone();
                newEntries[i + 1] = value;
                return new CollisionNode<>(hash, newEntries);
            }
        }

        // Add new entry
        Object[] newEntries = new Object[entries.length + 2];
        System.arraycopy(entries, 0, newEntries, 0, entries.length);
        newEntries[entries.length] = key;
        newEntries[entries.length + 1] = value;
        return new CollisionNode<>(hash, newEntries);
    }

    @Override
    @SuppressWarnings("unchecked")
    public HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx) {
        int idx = -1;
        for (int i = 0; i < entries.length; i += 2) {
            if (Objects.equals(key, (K) entries[i])) {
                idx = i;
                break;
            }
        }

        if (idx < 0) {
            return this; // not found
        }

        int pairCount = entries.length / 2;
        if (pairCount == 1) {
            return EmptyNode.instance();
        }

        if (pairCount == 2) {
            // Collapse to a single-entry BitmapIndexedNode
            int otherIdx = (idx == 0) ? 2 : 0;
            K otherKey = (K) entries[otherIdx];
            V otherValue = (V) entries[otherIdx + 1];
            return BitmapIndexedNode.single(otherKey, otherValue, this.hash, shift);
        }

        Object[] newEntries = new Object[entries.length - 2];
        System.arraycopy(entries, 0, newEntries, 0, idx);
        System.arraycopy(entries, idx + 2, newEntries, idx, entries.length - idx - 2);
        return new CollisionNode<>(hash, newEntries);
    }

    @Override
    public int size() {
        return entries.length / 2;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void forEach(BiConsumer<K, V> action) {
        for (int i = 0; i < entries.length; i += 2) {
            action.accept((K) entries[i], (V) entries[i + 1]);
        }
    }

    /**
     * Returns the common hash for all entries in this collision node.
     */
    public int collisionHash() {
        return hash;
    }

    @SuppressWarnings("unchecked")
    private HamtNode<K, V> mergeCollisionWithEntry(K newKey, V newValue, int newHash, int shift) {
        // Start from empty and insert all collision entries plus the new entry
        HamtNode<K, V> result = EmptyNode.<K, V>instance();
        for (int i = 0; i < entries.length; i += 2) {
            K k = (K) entries[i];
            V v = (V) entries[i + 1];
            result = result.put(k, v, this.hash, shift, null);
        }
        result = result.put(newKey, newValue, newHash, shift, null);
        return result;
    }
}
