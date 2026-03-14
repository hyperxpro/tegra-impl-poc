package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A HAMT node for hash collisions: multiple distinct keys sharing the same hash code.
 * Uses linear scan for operations.
 */
public final class CollisionNode<K, V> implements HamtNode<K, V> {

    private final int hash;
    private final List<Entry<K, V>> entries;

    record Entry<K, V>(K key, V value) {}

    CollisionNode(int hash, K key1, V val1, K key2, V val2) {
        this.hash = hash;
        this.entries = List.of(new Entry<>(key1, val1), new Entry<>(key2, val2));
    }

    private CollisionNode(int hash, List<Entry<K, V>> entries) {
        this.hash = hash;
        this.entries = entries;
    }

    @Override
    public HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx) {
        if (this.hash != hash) {
            // Different hash — create a BitmapIndexedNode containing this collision node and the new entry
            // Start with the new key, then merge this collision node back in
            HamtNode<K, V> result = BitmapIndexedNode.<K, V>single(key, value, hash, shift);
            for (Entry<K, V> e : entries) {
                result = result.put(e.key(), e.value(), this.hash, shift, ctx);
            }
            return result;
        }

        // Same hash — check for existing key
        for (int i = 0; i < entries.size(); i++) {
            Entry<K, V> e = entries.get(i);
            if (Objects.equals(e.key(), key)) {
                if (Objects.equals(e.value(), value)) {
                    return this;
                }
                List<Entry<K, V>> newEntries = new ArrayList<>(entries);
                newEntries.set(i, new Entry<>(key, value));
                return new CollisionNode<>(hash, List.copyOf(newEntries));
            }
        }

        // New key with same hash
        List<Entry<K, V>> newEntries = new ArrayList<>(entries);
        newEntries.add(new Entry<>(key, value));
        return new CollisionNode<>(hash, List.copyOf(newEntries));
    }

    @Override
    public HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx) {
        if (this.hash != hash) {
            return this;
        }

        for (int i = 0; i < entries.size(); i++) {
            Entry<K, V> e = entries.get(i);
            if (Objects.equals(e.key(), key)) {
                if (entries.size() == 2) {
                    // Collapse to a single-entry BitmapIndexedNode
                    Entry<K, V> remaining = entries.get(1 - i);
                    return BitmapIndexedNode.single(remaining.key(), remaining.value(),
                            Objects.hashCode(remaining.key()), shift);
                }
                List<Entry<K, V>> newEntries = new ArrayList<>(entries);
                newEntries.remove(i);
                return new CollisionNode<>(hash, List.copyOf(newEntries));
            }
        }
        return this;
    }

    @Override
    public V get(K key, int hash, int shift) {
        if (this.hash != hash) {
            return null;
        }
        for (Entry<K, V> e : entries) {
            if (Objects.equals(e.key(), key)) {
                return e.value();
            }
        }
        return null;
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        for (Entry<K, V> e : entries) {
            action.accept(e.key(), e.value());
        }
    }

    int collisionHash() {
        return hash;
    }
}
