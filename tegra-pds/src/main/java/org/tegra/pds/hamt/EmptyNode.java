package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.function.BiConsumer;

/**
 * Singleton empty node representing an empty HAMT.
 */
public final class EmptyNode<K, V> implements HamtNode<K, V> {

    @SuppressWarnings("rawtypes")
    private static final EmptyNode INSTANCE = new EmptyNode();

    private EmptyNode() {
    }

    @SuppressWarnings("unchecked")
    public static <K, V> EmptyNode<K, V> instance() {
        return (EmptyNode<K, V>) INSTANCE;
    }

    @Override
    public HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx) {
        return BitmapIndexedNode.single(key, value, hash, shift);
    }

    @Override
    public HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx) {
        return this;
    }

    @Override
    public V get(K key, int hash, int shift) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        // no-op
    }
}
