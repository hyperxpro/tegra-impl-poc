package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.function.BiConsumer;

/**
 * Sealed interface for nodes in a Hash Array Mapped Trie (HAMT).
 * <p>
 * The HAMT uses 32-way branching with 5 bits of the hash per level.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public sealed interface HamtNode<K, V>
        permits BitmapIndexedNode, ArrayNode, CollisionNode, EmptyNode {

    /**
     * Associates the given key with the given value.
     *
     * @param key   the key
     * @param value the value
     * @param hash  the full hash of the key
     * @param shift the current bit shift (0, 5, 10, ...)
     * @param ctx   mutation context for transient optimization (may be null)
     * @return a new or modified node containing the mapping
     */
    HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx);

    /**
     * Removes the mapping for the given key.
     *
     * @param key   the key to remove
     * @param hash  the full hash of the key
     * @param shift the current bit shift
     * @param ctx   mutation context (may be null)
     * @return a new or modified node without the mapping
     */
    HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx);

    /**
     * Looks up the value associated with the given key.
     *
     * @param key   the key to look up
     * @param hash  the full hash of the key
     * @param shift the current bit shift
     * @return the value, or null if not found
     */
    V get(K key, int hash, int shift);

    /**
     * Returns the number of key-value pairs in this subtree.
     */
    int size();

    /**
     * Applies the given action to each key-value pair in this subtree.
     */
    void forEach(BiConsumer<K, V> action);
}
