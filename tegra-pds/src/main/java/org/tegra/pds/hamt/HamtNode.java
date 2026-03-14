package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.function.BiConsumer;

/**
 * Sealed interface for Hash Array Mapped Trie (HAMT) nodes.
 * <p>
 * Uses 32-way branching with 5-bit hash fragments at each level.
 * Persistence is achieved via path-copying: modifications create new nodes
 * along the root-to-leaf path, sharing unchanged subtrees.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public sealed interface HamtNode<K, V>
        permits EmptyNode, BitmapIndexedNode, CollisionNode, ArrayNode {

    /**
     * Looks up a value by key.
     *
     * @param key   the key to look up
     * @param hash  the full hash of the key
     * @param shift the current bit offset (0, 5, 10, ...)
     * @return the value, or null if not found
     */
    V get(K key, int hash, int shift);

    /**
     * Inserts or updates a key-value pair, returning a new node (path-copy).
     *
     * @param key   the key
     * @param value the value
     * @param hash  the full hash of the key
     * @param shift the current bit offset
     * @param ctx   optional mutation context for transient optimization (may be null)
     * @return the new or modified node
     */
    HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx);

    /**
     * Removes a key, returning a new node (path-copy).
     *
     * @param key   the key to remove
     * @param hash  the full hash of the key
     * @param shift the current bit offset
     * @param ctx   optional mutation context (may be null)
     * @return the new node, or the empty node if this node becomes empty
     */
    HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx);

    /**
     * Returns the number of key-value entries in this subtree.
     */
    int size();

    /**
     * Applies the given action to each key-value pair in this subtree.
     */
    void forEach(BiConsumer<K, V> action);
}
