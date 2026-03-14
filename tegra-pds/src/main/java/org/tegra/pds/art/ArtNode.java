package org.tegra.pds.art;

import java.util.function.BiConsumer;

/**
 * Sealed interface for nodes in a persistent Adaptive Radix Tree (ART).
 * <p>
 * ART uses adaptive node sizes (4, 16, 48, 256) to balance memory usage
 * and lookup performance for byte-key indexed data.
 *
 * @param <V> the value type
 */
public sealed interface ArtNode<V>
        permits Node4, Node16, Node48, Node256, Leaf {

    /**
     * Inserts or updates a key-value pair.
     *
     * @param key   the full byte key
     * @param depth the current depth (byte position) in the key
     * @param value the value to associate
     * @return a new node containing the mapping
     */
    ArtNode<V> put(byte[] key, int depth, V value);

    /**
     * Removes a key.
     *
     * @param key   the full byte key
     * @param depth the current depth (byte position)
     * @return a new node without the mapping, or null if this node should be removed
     */
    ArtNode<V> remove(byte[] key, int depth);

    /**
     * Looks up the value for a key.
     *
     * @param key   the full byte key
     * @param depth the current depth (byte position)
     * @return the value, or null if not found
     */
    V get(byte[] key, int depth);

    /**
     * Returns the number of leaf entries in this subtree.
     */
    int size();

    /**
     * Applies the given action to each key-value pair in this subtree.
     */
    void forEach(BiConsumer<byte[], V> action);
}
