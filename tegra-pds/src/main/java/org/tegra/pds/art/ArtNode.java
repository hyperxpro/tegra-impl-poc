package org.tegra.pds.art;

import java.util.function.BiConsumer;

/**
 * Sealed interface for Adaptive Radix Tree (ART) nodes.
 * <p>
 * ART adaptively selects among four internal node types based on the number of children,
 * providing both memory efficiency and fast lookups. Persistence is achieved via path-copying.
 *
 * @param <V> the value type (keys are always byte arrays)
 */
public sealed interface ArtNode<V>
        permits Node4, Node16, Node48, Node256, Leaf {

    /**
     * Inserts a key-value pair, returning a new tree root via path-copying.
     *
     * @param key   the full key
     * @param depth the current depth (byte index into key)
     * @param value the value to insert
     * @return the new root node
     */
    ArtNode<V> insert(byte[] key, int depth, V value);

    /**
     * Removes a key, returning a new tree root via path-copying.
     *
     * @param key   the full key
     * @param depth the current depth
     * @return the new root node, or null if the tree becomes empty
     */
    ArtNode<V> remove(byte[] key, int depth);

    /**
     * Looks up a value by key.
     *
     * @param key   the full key
     * @param depth the current depth
     * @return the value, or null if not found
     */
    V lookup(byte[] key, int depth);

    /**
     * Applies the given action to each key-value pair in this subtree.
     *
     * @param action consumer of (key, value) pairs
     */
    void forEach(BiConsumer<byte[], V> action);

    /**
     * Returns the number of entries (leaves) in this subtree.
     */
    int size();

    /**
     * Returns the prefix data for this node, or null for leaf nodes.
     */
    PrefixData prefix();

    /**
     * Finds the child node for the given key byte, or null if no such child exists.
     * For leaf nodes, always returns null.
     */
    ArtNode<V> findChild(byte key);
}
