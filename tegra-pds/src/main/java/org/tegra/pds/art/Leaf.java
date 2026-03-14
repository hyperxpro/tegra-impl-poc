package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * Leaf node in the ART. Stores the full key and its associated value.
 *
 * @param <V> the value type
 */
public final class Leaf<V> implements ArtNode<V> {

    private final byte[] key;
    private final V value;

    public Leaf(byte[] key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the full key stored in this leaf.
     */
    public byte[] key() {
        return key;
    }

    /**
     * Returns the value stored in this leaf.
     */
    public V value() {
        return value;
    }

    /**
     * Checks if this leaf's key matches the given key.
     */
    public boolean keyEquals(byte[] other) {
        return Arrays.equals(key, other);
    }

    /**
     * Returns the number of prefix bytes in common between this leaf's key and the given key,
     * starting at the given depth.
     */
    public int longestCommonPrefix(byte[] otherKey, int depth) {
        int maxLen = Math.min(key.length, otherKey.length) - depth;
        int matched = 0;
        for (int i = 0; i < maxLen; i++) {
            if (key[depth + i] != otherKey[depth + i]) {
                break;
            }
            matched++;
        }
        return matched;
    }

    @Override
    public ArtNode<V> insert(byte[] newKey, int depth, V newValue) {
        if (keyEquals(newKey)) {
            // Update existing leaf
            return new Leaf<>(newKey, newValue);
        }

        // Need to create a new internal node that distinguishes the two keys
        // Find the longest common prefix from the current depth
        int lcp = longestCommonPrefix(newKey, depth);

        // Create a new Node4 with the shared prefix
        PrefixData prefix = PrefixData.fromKey(key, depth, depth + lcp);
        int newDepth = depth + lcp;

        // Since PersistentART always appends a terminator byte, keys will never
        // be exhausted at an internal node. But guard defensively.
        byte existingByte = key[newDepth];
        byte newByte = newKey[newDepth];

        Node4<V> node = new Node4<>(prefix);
        node = node.addChildDirect(existingByte, this);
        node = node.addChildDirect(newByte, new Leaf<>(newKey, newValue));
        return node;
    }

    @Override
    public ArtNode<V> remove(byte[] removeKey, int depth) {
        if (keyEquals(removeKey)) {
            return null; // removed
        }
        return this; // not our key, no change
    }

    @Override
    public V lookup(byte[] lookupKey, int depth) {
        if (keyEquals(lookupKey)) {
            return value;
        }
        return null;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        action.accept(key, value);
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public PrefixData prefix() {
        return null;
    }

    @Override
    public ArtNode<V> findChild(byte key) {
        return null;
    }
}
