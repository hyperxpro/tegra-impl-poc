package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * Terminal ART node storing the full key and its associated value.
 *
 * @param <V> the value type
 */
public record Leaf<V>(byte[] key, V value) implements ArtNode<V> {

    @Override
    public ArtNode<V> put(byte[] key, int depth, V value) {
        if (Arrays.equals(this.key, key)) {
            // Same key — update value
            if (this.value == value || (this.value != null && this.value.equals(value))) {
                return this;
            }
            return new Leaf<>(key, value);
        }

        // Different key — need to create an inner node
        // Find the common prefix length starting from depth
        int commonPrefix = 0;
        int maxLen = Math.min(this.key.length, key.length);
        while (depth + commonPrefix < maxLen
                && this.key[depth + commonPrefix] == key[depth + commonPrefix]) {
            commonPrefix++;
        }

        // Create a Node4 with the common prefix, containing both leaves
        byte[] prefix = new byte[commonPrefix];
        if (commonPrefix > 0) {
            System.arraycopy(this.key, depth, prefix, 0, commonPrefix);
        }

        Node4<V> node = Node4.empty(prefix, commonPrefix);

        // If one key is a prefix of the other, we need special handling
        int newDepth = depth + commonPrefix;
        if (newDepth < this.key.length && newDepth < key.length) {
            byte existingByte = this.key[newDepth];
            byte newByte = key[newDepth];
            node = node.addChild(existingByte, this);
            node = node.addChild(newByte, new Leaf<>(key, value));
        } else if (newDepth >= this.key.length && newDepth >= key.length) {
            // Keys are equal — should not happen since we checked Arrays.equals above
            return new Leaf<>(key, value);
        } else if (newDepth >= this.key.length) {
            // Existing key is a prefix of new key
            // Store existing as a child at a special "end-of-key" sentinel
            // We'll use the approach of requiring keys to be distinct at each byte
            // In practice, for ART we handle this by storing the leaf at a special position
            node = node.addChild((byte) 0, this); // sentinel
            node = node.addChild(key[newDepth], new Leaf<>(key, value));
        } else {
            // New key is a prefix of existing key
            node = node.addChild(this.key[newDepth], this);
            node = node.addChild((byte) 0, new Leaf<>(key, value)); // sentinel
        }

        return node;
    }

    @Override
    public ArtNode<V> remove(byte[] key, int depth) {
        if (Arrays.equals(this.key, key)) {
            return null; // remove this leaf
        }
        return this; // key not found
    }

    @Override
    public V get(byte[] key, int depth) {
        if (Arrays.equals(this.key, key)) {
            return value;
        }
        return null;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        action.accept(key, value);
    }

    /**
     * Checks if this leaf's key matches the given key.
     */
    boolean keyEquals(byte[] other) {
        return Arrays.equals(key, other);
    }

    /**
     * Checks if this leaf's key starts with the given prefix.
     */
    boolean keyStartsWith(byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }
}
