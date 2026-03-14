package org.tegra.pds.art;

import java.util.function.BiConsumer;

/**
 * ART inner node with full 256-child direct-indexed array.
 *
 * @param <V> the value type
 */
public final class Node256<V> implements ArtNode<V> {

    @SuppressWarnings("unchecked")
    private final ArtNode<V>[] children; // 256 direct-indexed
    private final int count;
    private final byte[] prefix;
    private final int prefixLen;

    @SuppressWarnings("unchecked")
    Node256(ArtNode<V>[] children, int count, byte[] prefix, int prefixLen) {
        this.children = children;
        this.count = count;
        this.prefix = prefix;
        this.prefixLen = prefixLen;
    }

    private int checkPrefix(byte[] key, int depth) {
        int matched = 0;
        while (matched < prefixLen && depth + matched < key.length) {
            if (prefix[matched] != key[depth + matched]) {
                return matched;
            }
            matched++;
        }
        return matched;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ArtNode<V> put(byte[] key, int depth, V value) {
        int matched = checkPrefix(key, depth);
        if (matched < prefixLen) {
            return splitPrefix(key, depth, value, matched);
        }

        int newDepth = depth + prefixLen;
        if (newDepth >= key.length) {
            return this;
        }

        int idx = key[newDepth] & 0xFF;
        ArtNode<V> child = children[idx];

        if (child == null) {
            ArtNode<V>[] newChildren = children.clone();
            newChildren[idx] = new Leaf<>(key, value);
            return new Node256<>(newChildren, count + 1, prefix, prefixLen);
        }

        ArtNode<V> newChild = child.put(key, newDepth + 1, value);
        if (newChild == child) return this;

        ArtNode<V>[] newChildren = children.clone();
        newChildren[idx] = newChild;
        return new Node256<>(newChildren, count, prefix, prefixLen);
    }

    @SuppressWarnings("unchecked")
    private ArtNode<V> splitPrefix(byte[] key, int depth, V value, int matched) {
        byte[] newPrefix = new byte[matched];
        System.arraycopy(prefix, 0, newPrefix, 0, matched);

        Node4<V> newNode = Node4.empty(newPrefix, matched);

        byte existingByte = prefix[matched];
        int remainingLen = prefixLen - matched - 1;
        byte[] remainingPrefix = new byte[Math.max(remainingLen, 0)];
        if (remainingLen > 0) {
            System.arraycopy(prefix, matched + 1, remainingPrefix, 0, remainingLen);
        }
        Node256<V> existingChild = new Node256<>(children.clone(), count, remainingPrefix, remainingLen);
        newNode = newNode.addChild(existingByte, existingChild);

        int newDepth = depth + matched;
        if (newDepth < key.length) {
            newNode = newNode.addChild(key[newDepth], new Leaf<>(key, value));
        }

        return newNode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ArtNode<V> remove(byte[] key, int depth) {
        int matched = checkPrefix(key, depth);
        if (matched < prefixLen) {
            return this;
        }

        int newDepth = depth + prefixLen;
        if (newDepth >= key.length) {
            return this;
        }

        int idx = key[newDepth] & 0xFF;
        ArtNode<V> child = children[idx];
        if (child == null) {
            return this;
        }

        ArtNode<V> newChild = child.remove(key, newDepth + 1);
        if (newChild == child) return this;

        if (newChild == null) {
            int newCount = count - 1;
            if (newCount <= 48) {
                return shrinkToNode48(idx);
            }
            ArtNode<V>[] newChildren = children.clone();
            newChildren[idx] = null;
            return new Node256<>(newChildren, newCount, prefix, prefixLen);
        }

        ArtNode<V>[] newChildren = children.clone();
        newChildren[idx] = newChild;
        return new Node256<>(newChildren, count, prefix, prefixLen);
    }

    @SuppressWarnings("unchecked")
    private Node48<V> shrinkToNode48(int excludeIdx) {
        byte[] newIndex = new byte[256];
        java.util.Arrays.fill(newIndex, (byte) -1);
        ArtNode<V>[] newChildren = new ArtNode[48];
        int slot = 0;
        for (int i = 0; i < 256; i++) {
            if (i == excludeIdx) continue;
            if (children[i] != null) {
                newIndex[i] = (byte) slot;
                newChildren[slot] = children[i];
                slot++;
            }
        }
        return new Node48<>(newIndex, newChildren, slot, prefix, prefixLen);
    }

    @Override
    public V get(byte[] key, int depth) {
        int matched = checkPrefix(key, depth);
        if (matched < prefixLen) {
            return null;
        }

        int newDepth = depth + prefixLen;
        if (newDepth >= key.length) {
            return null;
        }

        int idx = key[newDepth] & 0xFF;
        ArtNode<V> child = children[idx];
        if (child == null) {
            return null;
        }
        return child.get(key, newDepth + 1);
    }

    @Override
    public int size() {
        int s = 0;
        for (ArtNode<V> child : children) {
            if (child != null) {
                s += child.size();
            }
        }
        return s;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        for (ArtNode<V> child : children) {
            if (child != null) {
                child.forEach(action);
            }
        }
    }

    byte[] prefix() { return prefix; }
    int prefixLen() { return prefixLen; }

    Node256<V> withPrefix(byte[] newPrefix, int newPrefixLen) {
        return new Node256<>(children.clone(), count, newPrefix, newPrefixLen);
    }
}
