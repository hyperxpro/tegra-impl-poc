package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * ART inner node with up to 48 children, using a 256-byte index array
 * that maps key bytes to child slots.
 *
 * @param <V> the value type
 */
public final class Node48<V> implements ArtNode<V> {

    private final byte[] index; // 256 entries, maps key byte -> child slot (or -1)
    @SuppressWarnings("unchecked")
    private final ArtNode<V>[] children; // up to 48 children
    private final int count;
    private final byte[] prefix;
    private final int prefixLen;

    @SuppressWarnings("unchecked")
    Node48(byte[] index, ArtNode<V>[] children, int count, byte[] prefix, int prefixLen) {
        this.index = index;
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

        byte keyByte = key[newDepth];
        int slot = index[keyByte & 0xFF];

        if (slot != -1 && (slot & 0xFF) < 48) {
            int s = slot & 0xFF;
            ArtNode<V> child = children[s];
            if (child != null) {
                ArtNode<V> newChild = child.put(key, newDepth + 1, value);
                if (newChild == child) return this;

                ArtNode<V>[] newChildren = children.clone();
                newChildren[s] = newChild;
                return new Node48<>(index.clone(), newChildren, count, prefix, prefixLen);
            }
        }

        if (count < 48) {
            // Find a free slot
            int freeSlot = -1;
            for (int i = 0; i < 48; i++) {
                if (children[i] == null) {
                    freeSlot = i;
                    break;
                }
            }
            byte[] newIndex = index.clone();
            ArtNode<V>[] newChildren = children.clone();
            newIndex[keyByte & 0xFF] = (byte) freeSlot;
            newChildren[freeSlot] = new Leaf<>(key, value);
            return new Node48<>(newIndex, newChildren, count + 1, prefix, prefixLen);
        }

        // Upgrade to Node256
        return growToNode256().put(key, depth, value);
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
        Node48<V> existingChild = new Node48<>(index.clone(), children.clone(), count, remainingPrefix, remainingLen);
        newNode = newNode.addChild(existingByte, existingChild);

        int newDepth = depth + matched;
        if (newDepth < key.length) {
            newNode = newNode.addChild(key[newDepth], new Leaf<>(key, value));
        }

        return newNode;
    }

    @SuppressWarnings("unchecked")
    private Node256<V> growToNode256() {
        ArtNode<V>[] newChildren = new ArtNode[256];
        for (int i = 0; i < 256; i++) {
            int slot = index[i] & 0xFF;
            if (index[i] != -1 && slot < 48 && children[slot] != null) {
                newChildren[i] = children[slot];
            }
        }
        return new Node256<>(newChildren, count, prefix, prefixLen);
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

        byte keyByte = key[newDepth];
        int slot = index[keyByte & 0xFF];
        if (slot == -1 || (slot & 0xFF) >= 48) {
            return this;
        }
        int s = slot & 0xFF;
        ArtNode<V> child = children[s];
        if (child == null) {
            return this;
        }

        ArtNode<V> newChild = child.remove(key, newDepth + 1);
        if (newChild == child) return this;

        if (newChild == null) {
            int newCount = count - 1;
            if (newCount <= 16) {
                return shrinkToNode16(keyByte);
            }
            byte[] newIndex = index.clone();
            ArtNode<V>[] newChildren = children.clone();
            newIndex[keyByte & 0xFF] = -1;
            newChildren[s] = null;
            return new Node48<>(newIndex, newChildren, newCount, prefix, prefixLen);
        }

        ArtNode<V>[] newChildren = children.clone();
        newChildren[s] = newChild;
        return new Node48<>(index.clone(), newChildren, count, prefix, prefixLen);
    }

    @SuppressWarnings("unchecked")
    private Node16<V> shrinkToNode16(byte excludeKeyByte) {
        byte[] newKeys = new byte[16];
        ArtNode<V>[] newChildren = new ArtNode[16];
        int j = 0;
        // Iterate in key-byte order to maintain sorted keys
        for (int i = 0; i < 256; i++) {
            if (i == (excludeKeyByte & 0xFF)) continue;
            int slot = index[i];
            if (slot != -1 && (slot & 0xFF) < 48) {
                int s = slot & 0xFF;
                if (children[s] != null) {
                    newKeys[j] = (byte) i;
                    newChildren[j] = children[s];
                    j++;
                }
            }
        }
        return new Node16<>(newKeys, newChildren, j, prefix, prefixLen);
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

        byte keyByte = key[newDepth];
        int slot = index[keyByte & 0xFF];
        if (slot == -1 || (slot & 0xFF) >= 48) {
            return null;
        }
        int s = slot & 0xFF;
        ArtNode<V> child = children[s];
        if (child == null) {
            return null;
        }
        return child.get(key, newDepth + 1);
    }

    @Override
    public int size() {
        int s = 0;
        for (int i = 0; i < 48; i++) {
            if (children[i] != null) {
                s += children[i].size();
            }
        }
        return s;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        // Iterate in key-byte order
        for (int i = 0; i < 256; i++) {
            int slot = index[i];
            if (slot != -1 && (slot & 0xFF) < 48) {
                int s = slot & 0xFF;
                if (children[s] != null) {
                    children[s].forEach(action);
                }
            }
        }
    }

    byte[] prefix() { return prefix; }
    int prefixLen() { return prefixLen; }

    Node48<V> withPrefix(byte[] newPrefix, int newPrefixLen) {
        return new Node48<>(index.clone(), children.clone(), count, newPrefix, newPrefixLen);
    }
}
