package org.tegra.pds.art;

import java.util.function.BiConsumer;

/**
 * ART inner node with up to 16 children, using sorted keys.
 *
 * @param <V> the value type
 */
public final class Node16<V> implements ArtNode<V> {

    private final byte[] keys;
    @SuppressWarnings("unchecked")
    private final ArtNode<V>[] children;
    private final int count;
    private final byte[] prefix;
    private final int prefixLen;

    @SuppressWarnings("unchecked")
    Node16(byte[] keys, ArtNode<V>[] children, int count, byte[] prefix, int prefixLen) {
        this.keys = keys;
        this.children = children;
        this.count = count;
        this.prefix = prefix;
        this.prefixLen = prefixLen;
    }

    private int findChild(byte keyByte) {
        int kb = keyByte & 0xFF;
        for (int i = 0; i < count; i++) {
            if ((keys[i] & 0xFF) == kb) return i;
        }
        return -1;
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
        int childIdx = findChild(keyByte);

        if (childIdx >= 0) {
            ArtNode<V> child = children[childIdx];
            ArtNode<V> newChild = child.put(key, newDepth + 1, value);
            if (newChild == child) return this;

            ArtNode<V>[] newChildren = children.clone();
            newChildren[childIdx] = newChild;
            return new Node16<>(keys.clone(), newChildren, count, prefix, prefixLen);
        }

        if (count < 16) {
            return addChild(keyByte, new Leaf<>(key, value));
        }

        // Upgrade to Node48
        return growToNode48().put(key, depth, value);
    }

    @SuppressWarnings("unchecked")
    private Node16<V> addChild(byte keyByte, ArtNode<V> child) {
        int pos = 0;
        int kb = keyByte & 0xFF;
        while (pos < count && (keys[pos] & 0xFF) < kb) {
            pos++;
        }

        byte[] newKeys = keys.clone();
        ArtNode<V>[] newChildren = children.clone();

        for (int i = count; i > pos; i--) {
            newKeys[i] = newKeys[i - 1];
            newChildren[i] = newChildren[i - 1];
        }
        newKeys[pos] = keyByte;
        newChildren[pos] = child;

        return new Node16<>(newKeys, newChildren, count + 1, prefix, prefixLen);
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
        Node16<V> existingChild = new Node16<>(keys.clone(), children.clone(), count, remainingPrefix, remainingLen);
        newNode = newNode.addChild(existingByte, existingChild);

        int newDepth = depth + matched;
        if (newDepth < key.length) {
            newNode = newNode.addChild(key[newDepth], new Leaf<>(key, value));
        }

        return newNode;
    }

    @SuppressWarnings("unchecked")
    private Node48<V> growToNode48() {
        byte[] index = new byte[256];
        java.util.Arrays.fill(index, (byte) -1);
        ArtNode<V>[] newChildren = new ArtNode[48];
        for (int i = 0; i < count; i++) {
            index[keys[i] & 0xFF] = (byte) i;
            newChildren[i] = children[i];
        }
        return new Node48<>(index, newChildren, count, prefix, prefixLen);
    }

    @Override
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
        int childIdx = findChild(keyByte);
        if (childIdx < 0) {
            return this;
        }

        ArtNode<V> child = children[childIdx];
        ArtNode<V> newChild = child.remove(key, newDepth + 1);

        if (newChild == child) return this;

        if (newChild == null) {
            if (count <= 5) {
                // Shrink to Node4
                return shrinkToNode4(childIdx);
            }
            return removeChild(childIdx);
        }

        ArtNode<V>[] newChildren = children.clone();
        newChildren[childIdx] = newChild;
        return new Node16<>(keys.clone(), newChildren, count, prefix, prefixLen);
    }

    @SuppressWarnings("unchecked")
    private Node4<V> shrinkToNode4(int excludeIdx) {
        byte[] newKeys = new byte[4];
        ArtNode<V>[] newChildren = new ArtNode[4];
        int j = 0;
        for (int i = 0; i < count; i++) {
            if (i != excludeIdx) {
                newKeys[j] = keys[i];
                newChildren[j] = children[i];
                j++;
            }
        }
        return new Node4<>(newKeys, newChildren, count - 1, prefix, prefixLen);
    }

    @SuppressWarnings("unchecked")
    private Node16<V> removeChild(int idx) {
        byte[] newKeys = new byte[16];
        ArtNode<V>[] newChildren = new ArtNode[16];
        int j = 0;
        for (int i = 0; i < count; i++) {
            if (i != idx) {
                newKeys[j] = keys[i];
                newChildren[j] = children[i];
                j++;
            }
        }
        return new Node16<>(newKeys, newChildren, count - 1, prefix, prefixLen);
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
        int childIdx = findChild(keyByte);
        if (childIdx < 0) {
            return null;
        }

        return children[childIdx].get(key, newDepth + 1);
    }

    @Override
    public int size() {
        int s = 0;
        for (int i = 0; i < count; i++) {
            s += children[i].size();
        }
        return s;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        for (int i = 0; i < count; i++) {
            children[i].forEach(action);
        }
    }

    byte[] prefix() { return prefix; }
    int prefixLen() { return prefixLen; }

    Node16<V> withPrefix(byte[] newPrefix, int newPrefixLen) {
        return new Node16<>(keys.clone(), children.clone(), count, newPrefix, newPrefixLen);
    }
}
