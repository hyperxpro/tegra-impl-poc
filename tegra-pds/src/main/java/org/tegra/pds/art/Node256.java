package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * ART internal node with 49-256 children.
 * Uses a direct 256-slot array for O(1) lookups.
 * Shrinks to Node48 when children drop below threshold.
 *
 * @param <V> the value type
 */
public final class Node256<V> implements ArtNode<V> {

    static final int MIN_CHILDREN = 49; // below this, shrink to Node48

    private final PrefixData prefix;
    private final ArtNode<V>[] children; // 256 slots, null if empty
    private final int numChildren;
    private final int cachedSize;

    @SuppressWarnings("unchecked")
    Node256(PrefixData prefix, ArtNode<V>[] children, int numChildren, int cachedSize) {
        this.prefix = prefix;
        this.children = children;
        this.numChildren = numChildren;
        this.cachedSize = cachedSize;
    }

    @SuppressWarnings("unchecked")
    static <V> Node256<V> fromNode48(PrefixData prefix, byte[] n48ChildIndex, ArtNode<V>[] n48Children,
                                      int n48Count, int cachedSize) {
        ArtNode<V>[] children = new ArtNode[256];
        for (int i = 0; i < 256; i++) {
            int slot = n48ChildIndex[i] & 0xFF;
            if (slot != 0xFF) {
                children[i] = n48Children[slot];
            }
        }
        return new Node256<>(prefix, children, n48Count, cachedSize);
    }

    @Override
    public ArtNode<V> insert(byte[] key, int depth, V value) {
        int prefixMatchLen = prefix.matchPrefix(key, depth);

        if (prefixMatchLen < prefix.length()) {
            return splitAndInsert(key, depth, value, prefixMatchLen);
        }

        depth += prefix.length();

        if (depth >= key.length) {
            return this;
        }

        int nextByte = key[depth] & 0xFF;
        ArtNode<V> child = children[nextByte];

        if (child != null) {
            ArtNode<V> newChild = child.insert(key, depth + 1, value);
            if (newChild == child) {
                return this;
            }
            return copyAndSetChild(nextByte, newChild, cachedSize + (newChild.size() - child.size()));
        }

        // New child
        ArtNode<V> newChild = new Leaf<>(key, value);
        return copyAndSetChild(nextByte, newChild, cachedSize + 1, numChildren + 1);
    }

    private ArtNode<V> splitAndInsert(byte[] key, int depth, V value, int prefixMatchLen) {
        PrefixData sharedPrefix = PrefixData.fromKey(prefix.prefix(), 0, prefixMatchLen);
        byte splitByte = prefix.at(prefixMatchLen);

        int remainLen = prefix.length() - prefixMatchLen - 1;
        PrefixData remainingPrefix;
        if (remainLen > 0) {
            int storeLen = Math.min(remainLen, PrefixData.MAX_PREFIX_LENGTH);
            byte[] remainBytes = new byte[storeLen];
            int copyLen = Math.min(storeLen, prefix.prefix().length - prefixMatchLen - 1);
            if (copyLen > 0) {
                System.arraycopy(prefix.prefix(), prefixMatchLen + 1, remainBytes, 0, copyLen);
            }
            remainingPrefix = new PrefixData(remainBytes, remainLen);
        } else {
            remainingPrefix = PrefixData.EMPTY;
        }

        Node256<V> existingSubtree = new Node256<>(remainingPrefix, children.clone(), numChildren, cachedSize);

        Node4<V> newParent = new Node4<>(sharedPrefix);
        newParent = newParent.addChildDirect(splitByte, existingSubtree);

        int newDepth = depth + prefixMatchLen;
        if (newDepth < key.length) {
            byte newByte = key[newDepth];
            newParent = newParent.addChildDirect(newByte, new Leaf<>(key, value));
        }

        return newParent;
    }

    @Override
    public ArtNode<V> remove(byte[] key, int depth) {
        int prefixMatchLen = prefix.matchPrefix(key, depth);
        if (prefixMatchLen < prefix.length()) {
            return this;
        }

        depth += prefix.length();

        if (depth >= key.length) {
            return this;
        }

        int nextByte = key[depth] & 0xFF;
        ArtNode<V> child = children[nextByte];

        if (child == null) {
            return this;
        }

        ArtNode<V> newChild = child.remove(key, depth + 1);

        if (newChild == child) {
            return this;
        }

        if (newChild == null) {
            return removeChild(nextByte);
        }

        return copyAndSetChild(nextByte, newChild, cachedSize + (newChild.size() - child.size()));
    }

    private ArtNode<V> removeChild(int byteIndex) {
        int removedSize = children[byteIndex].size();
        int newCount = numChildren - 1;

        if (newCount < MIN_CHILDREN) {
            return shrinkToNode48(byteIndex, cachedSize - removedSize);
        }

        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();
        newChildren[byteIndex] = null;

        return new Node256<>(prefix, newChildren, newCount, cachedSize - removedSize);
    }

    @SuppressWarnings("unchecked")
    private Node48<V> shrinkToNode48(int skipByteIndex, int newCachedSize) {
        byte[] n48ChildIndex = new byte[256];
        Arrays.fill(n48ChildIndex, Node48.EMPTY_SLOT);
        ArtNode<V>[] n48Children = new ArtNode[Node48.MAX_CHILDREN];
        int slot = 0;

        for (int i = 0; i < 256; i++) {
            if (i == skipByteIndex) continue;
            if (children[i] != null) {
                n48ChildIndex[i] = (byte) slot;
                n48Children[slot] = children[i];
                slot++;
            }
        }

        return new Node48<>(prefix, n48ChildIndex, n48Children, slot, newCachedSize);
    }

    @Override
    public V lookup(byte[] key, int depth) {
        int prefixMatchLen = prefix.matchPrefix(key, depth);
        if (prefixMatchLen < prefix.length()) {
            return null;
        }

        depth += prefix.length();

        if (depth >= key.length) {
            return null;
        }

        int nextByte = key[depth] & 0xFF;
        ArtNode<V> child = children[nextByte];
        if (child == null) {
            return null;
        }
        return child.lookup(key, depth + 1);
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        for (int i = 0; i < 256; i++) {
            if (children[i] != null) {
                children[i].forEach(action);
            }
        }
    }

    @Override
    public int size() {
        return cachedSize;
    }

    @Override
    public PrefixData prefix() {
        return prefix;
    }

    @Override
    public ArtNode<V> findChild(byte key) {
        return children[key & 0xFF];
    }

    int numChildren() {
        return numChildren;
    }

    private Node256<V> copyAndSetChild(int byteIndex, ArtNode<V> newChild, int newCachedSize) {
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();
        newChildren[byteIndex] = newChild;
        return new Node256<>(prefix, newChildren, numChildren, newCachedSize);
    }

    private Node256<V> copyAndSetChild(int byteIndex, ArtNode<V> newChild, int newCachedSize, int newNumChildren) {
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();
        newChildren[byteIndex] = newChild;
        return new Node256<>(prefix, newChildren, newNumChildren, newCachedSize);
    }

    Node256<V> withPrefix(PrefixData newPrefix) {
        return new Node256<>(newPrefix, children.clone(), numChildren, cachedSize);
    }
}
