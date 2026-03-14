package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * ART internal node with 5-16 children.
 * Uses sorted byte array for keys and binary search for lookups.
 * Grows to Node48 when full, shrinks to Node4 when small enough.
 *
 * @param <V> the value type
 */
public final class Node16<V> implements ArtNode<V> {

    static final int MAX_CHILDREN = 16;
    static final int MIN_CHILDREN = 5; // below this, shrink to Node4

    private final PrefixData prefix;
    private final byte[] keys;
    private final ArtNode<V>[] children;
    private final int numChildren;
    private final int cachedSize;

    @SuppressWarnings("unchecked")
    Node16(PrefixData prefix, byte[] keys, ArtNode<V>[] children, int numChildren, int cachedSize) {
        this.prefix = prefix;
        this.keys = keys;
        this.children = children;
        this.numChildren = numChildren;
        this.cachedSize = cachedSize;
    }

    @SuppressWarnings("unchecked")
    static <V> Node16<V> fromNode4(PrefixData prefix, byte[] n4Keys, ArtNode<V>[] n4Children, int n4Count, int cachedSize) {
        byte[] newKeys = new byte[MAX_CHILDREN];
        ArtNode<V>[] newChildren = new ArtNode[MAX_CHILDREN];
        System.arraycopy(n4Keys, 0, newKeys, 0, n4Count);
        System.arraycopy(n4Children, 0, newChildren, 0, n4Count);
        return new Node16<>(prefix, newKeys, newChildren, n4Count, cachedSize);
    }

    private int findChildIndex(byte key) {
        // Binary search on sorted keys
        int keyUnsigned = key & 0xFF;
        int lo = 0, hi = numChildren - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int midVal = keys[mid] & 0xFF;
            if (midVal < keyUnsigned) {
                lo = mid + 1;
            } else if (midVal > keyUnsigned) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private int findInsertionPoint(byte key) {
        int keyUnsigned = key & 0xFF;
        int pos = 0;
        while (pos < numChildren && (keys[pos] & 0xFF) < keyUnsigned) {
            pos++;
        }
        return pos;
    }

    @Override
    public ArtNode<V> findChild(byte key) {
        int idx = findChildIndex(key);
        return (idx >= 0) ? children[idx] : null;
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

        byte nextByte = key[depth];
        int childIdx = findChildIndex(nextByte);

        if (childIdx >= 0) {
            ArtNode<V> oldChild = children[childIdx];
            ArtNode<V> newChild = oldChild.insert(key, depth + 1, value);
            if (newChild == oldChild) {
                return this;
            }
            return copyAndSetChild(childIdx, newChild, cachedSize + (newChild.size() - oldChild.size()));
        }

        ArtNode<V> newChild = new Leaf<>(key, value);

        if (numChildren < MAX_CHILDREN) {
            return addChild(nextByte, newChild);
        }

        // Grow to Node48
        return growToNode48().insert(key, depth - prefix.length(), value);
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

        Node16<V> existingSubtree = new Node16<>(remainingPrefix, keys.clone(), children.clone(), numChildren, cachedSize);

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

        byte nextByte = key[depth];
        int childIdx = findChildIndex(nextByte);

        if (childIdx < 0) {
            return this;
        }

        ArtNode<V> oldChild = children[childIdx];
        ArtNode<V> newChild = oldChild.remove(key, depth + 1);

        if (newChild == oldChild) {
            return this;
        }

        if (newChild == null) {
            return removeChildAt(childIdx);
        }

        return copyAndSetChild(childIdx, newChild, cachedSize + (newChild.size() - oldChild.size()));
    }

    private ArtNode<V> removeChildAt(int childIdx) {
        int removedSize = children[childIdx].size();
        int newCount = numChildren - 1;

        if (newCount < MIN_CHILDREN) {
            return shrinkToNode4(childIdx, cachedSize - removedSize);
        }

        byte[] newKeys = new byte[MAX_CHILDREN];
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = new ArtNode[MAX_CHILDREN];
        int j = 0;
        for (int i = 0; i < numChildren; i++) {
            if (i != childIdx) {
                newKeys[j] = keys[i];
                newChildren[j] = children[i];
                j++;
            }
        }

        return new Node16<>(prefix, newKeys, newChildren, newCount, cachedSize - removedSize);
    }

    @SuppressWarnings("unchecked")
    private Node4<V> shrinkToNode4(int skipIdx, int newCachedSize) {
        byte[] n4Keys = new byte[Node4.MAX_CHILDREN];
        ArtNode<V>[] n4Children = new ArtNode[Node4.MAX_CHILDREN];
        int j = 0;
        for (int i = 0; i < numChildren; i++) {
            if (i != skipIdx) {
                n4Keys[j] = keys[i];
                n4Children[j] = children[i];
                j++;
            }
        }
        // If only one child remains after removal, collapse
        if (j == 1) {
            return new Node4<V>(prefix, n4Keys, n4Children, 1, newCachedSize);
        }
        return new Node4<V>(prefix, n4Keys, n4Children, j, newCachedSize);
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

        byte nextByte = key[depth];
        ArtNode<V> child = findChild(nextByte);
        if (child == null) {
            return null;
        }
        return child.lookup(key, depth + 1);
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        for (int i = 0; i < numChildren; i++) {
            children[i].forEach(action);
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

    int numChildren() {
        return numChildren;
    }

    private Node16<V> addChild(byte key, ArtNode<V> child) {
        byte[] newKeys = keys.clone();
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();

        int pos = findInsertionPoint(key);

        // Shift right
        for (int i = numChildren; i > pos; i--) {
            newKeys[i] = newKeys[i - 1];
            newChildren[i] = newChildren[i - 1];
        }

        newKeys[pos] = key;
        newChildren[pos] = child;

        return new Node16<>(prefix, newKeys, newChildren, numChildren + 1, cachedSize + child.size());
    }

    private Node16<V> copyAndSetChild(int idx, ArtNode<V> newChild, int newCachedSize) {
        byte[] newKeys = keys.clone();
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();
        newChildren[idx] = newChild;
        return new Node16<>(prefix, newKeys, newChildren, numChildren, newCachedSize);
    }

    Node48<V> growToNode48() {
        return Node48.fromNode16(prefix, keys, children, numChildren, cachedSize);
    }

    Node16<V> withPrefix(PrefixData newPrefix) {
        return new Node16<>(newPrefix, keys.clone(), children.clone(), numChildren, cachedSize);
    }
}
