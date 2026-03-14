package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * ART internal node with 17-48 children.
 * Uses a 256-byte index array mapping each possible key byte to a child slot index.
 * Empty slots are marked with -1 (0xFF as byte).
 * Grows to Node256 when full, shrinks to Node16 when small enough.
 *
 * @param <V> the value type
 */
public final class Node48<V> implements ArtNode<V> {

    static final int MAX_CHILDREN = 48;
    static final int MIN_CHILDREN = 17; // below this, shrink to Node16
    static final byte EMPTY_SLOT = (byte) 0xFF;

    private final PrefixData prefix;
    // Maps key byte (0-255) to child slot index (0-47), or EMPTY_SLOT if no child
    private final byte[] childIndex;
    private final ArtNode<V>[] children;
    private final int numChildren;
    private final int cachedSize;

    @SuppressWarnings("unchecked")
    Node48(PrefixData prefix, byte[] childIndex, ArtNode<V>[] children, int numChildren, int cachedSize) {
        this.prefix = prefix;
        this.childIndex = childIndex;
        this.children = children;
        this.numChildren = numChildren;
        this.cachedSize = cachedSize;
    }

    @SuppressWarnings("unchecked")
    static <V> Node48<V> fromNode16(PrefixData prefix, byte[] n16Keys, ArtNode<V>[] n16Children, int n16Count, int cachedSize) {
        byte[] childIndex = new byte[256];
        Arrays.fill(childIndex, EMPTY_SLOT);
        ArtNode<V>[] children = new ArtNode[MAX_CHILDREN];

        for (int i = 0; i < n16Count; i++) {
            int keyIdx = n16Keys[i] & 0xFF;
            childIndex[keyIdx] = (byte) i;
            children[i] = n16Children[i];
        }

        return new Node48<>(prefix, childIndex, children, n16Count, cachedSize);
    }

    @Override
    public ArtNode<V> findChild(byte key) {
        int idx = childIndex[key & 0xFF] & 0xFF;
        if (idx == 0xFF) {
            return null;
        }
        return children[idx];
    }

    private int findFreeSlot() {
        for (int i = 0; i < MAX_CHILDREN; i++) {
            if (children[i] == null) {
                return i;
            }
        }
        return -1; // should never happen if numChildren < MAX_CHILDREN
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
        int slotIdx = childIndex[nextByte & 0xFF] & 0xFF;

        if (slotIdx != 0xFF) {
            // Child exists — recurse
            ArtNode<V> oldChild = children[slotIdx];
            ArtNode<V> newChild = oldChild.insert(key, depth + 1, value);
            if (newChild == oldChild) {
                return this;
            }
            return copyAndSetChild(nextByte, slotIdx, newChild, cachedSize + (newChild.size() - oldChild.size()));
        }

        // New child needed
        ArtNode<V> newChild = new Leaf<>(key, value);

        if (numChildren < MAX_CHILDREN) {
            return addChild(nextByte, newChild);
        }

        // Grow to Node256
        return growToNode256().insert(key, depth - prefix.length(), value);
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

        Node48<V> existingSubtree = new Node48<>(remainingPrefix, childIndex.clone(), children.clone(), numChildren, cachedSize);

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
        int slotIdx = childIndex[nextByte & 0xFF] & 0xFF;

        if (slotIdx == 0xFF) {
            return this;
        }

        ArtNode<V> oldChild = children[slotIdx];
        ArtNode<V> newChild = oldChild.remove(key, depth + 1);

        if (newChild == oldChild) {
            return this;
        }

        if (newChild == null) {
            return removeChild(nextByte, slotIdx);
        }

        return copyAndSetChild(nextByte, slotIdx, newChild, cachedSize + (newChild.size() - oldChild.size()));
    }

    private ArtNode<V> removeChild(byte key, int slotIdx) {
        int removedSize = children[slotIdx].size();
        int newCount = numChildren - 1;

        if (newCount < MIN_CHILDREN) {
            return shrinkToNode16(key, cachedSize - removedSize);
        }

        byte[] newChildIndex = childIndex.clone();
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();

        newChildIndex[key & 0xFF] = EMPTY_SLOT;
        newChildren[slotIdx] = null;

        return new Node48<>(prefix, newChildIndex, newChildren, newCount, cachedSize - removedSize);
    }

    @SuppressWarnings("unchecked")
    private Node16<V> shrinkToNode16(byte skipKey, int newCachedSize) {
        byte[] n16Keys = new byte[Node16.MAX_CHILDREN];
        ArtNode<V>[] n16Children = new ArtNode[Node16.MAX_CHILDREN];
        int j = 0;

        for (int i = 0; i < 256; i++) {
            if (i == (skipKey & 0xFF)) continue;
            int slot = childIndex[i] & 0xFF;
            if (slot != 0xFF && children[slot] != null) {
                n16Keys[j] = (byte) i;
                n16Children[j] = children[slot];
                j++;
            }
        }

        return Node16.fromNode4(prefix, n16Keys, n16Children, j, newCachedSize);
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
        for (int i = 0; i < 256; i++) {
            int slot = childIndex[i] & 0xFF;
            if (slot != 0xFF && children[slot] != null) {
                children[slot].forEach(action);
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

    int numChildren() {
        return numChildren;
    }

    private Node48<V> addChild(byte key, ArtNode<V> child) {
        byte[] newChildIndex = childIndex.clone();
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();

        int slot = findFreeSlotIn(newChildren);
        newChildIndex[key & 0xFF] = (byte) slot;
        newChildren[slot] = child;

        return new Node48<>(prefix, newChildIndex, newChildren, numChildren + 1, cachedSize + child.size());
    }

    private static <V> int findFreeSlotIn(ArtNode<V>[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == null) return i;
        }
        return -1;
    }

    private Node48<V> copyAndSetChild(byte key, int slotIdx, ArtNode<V> newChild, int newCachedSize) {
        byte[] newChildIndex = childIndex.clone();
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();
        newChildren[slotIdx] = newChild;
        return new Node48<>(prefix, newChildIndex, newChildren, numChildren, newCachedSize);
    }

    Node256<V> growToNode256() {
        return Node256.fromNode48(prefix, childIndex, children, numChildren, cachedSize);
    }

    Node48<V> withPrefix(PrefixData newPrefix) {
        return new Node48<>(newPrefix, childIndex.clone(), children.clone(), numChildren, cachedSize);
    }
}
