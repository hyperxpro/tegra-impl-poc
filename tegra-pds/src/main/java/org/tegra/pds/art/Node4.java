package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * ART internal node with 1-4 children.
 * Uses sorted byte array for keys and linear scan for lookups.
 * Grows to Node16 when full.
 *
 * @param <V> the value type
 */
public final class Node4<V> implements ArtNode<V> {

    static final int MAX_CHILDREN = 4;

    private final PrefixData prefix;
    private final byte[] keys;
    private final ArtNode<V>[] children;
    private final int numChildren;
    private final int cachedSize;

    @SuppressWarnings("unchecked")
    public Node4(PrefixData prefix) {
        this.prefix = prefix;
        this.keys = new byte[MAX_CHILDREN];
        this.children = new ArtNode[MAX_CHILDREN];
        this.numChildren = 0;
        this.cachedSize = 0;
    }

    @SuppressWarnings("unchecked")
    Node4(PrefixData prefix, byte[] keys, ArtNode<V>[] children, int numChildren, int cachedSize) {
        this.prefix = prefix;
        this.keys = keys;
        this.children = children;
        this.numChildren = numChildren;
        this.cachedSize = cachedSize;
    }

    /**
     * Adds a child directly during node construction. Used by Leaf.insert().
     * Returns a new Node4 with the child added in sorted order.
     */
    Node4<V> addChildDirect(byte key, ArtNode<V> child) {
        byte[] newKeys = keys.clone();
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();

        // Find insertion point (maintain sorted order)
        int pos = 0;
        while (pos < numChildren && (newKeys[pos] & 0xFF) < (key & 0xFF)) {
            pos++;
        }

        // Shift elements right
        for (int i = numChildren; i > pos; i--) {
            newKeys[i] = newKeys[i - 1];
            newChildren[i] = newChildren[i - 1];
        }

        newKeys[pos] = key;
        newChildren[pos] = child;

        return new Node4<>(prefix, newKeys, newChildren, numChildren + 1, cachedSize + child.size());
    }

    @Override
    public ArtNode<V> findChild(byte key) {
        for (int i = 0; i < numChildren; i++) {
            if (keys[i] == key) {
                return children[i];
            }
        }
        return null;
    }

    private int findChildIndex(byte key) {
        for (int i = 0; i < numChildren; i++) {
            if (keys[i] == key) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public ArtNode<V> insert(byte[] key, int depth, V value) {
        // Check prefix match
        int prefixMatchLen = prefix.matchPrefix(key, depth);

        if (prefixMatchLen < prefix.length()) {
            // Partial prefix match — need to split this node
            return splitAndInsert(key, depth, value, prefixMatchLen);
        }

        depth += prefix.length();

        if (depth >= key.length) {
            // Key is exhausted at an internal node. This shouldn't normally happen
            // with proper key encoding but handle gracefully.
            return this;
        }

        byte nextByte = key[depth];
        int childIdx = findChildIndex(nextByte);

        if (childIdx >= 0) {
            // Child exists — recurse
            ArtNode<V> oldChild = children[childIdx];
            ArtNode<V> newChild = oldChild.insert(key, depth + 1, value);
            if (newChild == oldChild) {
                return this;
            }
            return copyAndSetChild(childIdx, newChild, cachedSize + (newChild.size() - oldChild.size()));
        }

        // New child needed
        ArtNode<V> newChild = new Leaf<>(key, value);

        if (numChildren < MAX_CHILDREN) {
            return addChildDirect(nextByte, newChild);
        }

        // Grow to Node16
        return growToNode16().insert(key, depth - prefix.length(), value);
    }

    private ArtNode<V> splitAndInsert(byte[] key, int depth, V value, int prefixMatchLen) {
        // Create new parent node with the shared prefix
        PrefixData sharedPrefix = PrefixData.fromKey(prefix.prefix(), 0, prefixMatchLen);

        // This node's remaining prefix after the split point
        byte splitByte = prefix.at(prefixMatchLen);
        PrefixData remainingPrefix;
        if (prefix.length() - prefixMatchLen - 1 > 0) {
            int remainLen = prefix.length() - prefixMatchLen - 1;
            byte[] remainBytes = new byte[Math.min(remainLen, PrefixData.MAX_PREFIX_LENGTH)];
            int copyLen = Math.min(remainLen, Math.min(PrefixData.MAX_PREFIX_LENGTH, prefix.prefix().length - prefixMatchLen - 1));
            if (copyLen > 0) {
                System.arraycopy(prefix.prefix(), prefixMatchLen + 1, remainBytes, 0, copyLen);
            }
            remainingPrefix = new PrefixData(remainBytes, remainLen);
        } else {
            remainingPrefix = PrefixData.EMPTY;
        }

        // Copy of this node with truncated prefix
        Node4<V> existingSubtree = new Node4<>(remainingPrefix, keys.clone(), children.clone(), numChildren, cachedSize);

        // Create new parent with two children
        Node4<V> newParent = new Node4<>(sharedPrefix);

        // Add the existing subtree
        newParent = newParent.addChildDirect(splitByte, existingSubtree);

        // Add the new leaf
        int newDepth = depth + prefixMatchLen;
        if (newDepth < key.length) {
            byte newByte = key[newDepth];
            ArtNode<V> newLeaf = new Leaf<>(key, value);
            newParent = newParent.addChildDirect(newByte, newLeaf);
        }

        return newParent;
    }

    @Override
    public ArtNode<V> remove(byte[] key, int depth) {
        int prefixMatchLen = prefix.matchPrefix(key, depth);
        if (prefixMatchLen < prefix.length()) {
            return this; // prefix doesn't match
        }

        depth += prefix.length();

        if (depth >= key.length) {
            return this;
        }

        byte nextByte = key[depth];
        int childIdx = findChildIndex(nextByte);

        if (childIdx < 0) {
            return this; // child not found
        }

        ArtNode<V> oldChild = children[childIdx];
        ArtNode<V> newChild = oldChild.remove(key, depth + 1);

        if (newChild == oldChild) {
            return this; // nothing changed
        }

        if (newChild == null) {
            // Child was removed
            return removeChild(childIdx);
        }

        return copyAndSetChild(childIdx, newChild, cachedSize + (newChild.size() - oldChild.size()));
    }

    private ArtNode<V> removeChild(int childIdx) {
        if (numChildren == 2) {
            // Collapse: remaining child gets merged with this node's prefix
            int otherIdx = (childIdx == 0) ? 1 : 0;
            // But we must check if there's only one other child
            // With 2 children and removing 1, we have 1 remaining
            // If the remaining child is a leaf or another internal node,
            // we need to merge the prefix
            byte otherKey = keys[otherIdx];
            ArtNode<V> otherChild = children[otherIdx];

            return collapseChild(otherKey, otherChild);
        }

        if (numChildren == 1) {
            return null; // node becomes empty
        }

        // Remove and compact
        byte[] newKeys = new byte[MAX_CHILDREN];
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = new ArtNode[MAX_CHILDREN];
        int j = 0;
        int removedSize = children[childIdx].size();
        for (int i = 0; i < numChildren; i++) {
            if (i != childIdx) {
                newKeys[j] = keys[i];
                newChildren[j] = children[i];
                j++;
            }
        }

        return new Node4<>(prefix, newKeys, newChildren, numChildren - 1, cachedSize - removedSize);
    }

    /**
     * Collapses a single remaining child into this node, merging prefixes.
     */
    private ArtNode<V> collapseChild(byte edgeByte, ArtNode<V> child) {
        if (child instanceof Leaf<V> leaf) {
            // Just return the leaf as-is
            return leaf;
        }

        if (child instanceof Node4<V> n4) {
            // Merge prefixes: this.prefix + edgeByte + child.prefix
            PrefixData merged = mergePrefix(prefix, edgeByte, n4.prefix);
            return new Node4<>(merged, n4.keys.clone(), n4.children.clone(), n4.numChildren, n4.cachedSize);
        }
        if (child instanceof Node16<V> n16) {
            PrefixData merged = mergePrefix(prefix, edgeByte, n16.prefix());
            return n16.withPrefix(merged);
        }
        if (child instanceof Node48<V> n48) {
            PrefixData merged = mergePrefix(prefix, edgeByte, n48.prefix());
            return n48.withPrefix(merged);
        }
        if (child instanceof Node256<V> n256) {
            PrefixData merged = mergePrefix(prefix, edgeByte, n256.prefix());
            return n256.withPrefix(merged);
        }

        return child;
    }

    static PrefixData mergePrefix(PrefixData parentPrefix, byte edgeByte, PrefixData childPrefix) {
        int newLen = parentPrefix.length() + 1 + childPrefix.length();
        int storeLen = Math.min(newLen, PrefixData.MAX_PREFIX_LENGTH);
        byte[] newBytes = new byte[storeLen];
        int pos = 0;

        // Copy parent prefix bytes
        int parentCopy = Math.min(parentPrefix.length(), storeLen);
        for (int i = 0; i < parentCopy && pos < storeLen; i++) {
            newBytes[pos++] = parentPrefix.at(i);
        }

        // Add edge byte
        if (pos < storeLen) {
            newBytes[pos++] = edgeByte;
        }

        // Copy child prefix bytes
        int childCopy = Math.min(childPrefix.length(), storeLen - pos);
        for (int i = 0; i < childCopy && pos < storeLen; i++) {
            newBytes[pos++] = childPrefix.at(i);
        }

        return new PrefixData(newBytes, newLen);
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

    private Node4<V> copyAndSetChild(int idx, ArtNode<V> newChild, int newCachedSize) {
        byte[] newKeys = keys.clone();
        @SuppressWarnings("unchecked")
        ArtNode<V>[] newChildren = children.clone();
        newChildren[idx] = newChild;
        return new Node4<>(prefix, newKeys, newChildren, numChildren, newCachedSize);
    }

    Node16<V> growToNode16() {
        return Node16.fromNode4(prefix, keys, children, numChildren, cachedSize);
    }

    Node4<V> withPrefix(PrefixData newPrefix) {
        return new Node4<>(newPrefix, keys.clone(), children.clone(), numChildren, cachedSize);
    }
}
