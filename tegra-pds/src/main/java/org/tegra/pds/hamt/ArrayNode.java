package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.function.BiConsumer;

/**
 * Full 32-slot HAMT node for densely populated positions.
 * Each of the 32 positions either has a child node or is null.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class ArrayNode<K, V> implements HamtNode<K, V> {

    private static final int PARTITION_BITS = 5;
    private static final int PARTITION_SIZE = 1 << PARTITION_BITS; // 32
    private static final int MASK = PARTITION_SIZE - 1;

    /**
     * Threshold below which we downgrade to BitmapIndexedNode.
     */
    private static final int SHRINK_THRESHOLD = 8;

    private final HamtNode<K, V>[] children;
    private final int childCount; // number of non-null children
    private final int cachedSize;

    @SuppressWarnings("unchecked")
    ArrayNode(HamtNode<K, V>[] children, int childCount, int cachedSize) {
        this.children = children;
        this.childCount = childCount;
        this.cachedSize = cachedSize;
    }

    @Override
    public V get(K key, int hash, int shift) {
        int idx = (hash >>> shift) & MASK;
        HamtNode<K, V> child = children[idx];
        if (child == null) {
            return null;
        }
        return child.get(key, hash, shift + PARTITION_BITS);
    }

    @Override
    public HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx) {
        int idx = (hash >>> shift) & MASK;
        HamtNode<K, V> child = children[idx];

        if (child == null) {
            // Insert new entry
            HamtNode<K, V> newChild = BitmapIndexedNode.single(key, value, hash, shift + PARTITION_BITS);
            return copyAndSetChild(idx, newChild, childCount + 1, cachedSize + 1);
        }

        int oldSize = child.size();
        HamtNode<K, V> newChild = child.put(key, value, hash, shift + PARTITION_BITS, ctx);
        if (newChild == child) {
            return this;
        }
        int newSize = newChild.size();
        return copyAndSetChild(idx, newChild, childCount, cachedSize + (newSize - oldSize));
    }

    @Override
    public HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx) {
        int idx = (hash >>> shift) & MASK;
        HamtNode<K, V> child = children[idx];

        if (child == null) {
            return this;
        }

        int oldSize = child.size();
        HamtNode<K, V> newChild = child.remove(key, hash, shift + PARTITION_BITS, ctx);

        if (newChild == child) {
            return this;
        }

        int newSize = newChild.size();
        int newCachedSize = cachedSize + (newSize - oldSize);

        if (newChild instanceof EmptyNode) {
            int newChildCount = childCount - 1;
            if (newChildCount < SHRINK_THRESHOLD) {
                return shrinkToBitmapNode(idx, shift, newCachedSize);
            }
            return copyAndSetChild(idx, null, newChildCount, newCachedSize);
        }

        return copyAndSetChild(idx, newChild, childCount, newCachedSize);
    }

    @Override
    public int size() {
        return cachedSize;
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        for (HamtNode<K, V> child : children) {
            if (child != null) {
                child.forEach(action);
            }
        }
    }

    private ArrayNode<K, V> copyAndSetChild(int idx, HamtNode<K, V> child, int newChildCount, int newCachedSize) {
        @SuppressWarnings("unchecked")
        HamtNode<K, V>[] newChildren = children.clone();
        newChildren[idx] = child;
        return new ArrayNode<>(newChildren, newChildCount, newCachedSize);
    }

    @SuppressWarnings("unchecked")
    private HamtNode<K, V> shrinkToBitmapNode(int removedIdx, int shift, int newCachedSize) {
        // Count remaining children and build arrays
        int remaining = childCount - 1;
        int nodeMap = 0;

        for (int i = 0; i < PARTITION_SIZE; i++) {
            if (i == removedIdx) continue;
            if (children[i] != null) {
                nodeMap |= (1 << i);
            }
        }

        Object[] content = new Object[remaining];
        int j = content.length - 1;
        for (int i = 0; i < PARTITION_SIZE; i++) {
            if (i == removedIdx) continue;
            if (children[i] != null) {
                content[j--] = children[i];
            }
        }

        return new BitmapIndexedNode<>(0, nodeMap, content, newCachedSize);
    }
}
