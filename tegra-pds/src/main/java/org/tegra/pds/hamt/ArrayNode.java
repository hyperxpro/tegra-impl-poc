package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A full 32-child HAMT node used when a BitmapIndexedNode has too many children.
 * Direct-indexed by the 5-bit hash fragment.
 */
public final class ArrayNode<K, V> implements HamtNode<K, V> {

    static final int DOWNGRADE_THRESHOLD = 8;

    private final HamtNode<K, V>[] children;
    private final int count; // number of non-null children
    private final MutationContext ownerCtx;

    @SuppressWarnings("unchecked")
    ArrayNode(HamtNode<K, V>[] children, int count, MutationContext ctx) {
        this.children = children;
        this.count = count;
        this.ownerCtx = ctx;
    }

    private boolean isEditable(MutationContext ctx) {
        return ctx != null && ctx == this.ownerCtx && ctx.isEditable();
    }

    @Override
    public HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx) {
        int frag = BitmapIndexedNode.mask(hash, shift);
        HamtNode<K, V> child = children[frag];

        if (child == null) {
            HamtNode<K, V> newChild = EmptyNode.<K, V>instance().put(key, value, hash, shift + BitmapIndexedNode.BITS_PER_LEVEL, ctx);
            if (isEditable(ctx)) {
                children[frag] = newChild;
                return new ArrayNode<>(children, count + 1, ctx);
            }
            HamtNode<K, V>[] newChildren = children.clone();
            newChildren[frag] = newChild;
            return new ArrayNode<>(newChildren, count + 1, ctx);
        }

        HamtNode<K, V> newChild = child.put(key, value, hash, shift + BitmapIndexedNode.BITS_PER_LEVEL, ctx);
        if (newChild == child) {
            return this;
        }
        if (isEditable(ctx)) {
            children[frag] = newChild;
            return this;
        }
        HamtNode<K, V>[] newChildren = children.clone();
        newChildren[frag] = newChild;
        return new ArrayNode<>(newChildren, count, ctx);
    }

    @Override
    public HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx) {
        int frag = BitmapIndexedNode.mask(hash, shift);
        HamtNode<K, V> child = children[frag];

        if (child == null) {
            return this;
        }

        HamtNode<K, V> newChild = child.remove(key, hash, shift + BitmapIndexedNode.BITS_PER_LEVEL, ctx);
        if (newChild == child) {
            return this;
        }

        if (newChild instanceof EmptyNode) {
            int newCount = count - 1;
            if (newCount < DOWNGRADE_THRESHOLD) {
                return downgrade(frag, shift, ctx);
            }
            HamtNode<K, V>[] newChildren = children.clone();
            newChildren[frag] = null;
            return new ArrayNode<>(newChildren, newCount, ctx);
        }

        HamtNode<K, V>[] newChildren = children.clone();
        newChildren[frag] = newChild;
        return new ArrayNode<>(newChildren, count, ctx);
    }

    @SuppressWarnings("unchecked")
    private HamtNode<K, V> downgrade(int removedFrag, int shift, MutationContext ctx) {
        // Collect all remaining entries into a BitmapIndexedNode
        // All children are sub-nodes in a BitmapIndexedNode
        int nodemap = 0;
        int nodeCount = 0;
        for (int i = 0; i < BitmapIndexedNode.WIDTH; i++) {
            if (i != removedFrag && children[i] != null) {
                nodemap |= (1 << i);
                nodeCount++;
            }
        }

        Object[] contents = new Object[nodeCount]; // only sub-nodes, no inline data
        // Store sub-nodes in reverse order
        int idx = 0;
        for (int i = BitmapIndexedNode.WIDTH - 1; i >= 0; i--) {
            if (i != removedFrag && children[i] != null) {
                // We need to store in reverse of bit order
            }
        }

        // Correct approach: iterate in bit order, store in reverse at end of array
        HamtNode<K, V>[] nodes = new HamtNode[nodeCount];
        int ni = 0;
        for (int i = 0; i < BitmapIndexedNode.WIDTH; i++) {
            if (i != removedFrag && children[i] != null) {
                nodes[ni++] = children[i];
            }
        }

        // contents layout: [subnodes in reverse order]
        Object[] newContents = new Object[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            newContents[nodeCount - 1 - i] = nodes[i];
        }

        return new BitmapIndexedNode<>(0, nodemap, newContents, ctx);
    }

    @Override
    public V get(K key, int hash, int shift) {
        int frag = BitmapIndexedNode.mask(hash, shift);
        HamtNode<K, V> child = children[frag];
        if (child == null) {
            return null;
        }
        return child.get(key, hash, shift + BitmapIndexedNode.BITS_PER_LEVEL);
    }

    @Override
    public int size() {
        int s = 0;
        for (HamtNode<K, V> child : children) {
            if (child != null) {
                s += child.size();
            }
        }
        return s;
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        for (HamtNode<K, V> child : children) {
            if (child != null) {
                child.forEach(action);
            }
        }
    }
}
