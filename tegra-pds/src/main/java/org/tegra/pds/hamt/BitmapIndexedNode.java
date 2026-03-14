package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A sparse HAMT node that uses a bitmap to indicate which of the 32 positions
 * are occupied and a compact array to store key-value pairs and child sub-nodes.
 * <p>
 * The contents array is laid out as:
 * [key0, val0, key1, val1, ..., subNode0, subNode1, ...]
 * <p>
 * The bitmap has two parts: datamap tracks positions with inline key-value pairs,
 * and nodemap tracks positions with child sub-nodes.
 */
public final class BitmapIndexedNode<K, V> implements HamtNode<K, V> {

    static final int BITS_PER_LEVEL = 5;
    static final int WIDTH = 1 << BITS_PER_LEVEL; // 32
    static final int MASK = WIDTH - 1;

    /**
     * Threshold: when the total number of entries in a BitmapIndexedNode
     * (data entries + sub-nodes) reaches this, upgrade to ArrayNode.
     */
    static final int UPGRADE_THRESHOLD = 16;

    private int datamap;  // bitmap for inline key-value pairs
    private int nodemap;  // bitmap for child sub-nodes
    private Object[] contents; // [k0,v0,k1,v1,...,subN-1,...,sub0] — sub-nodes stored in reverse at end

    private final MutationContext ownerCtx;

    BitmapIndexedNode(int datamap, int nodemap, Object[] contents, MutationContext ctx) {
        this.datamap = datamap;
        this.nodemap = nodemap;
        this.contents = contents;
        this.ownerCtx = ctx;
    }

    static int mask(int hash, int shift) {
        return (hash >>> shift) & MASK;
    }

    static int bitpos(int hash, int shift) {
        return 1 << mask(hash, shift);
    }

    int dataIndex(int bit) {
        return Integer.bitCount(datamap & (bit - 1));
    }

    int nodeIndex(int bit) {
        return Integer.bitCount(nodemap & (bit - 1));
    }

    int dataCount() {
        return Integer.bitCount(datamap);
    }

    int nodeCount() {
        return Integer.bitCount(nodemap);
    }

    @SuppressWarnings("unchecked")
    K keyAt(int dataIdx) {
        return (K) contents[2 * dataIdx];
    }

    @SuppressWarnings("unchecked")
    V valAt(int dataIdx) {
        return (V) contents[2 * dataIdx + 1];
    }

    @SuppressWarnings("unchecked")
    HamtNode<K, V> nodeAt(int nodeIdx) {
        // Sub-nodes are stored in reverse order at the end of the array
        return (HamtNode<K, V>) contents[contents.length - 1 - nodeIdx];
    }

    /**
     * Creates a BitmapIndexedNode with a single key-value pair.
     */
    static <K, V> BitmapIndexedNode<K, V> single(K key, V value, int hash, int shift) {
        int bit = bitpos(hash, shift);
        return new BitmapIndexedNode<>(bit, 0, new Object[]{key, value}, null);
    }

    private boolean isEditable(MutationContext ctx) {
        return ctx != null && ctx == this.ownerCtx && ctx.isEditable();
    }

    private BitmapIndexedNode<K, V> editableOrCopy(MutationContext ctx) {
        if (isEditable(ctx)) {
            return this;
        }
        return new BitmapIndexedNode<>(datamap, nodemap, contents.clone(), ctx);
    }

    @Override
    public HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx) {
        int bit = bitpos(hash, shift);

        if ((datamap & bit) != 0) {
            // Position has an inline key-value pair
            int dIdx = dataIndex(bit);
            K existingKey = keyAt(dIdx);

            if (Objects.equals(existingKey, key)) {
                // Same key — update value
                V existingVal = valAt(dIdx);
                if (Objects.equals(existingVal, value)) {
                    return this;
                }
                BitmapIndexedNode<K, V> editable = editableOrCopy(ctx);
                editable.contents[2 * dIdx + 1] = value;
                return editable;
            }

            // Different key at same position — need to push down
            int existingHash = Objects.hashCode(existingKey);
            V existingVal = valAt(dIdx);

            if (existingHash == hash && shift >= 30) {
                // True hash collision — create collision node
                CollisionNode<K, V> collision = new CollisionNode<>(hash, existingKey, existingVal, key, value);
                return replaceDataWithNode(bit, dIdx, collision, ctx);
            }

            // Create a sub-node with both entries
            HamtNode<K, V> subNode = EmptyNode.<K, V>instance()
                    .put(existingKey, existingVal, existingHash, shift + BITS_PER_LEVEL, ctx)
                    .put(key, value, hash, shift + BITS_PER_LEVEL, ctx);

            return replaceDataWithNode(bit, dIdx, subNode, ctx);

        } else if ((nodemap & bit) != 0) {
            // Position has a sub-node
            int nIdx = nodeIndex(bit);
            HamtNode<K, V> subNode = nodeAt(nIdx);
            HamtNode<K, V> newSubNode = subNode.put(key, value, hash, shift + BITS_PER_LEVEL, ctx);
            if (newSubNode == subNode) {
                return this;
            }
            BitmapIndexedNode<K, V> editable = editableOrCopy(ctx);
            editable.contents[editable.contents.length - 1 - nIdx] = newSubNode;
            return editable;

        } else {
            // Position is empty — insert inline key-value pair
            int dIdx = dataIndex(bit);
            int dc = dataCount();
            int nc = nodeCount();

            // Check if we should upgrade to ArrayNode
            if (dc + nc >= UPGRADE_THRESHOLD) {
                return upgradeToArrayNode(key, value, hash, shift, ctx);
            }

            Object[] newContents = new Object[2 * (dc + 1) + nc];
            // Copy data entries before insertion point
            System.arraycopy(contents, 0, newContents, 0, 2 * dIdx);
            // Insert new entry
            newContents[2 * dIdx] = key;
            newContents[2 * dIdx + 1] = value;
            // Copy remaining data entries
            System.arraycopy(contents, 2 * dIdx, newContents, 2 * (dIdx + 1), 2 * (dc - dIdx));
            // Copy sub-nodes (at the end)
            System.arraycopy(contents, 2 * dc, newContents, 2 * (dc + 1), nc);

            return new BitmapIndexedNode<>(datamap | bit, nodemap, newContents, ctx);
        }
    }

    private HamtNode<K, V> replaceDataWithNode(int bit, int dIdx, HamtNode<K, V> subNode, MutationContext ctx) {
        int dc = dataCount();
        int nc = nodeCount();
        int nIdx = Integer.bitCount((nodemap | bit) & (bit - 1));

        // New array: one fewer data pair, one more node
        Object[] newContents = new Object[2 * (dc - 1) + (nc + 1)];

        // Copy data entries, skipping the one being replaced
        System.arraycopy(contents, 0, newContents, 0, 2 * dIdx);
        System.arraycopy(contents, 2 * (dIdx + 1), newContents, 2 * dIdx, 2 * (dc - dIdx - 1));

        // Copy existing sub-nodes and insert new one
        // Sub-nodes are stored in reverse order at end
        int newNodeCount = nc + 1;
        int newDataCount = dc - 1;
        // Copy nodes that come after the new node (in reverse storage, these are at lower indices from the end)
        int existingNodesStart = 2 * dc; // old position of nodes in old array
        int newNodesStart = 2 * newDataCount; // new position of nodes in new array

        // We need to insert the sub-node at the right reverse position
        // nodes stored: contents[len-1] = node at nodeIndex 0, contents[len-2] = node at nodeIndex 1, etc.
        int newLen = newContents.length;
        int oldNodesCopied = 0;
        for (int i = 0; i < newNodeCount; i++) {
            if (i == nIdx) {
                newContents[newLen - 1 - i] = subNode;
            } else {
                newContents[newLen - 1 - i] = contents[contents.length - 1 - oldNodesCopied];
                oldNodesCopied++;
            }
        }

        return new BitmapIndexedNode<>(datamap ^ bit, nodemap | bit, newContents, ctx);
    }

    @SuppressWarnings("unchecked")
    private HamtNode<K, V> upgradeToArrayNode(K key, V value, int hash, int shift, MutationContext ctx) {
        HamtNode<K, V>[] children = new HamtNode[WIDTH];
        int count = 0;

        // Place existing data entries
        int dc = dataCount();
        for (int i = 0; i < dc; i++) {
            K k = keyAt(i);
            V v = valAt(i);
            int kHash = Objects.hashCode(k);
            int frag = mask(kHash, shift);
            children[frag] = EmptyNode.<K, V>instance().put(k, v, kHash, shift + BITS_PER_LEVEL, ctx);
            count++;
        }

        // Place existing sub-nodes
        int nc = nodeCount();
        int nodeMapCopy = nodemap;
        for (int i = 0; i < nc; i++) {
            int lowestBit = Integer.lowestOneBit(nodeMapCopy);
            int frag = Integer.numberOfTrailingZeros(lowestBit);
            children[frag] = nodeAt(i);
            count++;
            nodeMapCopy ^= lowestBit;
        }

        // Insert the new entry
        int frag = mask(hash, shift);
        if (children[frag] == null) {
            children[frag] = EmptyNode.<K, V>instance().put(key, value, hash, shift + BITS_PER_LEVEL, ctx);
            count++;
        } else {
            children[frag] = children[frag].put(key, value, hash, shift + BITS_PER_LEVEL, ctx);
        }

        return new ArrayNode<>(children, count, ctx);
    }

    @Override
    public HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx) {
        int bit = bitpos(hash, shift);

        if ((datamap & bit) != 0) {
            int dIdx = dataIndex(bit);
            K existingKey = keyAt(dIdx);
            if (!Objects.equals(existingKey, key)) {
                return this; // key not found
            }

            int dc = dataCount();
            int nc = nodeCount();

            if (dc == 1 && nc == 0) {
                // This was the only entry
                return EmptyNode.instance();
            }

            // Remove the data entry
            Object[] newContents = new Object[2 * (dc - 1) + nc];
            System.arraycopy(contents, 0, newContents, 0, 2 * dIdx);
            System.arraycopy(contents, 2 * (dIdx + 1), newContents, 2 * (dIdx + 1) - 2, 2 * (dc - dIdx - 1) + nc);

            return new BitmapIndexedNode<>(datamap ^ bit, nodemap, newContents, ctx);

        } else if ((nodemap & bit) != 0) {
            int nIdx = nodeIndex(bit);
            HamtNode<K, V> subNode = nodeAt(nIdx);
            HamtNode<K, V> newSubNode = subNode.remove(key, hash, shift + BITS_PER_LEVEL, ctx);

            if (newSubNode == subNode) {
                return this; // nothing changed
            }

            if (newSubNode instanceof EmptyNode) {
                // Sub-node became empty, remove it
                int dc = dataCount();
                int nc = nodeCount();
                if (dc == 0 && nc == 1) {
                    return EmptyNode.instance();
                }
                Object[] newContents = new Object[2 * dc + (nc - 1)];
                System.arraycopy(contents, 0, newContents, 0, 2 * dc);
                // Copy nodes, skipping the removed one
                int newLen = newContents.length;
                int oldNodesCopied = 0;
                for (int i = 0; i < nc; i++) {
                    if (i == nIdx) continue;
                    newContents[newLen - 1 - oldNodesCopied] = contents[contents.length - 1 - i];
                    oldNodesCopied++;
                }
                return new BitmapIndexedNode<>(datamap, nodemap ^ bit, newContents, ctx);
            }

            // If the sub-node collapsed to a single-entry BitmapIndexedNode, inline it
            if (newSubNode instanceof BitmapIndexedNode<K, V> bin
                    && bin.dataCount() == 1 && bin.nodeCount() == 0) {
                // Inline the single entry back up
                K inlineKey = bin.keyAt(0);
                V inlineVal = bin.valAt(0);
                return replaceNodeWithData(bit, nIdx, inlineKey, inlineVal, ctx);
            }

            BitmapIndexedNode<K, V> editable = editableOrCopy(ctx);
            editable.contents[editable.contents.length - 1 - nIdx] = newSubNode;
            return editable;

        } else {
            // Key not present
            return this;
        }
    }

    private HamtNode<K, V> replaceNodeWithData(int bit, int nIdx, K key, V value, MutationContext ctx) {
        int dc = dataCount();
        int nc = nodeCount();
        int dIdx = Integer.bitCount((datamap | bit) & (bit - 1));

        Object[] newContents = new Object[2 * (dc + 1) + (nc - 1)];

        // Copy data before insertion
        System.arraycopy(contents, 0, newContents, 0, 2 * dIdx);
        newContents[2 * dIdx] = key;
        newContents[2 * dIdx + 1] = value;
        System.arraycopy(contents, 2 * dIdx, newContents, 2 * (dIdx + 1), 2 * (dc - dIdx));

        // Copy nodes, skipping the removed one
        int newLen = newContents.length;
        int newNodeCount = nc - 1;
        int oldNodesCopied = 0;
        for (int i = 0; i < nc; i++) {
            if (i == nIdx) continue;
            newContents[newLen - 1 - oldNodesCopied] = contents[contents.length - 1 - i];
            oldNodesCopied++;
        }

        return new BitmapIndexedNode<>(datamap | bit, nodemap ^ bit, newContents, ctx);
    }

    @Override
    public V get(K key, int hash, int shift) {
        int bit = bitpos(hash, shift);

        if ((datamap & bit) != 0) {
            int dIdx = dataIndex(bit);
            K existingKey = keyAt(dIdx);
            if (Objects.equals(existingKey, key)) {
                return valAt(dIdx);
            }
            return null;
        }

        if ((nodemap & bit) != 0) {
            int nIdx = nodeIndex(bit);
            return nodeAt(nIdx).get(key, hash, shift + BITS_PER_LEVEL);
        }

        return null;
    }

    @Override
    public int size() {
        int s = dataCount();
        int nc = nodeCount();
        for (int i = 0; i < nc; i++) {
            s += nodeAt(i).size();
        }
        return s;
    }

    @Override
    public void forEach(BiConsumer<K, V> action) {
        int dc = dataCount();
        for (int i = 0; i < dc; i++) {
            action.accept(keyAt(i), valAt(i));
        }
        int nc = nodeCount();
        for (int i = 0; i < nc; i++) {
            nodeAt(i).forEach(action);
        }
    }

    // Expose for diff computation
    int datamap() { return datamap; }
    int nodemap() { return nodemap; }
}
