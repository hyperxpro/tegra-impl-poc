package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Bitmap-indexed HAMT node with sparse 32-way branching.
 * <p>
 * Uses a 32-bit bitmap to track which of the 32 positions are populated.
 * The content array stores entries compactly: for each set bit in the bitmap,
 * there is either a key-value pair (two adjacent slots) or a child node (one slot).
 * <p>
 * Layout of the content array:
 * - dataMap tracks positions that contain inline key-value pairs
 * - nodeMap tracks positions that contain child sub-nodes
 * - Key-value pairs are stored at the beginning: [k0, v0, k1, v1, ...]
 * - Child nodes are stored at the end (in reverse order): [..., child1, child0]
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class BitmapIndexedNode<K, V> implements HamtNode<K, V> {

    private static final int PARTITION_BITS = 5;
    private static final int PARTITION_SIZE = 1 << PARTITION_BITS; // 32
    private static final int MASK = PARTITION_SIZE - 1;

    /**
     * Threshold above which we upgrade to ArrayNode.
     */
    private static final int ARRAY_NODE_THRESHOLD = 16;

    private final int dataMap;
    private final int nodeMap;
    private final Object[] content;
    private final int cachedSize;
    private long ownerContextId;

    BitmapIndexedNode(int dataMap, int nodeMap, Object[] content, int cachedSize) {
        this.dataMap = dataMap;
        this.nodeMap = nodeMap;
        this.content = content;
        this.cachedSize = cachedSize;
        this.ownerContextId = 0;
    }

    BitmapIndexedNode(int dataMap, int nodeMap, Object[] content, int cachedSize, long ownerContextId) {
        this.dataMap = dataMap;
        this.nodeMap = nodeMap;
        this.content = content;
        this.cachedSize = cachedSize;
        this.ownerContextId = ownerContextId;
    }

    static int mask(int hash, int shift) {
        return (hash >>> shift) & MASK;
    }

    static int bitpos(int hash, int shift) {
        return 1 << mask(hash, shift);
    }

    static int index(int bitmap, int bit) {
        return Integer.bitCount(bitmap & (bit - 1));
    }

    /**
     * Creates a single-entry node.
     */
    static <K, V> BitmapIndexedNode<K, V> single(K key, V value, int hash, int shift) {
        int bit = bitpos(hash, shift);
        Object[] content = new Object[]{key, value};
        return new BitmapIndexedNode<>(bit, 0, content, 1);
    }

    private int dataIndex(int bit) {
        return 2 * index(dataMap, bit);
    }

    private int nodeIndex(int bit) {
        return content.length - 1 - index(nodeMap, bit);
    }

    private int dataCount() {
        return Integer.bitCount(dataMap);
    }

    private int nodeCount() {
        return Integer.bitCount(nodeMap);
    }

    @SuppressWarnings("unchecked")
    private K keyAt(int dataIdx) {
        return (K) content[dataIdx];
    }

    @SuppressWarnings("unchecked")
    private V valueAt(int dataIdx) {
        return (V) content[dataIdx + 1];
    }

    @SuppressWarnings("unchecked")
    private HamtNode<K, V> nodeAt(int nodeIdx) {
        return (HamtNode<K, V>) content[nodeIdx];
    }

    @Override
    public V get(K key, int hash, int shift) {
        int bit = bitpos(hash, shift);

        if ((dataMap & bit) != 0) {
            int idx = dataIndex(bit);
            K k = keyAt(idx);
            if (Objects.equals(key, k)) {
                return valueAt(idx);
            }
            return null;
        }

        if ((nodeMap & bit) != 0) {
            return nodeAt(nodeIndex(bit)).get(key, hash, shift + PARTITION_BITS);
        }

        return null;
    }

    @Override
    public HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx) {
        int bit = bitpos(hash, shift);

        if ((dataMap & bit) != 0) {
            // Position has an inline key-value pair
            int idx = dataIndex(bit);
            K existingKey = keyAt(idx);
            V existingValue = valueAt(idx);

            if (Objects.equals(key, existingKey)) {
                // Same key — update value
                if (Objects.equals(value, existingValue)) {
                    return this;
                }
                return copyAndSetValue(idx, value, ctx);
            }

            // Hash collision at this level — push both entries down
            int existingHash = existingKey.hashCode();
            HamtNode<K, V> subNode = mergeTwoEntries(
                    existingKey, existingValue, existingHash,
                    key, value, hash,
                    shift + PARTITION_BITS
            );
            return copyAndMigrateFromDataToNode(bit, idx, subNode, ctx);
        }

        if ((nodeMap & bit) != 0) {
            // Position has a sub-node — recurse
            int nIdx = nodeIndex(bit);
            HamtNode<K, V> subNode = nodeAt(nIdx);
            int oldSize = subNode.size();
            HamtNode<K, V> newSubNode = subNode.put(key, value, hash, shift + PARTITION_BITS, ctx);
            if (newSubNode == subNode) {
                return this;
            }
            int newSize = newSubNode.size();
            return copyAndSetNode(nIdx, newSubNode, cachedSize + (newSize - oldSize), ctx);
        }

        // Position is empty — insert inline key-value pair
        // Check if we should upgrade to ArrayNode
        int totalEntries = dataCount() + nodeCount();
        if (totalEntries >= ARRAY_NODE_THRESHOLD) {
            return upgradeToArrayNode(shift).put(key, value, hash, shift, ctx);
        }

        return copyAndInsertData(bit, key, value, ctx);
    }

    @Override
    public HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx) {
        int bit = bitpos(hash, shift);

        if ((dataMap & bit) != 0) {
            int idx = dataIndex(bit);
            K existingKey = keyAt(idx);
            if (!Objects.equals(key, existingKey)) {
                return this;
            }
            // Remove this data entry
            if (dataCount() == 1 && nodeCount() == 0) {
                return EmptyNode.instance();
            }
            if (dataCount() == 2 && nodeCount() == 0) {
                // Return a single-entry node with the other pair
                int otherIdx = (idx == 0) ? 2 : 0;
                K otherKey = keyAt(otherIdx);
                V otherValue = valueAt(otherIdx);
                int newBitmap = dataMap ^ bit;
                Object[] newContent = new Object[]{otherKey, otherValue};
                return new BitmapIndexedNode<>(newBitmap, 0, newContent, 1);
            }
            return copyAndRemoveData(bit, idx, ctx);
        }

        if ((nodeMap & bit) != 0) {
            int nIdx = nodeIndex(bit);
            HamtNode<K, V> subNode = nodeAt(nIdx);
            int oldSize = subNode.size();
            HamtNode<K, V> newSubNode = subNode.remove(key, hash, shift + PARTITION_BITS, ctx);

            if (newSubNode == subNode) {
                return this;
            }

            int newSize = newSubNode.size();

            if (newSubNode instanceof EmptyNode) {
                // Sub-node became empty — remove it
                if (dataCount() == 0 && nodeCount() == 1) {
                    return EmptyNode.instance();
                }
                return copyAndRemoveNode(bit, nIdx, cachedSize + (newSize - oldSize), ctx);
            }

            if (newSubNode instanceof BitmapIndexedNode<K, V> bm
                    && bm.nodeCount() == 0 && bm.dataCount() == 1) {
                // Sub-node collapsed to single entry — inline it
                return copyAndMigrateFromNodeToData(bit, nIdx, bm.keyAt(0), bm.valueAt(0),
                        cachedSize + (newSize - oldSize), ctx);
            }

            return copyAndSetNode(nIdx, newSubNode, cachedSize + (newSize - oldSize), ctx);
        }

        // Key not present
        return this;
    }

    @Override
    public int size() {
        return cachedSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void forEach(BiConsumer<K, V> action) {
        int dc = dataCount();
        for (int i = 0; i < dc; i++) {
            action.accept((K) content[2 * i], (V) content[2 * i + 1]);
        }
        int nc = nodeCount();
        for (int i = 0; i < nc; i++) {
            int idx = content.length - 1 - i;
            ((HamtNode<K, V>) content[idx]).forEach(action);
        }
    }

    // --- Internal mutation helpers ---

    private boolean canEditInPlace(MutationContext ctx) {
        return ctx != null && ctx.canEdit(ownerContextId);
    }

    private BitmapIndexedNode<K, V> copyAndSetValue(int dataIdx, V value, MutationContext ctx) {
        if (canEditInPlace(ctx)) {
            content[dataIdx + 1] = value;
            return this;
        }
        Object[] newContent = content.clone();
        newContent[dataIdx + 1] = value;
        return new BitmapIndexedNode<>(dataMap, nodeMap, newContent, cachedSize, contextId(ctx));
    }

    private BitmapIndexedNode<K, V> copyAndSetNode(int nodeIdx, HamtNode<K, V> node, int newCachedSize, MutationContext ctx) {
        if (canEditInPlace(ctx)) {
            content[nodeIdx] = node;
            // Note: cachedSize is final, so we must create a new node even in transient mode
            // Actually, for true transient optimization we'd need mutable cachedSize;
            // for correctness, we always create a new node when size changes
        }
        Object[] newContent = content.clone();
        newContent[nodeIdx] = node;
        return new BitmapIndexedNode<>(dataMap, nodeMap, newContent, newCachedSize, contextId(ctx));
    }

    private BitmapIndexedNode<K, V> copyAndInsertData(int bit, K key, V value, MutationContext ctx) {
        // idx = slot position in the data section (already *2 for key+value pairs)
        int idx = dataIndex(bit);
        int dataSlotsCount = 2 * dataCount(); // total data slots (pairs of key+value)
        int nc = nodeCount();
        Object[] newContent = new Object[content.length + 2];

        // Copy data entries before insertion point
        System.arraycopy(content, 0, newContent, 0, idx);
        // Insert new key-value pair
        newContent[idx] = key;
        newContent[idx + 1] = value;
        // Copy remaining data entries after insertion point
        System.arraycopy(content, idx, newContent, idx + 2, dataSlotsCount - idx);
        // Copy node entries (at end of array)
        System.arraycopy(content, dataSlotsCount, newContent, dataSlotsCount + 2, nc);

        return new BitmapIndexedNode<>(dataMap | bit, nodeMap, newContent, cachedSize + 1, contextId(ctx));
    }

    private BitmapIndexedNode<K, V> copyAndRemoveData(int bit, int dataIdx, MutationContext ctx) {
        int dc = dataCount();
        int nc = nodeCount();
        Object[] newContent = new Object[content.length - 2];

        // Copy data entries, skipping the removed pair
        System.arraycopy(content, 0, newContent, 0, dataIdx);
        System.arraycopy(content, dataIdx + 2, newContent, dataIdx, 2 * dc - dataIdx - 2);
        // Copy node entries
        System.arraycopy(content, 2 * dc, newContent, 2 * (dc - 1), nc);

        return new BitmapIndexedNode<>(dataMap ^ bit, nodeMap, newContent, cachedSize - 1, contextId(ctx));
    }

    private HamtNode<K, V> copyAndMigrateFromDataToNode(int bit, int dataIdx, HamtNode<K, V> node, MutationContext ctx) {
        int dc = dataCount();
        int nc = nodeCount();
        // Remove 2 data slots, add 1 node slot: net change = -1
        int newLen = content.length - 1;
        Object[] newContent = new Object[newLen];

        int newDc = dc - 1;
        int newNc = nc + 1;
        int newNodeMap = nodeMap | bit;
        int nodeInsertionIdx = index(newNodeMap, bit);

        // Copy data section, skipping the removed data pair at dataIdx
        System.arraycopy(content, 0, newContent, 0, dataIdx);
        System.arraycopy(content, dataIdx + 2, newContent, dataIdx, 2 * dc - dataIdx - 2);

        // Build node section at end of new array
        // Nodes are stored at the end in reverse logical order: newContent[newLen-1-i] = node at logical index i
        for (int i = 0, j = 0; i < newNc; i++) {
            if (i == nodeInsertionIdx) {
                newContent[newLen - 1 - i] = node;
            } else {
                // Copy from old node section. Old node at logical index j is at content[content.length-1-j]
                newContent[newLen - 1 - i] = content[content.length - 1 - j];
                j++;
            }
        }

        int newDataMap = dataMap ^ bit;
        // Size changes because we replaced 1 inline entry with a subtree that has subNode.size() entries
        // The net change is (subNode.size() - 1)
        int newCachedSize = cachedSize + (node.size() - 1);
        return new BitmapIndexedNode<>(newDataMap, newNodeMap, newContent, newCachedSize, contextId(ctx));
    }

    private HamtNode<K, V> copyAndMigrateFromNodeToData(int bit, int nodeIdx, K key, V value, int newCachedSize, MutationContext ctx) {
        int dc = dataCount();
        int nc = nodeCount();
        // We gain 2 data slots and lose 1 node slot = net +1
        Object[] newContent = new Object[content.length + 1];

        int dataInsertIdx = dataIndex(bit);
        // Copy data before insertion
        System.arraycopy(content, 0, newContent, 0, dataInsertIdx);
        newContent[dataInsertIdx] = key;
        newContent[dataInsertIdx + 1] = value;
        System.arraycopy(content, dataInsertIdx, newContent, dataInsertIdx + 2, 2 * dc - dataInsertIdx);

        // Copy nodes, skipping the removed one
        int newNodeSectionStart = 2 * (dc + 1);
        int nodeLogicalIdx = index(nodeMap, bit);
        int oldNodeCount = nc;
        int newNodeCount = nc - 1;

        for (int i = 0, j = 0; i < oldNodeCount; i++) {
            if (i == nodeLogicalIdx) {
                continue; // skip the node we're inlining
            }
            newContent[newContent.length - 1 - j] = content[content.length - 1 - i];
            j++;
        }

        int newDataMap = dataMap | bit;
        int newNodeMap = nodeMap ^ bit;
        return new BitmapIndexedNode<>(newDataMap, newNodeMap, newContent, newCachedSize, contextId(ctx));
    }

    private BitmapIndexedNode<K, V> copyAndRemoveNode(int bit, int nodeIdx, int newCachedSize, MutationContext ctx) {
        int dc = dataCount();
        int nc = nodeCount();
        Object[] newContent = new Object[content.length - 1];

        // Copy data section
        System.arraycopy(content, 0, newContent, 0, 2 * dc);

        // Copy nodes, skipping the removed one
        int nodeLogicalIdx = index(nodeMap, bit);
        for (int i = 0, j = 0; i < nc; i++) {
            if (i == nodeLogicalIdx) continue;
            newContent[newContent.length - 1 - j] = content[content.length - 1 - i];
            j++;
        }

        return new BitmapIndexedNode<>(dataMap, nodeMap ^ bit, newContent, newCachedSize, contextId(ctx));
    }

    /**
     * Merges two entries that land in the same hash partition into a deeper sub-structure.
     */
    @SuppressWarnings("unchecked")
    private HamtNode<K, V> mergeTwoEntries(K key1, V val1, int hash1, K key2, V val2, int hash2, int shift) {
        if (shift > 30) {
            // Hash exhausted — use collision node
            return new CollisionNode<>(hash1, new Object[]{key1, val1, key2, val2});
        }

        int m1 = mask(hash1, shift);
        int m2 = mask(hash2, shift);

        if (m1 == m2) {
            // Same partition at this level — recurse deeper
            HamtNode<K, V> subNode = mergeTwoEntries(key1, val1, hash1, key2, val2, hash2, shift + PARTITION_BITS);
            int bit = 1 << m1;
            Object[] content = new Object[]{subNode};
            return new BitmapIndexedNode<>(0, bit, content, 2);
        }

        int bit1 = 1 << m1;
        int bit2 = 1 << m2;
        int newDataMap = bit1 | bit2;

        if (m1 < m2) {
            return new BitmapIndexedNode<>(newDataMap, 0, new Object[]{key1, val1, key2, val2}, 2);
        } else {
            return new BitmapIndexedNode<>(newDataMap, 0, new Object[]{key2, val2, key1, val1}, 2);
        }
    }

    private ArrayNode<K, V> upgradeToArrayNode(int shift) {
        @SuppressWarnings("unchecked")
        HamtNode<K, V>[] children = new HamtNode[PARTITION_SIZE];
        int totalSize = 0;

        // Convert all data entries to single-entry BitmapIndexedNodes at the next level
        int dc = dataCount();
        for (int i = 0; i < PARTITION_SIZE; i++) {
            int bit = 1 << i;
            if ((dataMap & bit) != 0) {
                int dIdx = dataIndex(bit);
                K key = keyAt(dIdx);
                V value = valueAt(dIdx);
                children[i] = BitmapIndexedNode.single(key, value, key.hashCode(), shift + PARTITION_BITS);
                totalSize += 1;
            } else if ((nodeMap & bit) != 0) {
                HamtNode<K, V> node = nodeAt(nodeIndex(bit));
                children[i] = node;
                totalSize += node.size();
            }
        }
        int childCount = dc + nodeCount();
        return new ArrayNode<>(children, childCount, totalSize);
    }

    private static long contextId(MutationContext ctx) {
        return ctx != null ? ctx.id() : 0;
    }
}
