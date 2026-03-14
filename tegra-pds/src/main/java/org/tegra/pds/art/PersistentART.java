package org.tegra.pds.art;

import org.tegra.pds.common.ChangeType;
import org.tegra.pds.common.DiffEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Persistent Adaptive Radix Tree (pART) — the core data structure from the TEGRA paper.
 * <p>
 * Provides an immutable, versioned byte-array-keyed map with structural sharing via path-copying.
 * Each mutation returns a new PersistentART instance sharing unchanged subtrees with the old version.
 * <p>
 * Supports prefix iteration for efficient edge queries by source vertex.
 *
 * @param <V> the value type
 */
public final class PersistentART<V> {

    private final ArtNode<V> root;
    private final int size;

    PersistentART(ArtNode<V> root, int size) {
        this.root = root;
        this.size = size;
    }

    /**
     * Returns an empty pART.
     */
    public static <V> PersistentART<V> empty() {
        return new PersistentART<>(null, 0);
    }

    /**
     * Creates a PersistentART wrapping the given root node.
     * Public factory for use by serialization.
     */
    public static <V> PersistentART<V> fromRoot(ArtNode<V> root) {
        if (root == null) {
            return empty();
        }
        return new PersistentART<>(root, root.size());
    }

    /**
     * Inserts a key-value pair, returning a new pART with the mapping.
     * <p>
     * Internally, a null-terminator byte is appended to all keys to ensure
     * correct handling of variable-length keys where one key may be a prefix
     * of another. This terminator is stripped from results.
     */
    public PersistentART<V> insert(byte[] key, V value) {
        byte[] internalKey = appendTerminator(key);
        if (root == null) {
            return new PersistentART<>(new Leaf<>(internalKey, value), 1);
        }
        ArtNode<V> newRoot = root.insert(internalKey, 0, value);
        if (newRoot == root) {
            return this;
        }
        return new PersistentART<>(newRoot, newRoot.size());
    }

    /**
     * Looks up a value by key.
     *
     * @return the value, or null if not found
     */
    public V lookup(byte[] key) {
        if (root == null) {
            return null;
        }
        return root.lookup(appendTerminator(key), 0);
    }

    /**
     * Removes a key, returning a new pART without the mapping.
     */
    public PersistentART<V> remove(byte[] key) {
        if (root == null) {
            return this;
        }
        ArtNode<V> newRoot = root.remove(appendTerminator(key), 0);
        if (newRoot == root) {
            return this;
        }
        if (newRoot == null) {
            return empty();
        }
        return new PersistentART<>(newRoot, newRoot.size());
    }

    /**
     * Applies the given action to each key-value pair.
     * Keys returned have the internal terminator stripped.
     */
    public void forEach(BiConsumer<byte[], V> action) {
        if (root != null) {
            root.forEach((k, v) -> action.accept(stripTerminator(k), v));
        }
    }

    /**
     * Returns the number of entries.
     */
    public int size() {
        return size;
    }

    /**
     * Returns true if empty.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Iterates over all entries whose keys start with the given prefix.
     * This is critical for edge queries by source vertex in the TEGRA system.
     *
     * @param prefix the prefix to match
     * @param action consumer of (key, value) pairs
     */
    public void prefixIterator(byte[] prefix, BiConsumer<byte[], V> action) {
        if (root == null) {
            return;
        }
        // Do NOT append terminator to prefix — we want to match all keys starting with this prefix
        prefixSearch(root, prefix, 0, (k, v) -> action.accept(stripTerminator(k), v));
    }

    /**
     * Computes the diff between this pART and another.
     * Returns a list of DiffEntry objects describing changes from {@code this} to {@code other}.
     */
    public List<DiffEntry<byte[], V>> diff(PersistentART<V> other) {
        if (this.root == other.root) {
            return List.of();
        }

        List<DiffEntry<byte[], V>> diffs = new ArrayList<>();
        Map<ByteArrayKey, V> thisMap = new LinkedHashMap<>();
        Map<ByteArrayKey, V> otherMap = new LinkedHashMap<>();
        this.forEach((k, v) -> thisMap.put(new ByteArrayKey(k), v));
        other.forEach((k, v) -> otherMap.put(new ByteArrayKey(k), v));

        // Find removed and modified
        for (Map.Entry<ByteArrayKey, V> entry : thisMap.entrySet()) {
            ByteArrayKey key = entry.getKey();
            V thisValue = entry.getValue();
            V otherValue = otherMap.get(key);
            if (otherValue == null && !otherMap.containsKey(key)) {
                diffs.add(DiffEntry.removed(key.bytes(), thisValue));
            } else if (thisValue != null && !thisValue.equals(otherValue)) {
                diffs.add(DiffEntry.modified(key.bytes(), thisValue, otherValue));
            }
        }

        // Find added
        for (Map.Entry<ByteArrayKey, V> entry : otherMap.entrySet()) {
            ByteArrayKey key = entry.getKey();
            if (!thisMap.containsKey(key)) {
                diffs.add(DiffEntry.added(key.bytes(), entry.getValue()));
            }
        }

        return diffs;
    }

    /**
     * Returns the root node. Package-private for serialization.
     */
    /**
     * Returns the root node of this tree. May be null if the tree is empty.
     */
    public ArtNode<V> root() {
        return root;
    }

    // --- Prefix search ---

    private void prefixSearch(ArtNode<V> node, byte[] searchPrefix, int depth, BiConsumer<byte[], V> action) {
        if (node instanceof Leaf<V> leaf) {
            if (startsWith(leaf.key(), searchPrefix)) {
                action.accept(leaf.key(), leaf.value());
            }
            return;
        }

        // Internal node: match prefix bytes
        PrefixData nodePrefix = node.prefix();
        int prefixLen = nodePrefix != null ? nodePrefix.length() : 0;

        if (prefixLen > 0) {
            int matched = 0;
            int maxMatch = Math.min(prefixLen, searchPrefix.length - depth);
            int storeLen = (nodePrefix != null) ? nodePrefix.prefix().length : 0;
            while (matched < maxMatch) {
                if (matched < storeLen
                        && nodePrefix.at(matched) != searchPrefix[depth + matched]) {
                    return; // prefix mismatch
                }
                matched++;
            }

            if (matched < prefixLen) {
                // Search prefix was exhausted while still matching the node's prefix.
                // All entries under this node match the search prefix.
                node.forEach(action);
                return;
            }

            depth += prefixLen;
        }

        if (depth >= searchPrefix.length) {
            // The search prefix is fully consumed — all entries under this node match
            node.forEach(action);
            return;
        }

        // Search prefix has more bytes — follow the specific child
        byte nextByte = searchPrefix[depth];
        ArtNode<V> child = node.findChild(nextByte);
        if (child != null) {
            prefixSearch(child, searchPrefix, depth + 1, action);
        }
    }

    private static boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    // --- Key termination helpers ---

    /**
     * Appends a null terminator byte (0xFF) to ensure all keys have unique termination.
     * This prevents ambiguity when one key is a prefix of another.
     */
    private static byte[] appendTerminator(byte[] key) {
        byte[] terminated = new byte[key.length + 1];
        System.arraycopy(key, 0, terminated, 0, key.length);
        terminated[key.length] = (byte) 0xFF; // terminator
        return terminated;
    }

    /**
     * Strips the terminator byte from an internal key to produce the external key.
     */
    private static byte[] stripTerminator(byte[] internalKey) {
        if (internalKey.length == 0) return internalKey;
        byte[] key = new byte[internalKey.length - 1];
        System.arraycopy(internalKey, 0, key, 0, key.length);
        return key;
    }

    /**
     * Wrapper for byte[] to use as a Map key (with equals/hashCode based on contents).
     */
    private record ByteArrayKey(byte[] bytes) {
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof ByteArrayKey other)) return false;
            return Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
