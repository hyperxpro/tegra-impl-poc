package org.tegra.pds.art;

import java.util.Arrays;

/**
 * Stores the compressed prefix for an ART internal node (path compression / lazy expansion).
 * <p>
 * When a chain of single-child internal nodes would exist (e.g., a path segment
 * where each node has exactly one child), the prefix bytes are stored here
 * to collapse the chain into a single node.
 *
 * @param prefix the prefix bytes (may be longer than {@code length}; only first {@code length} bytes are valid)
 * @param length the number of valid prefix bytes
 */
public record PrefixData(byte[] prefix, int length) {

    /**
     * Maximum stored prefix length. Beyond this, we rely on leaf key comparison
     * for disambiguation (pessimistic path compression).
     */
    public static final int MAX_PREFIX_LENGTH = 8;

    public static final PrefixData EMPTY = new PrefixData(new byte[0], 0);

    public PrefixData {
        if (length < 0) {
            throw new IllegalArgumentException("prefix length must be non-negative");
        }
    }

    /**
     * Returns the prefix byte at the given index.
     */
    public byte at(int index) {
        return prefix[index];
    }

    /**
     * Checks how many bytes of this prefix match the given key starting at {@code depth}.
     *
     * @param key   the key to match against
     * @param depth the starting depth in the key
     * @return the number of matching prefix bytes
     */
    public int matchPrefix(byte[] key, int depth) {
        int maxCheck = Math.min(length, Math.min(MAX_PREFIX_LENGTH, key.length - depth));
        int matched = 0;
        for (int i = 0; i < maxCheck; i++) {
            if (prefix[i] != key[depth + i]) {
                break;
            }
            matched++;
        }
        return matched;
    }

    /**
     * Creates a new PrefixData by prepending a byte and this prefix (up to MAX_PREFIX_LENGTH).
     */
    public PrefixData prepend(byte b) {
        int newLen = Math.min(length + 1, MAX_PREFIX_LENGTH);
        byte[] newPrefix = new byte[newLen];
        newPrefix[0] = b;
        System.arraycopy(prefix, 0, newPrefix, 1, newLen - 1);
        return new PrefixData(newPrefix, newLen);
    }

    /**
     * Creates a PrefixData from key bytes in range [from, to).
     */
    public static PrefixData fromKey(byte[] key, int from, int to) {
        int len = to - from;
        int storedLen = Math.min(len, MAX_PREFIX_LENGTH);
        byte[] prefix = new byte[storedLen];
        System.arraycopy(key, from, prefix, 0, storedLen);
        return new PrefixData(prefix, len);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PrefixData other)) return false;
        if (this.length != other.length) return false;
        int cmpLen = Math.min(length, MAX_PREFIX_LENGTH);
        for (int i = 0; i < cmpLen; i++) {
            if (this.prefix[i] != other.prefix[i]) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int cmpLen = Math.min(length, MAX_PREFIX_LENGTH);
        int h = length;
        for (int i = 0; i < cmpLen; i++) {
            h = h * 31 + prefix[i];
        }
        return h;
    }

    @Override
    public String toString() {
        int cmpLen = Math.min(length, MAX_PREFIX_LENGTH);
        return "PrefixData[len=" + length + ", bytes=" + Arrays.toString(Arrays.copyOf(prefix, cmpLen)) + "]";
    }
}
