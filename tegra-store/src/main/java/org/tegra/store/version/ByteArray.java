package org.tegra.store.version;

import java.util.Arrays;
import java.util.HexFormat;

/**
 * Value-based byte[] wrapper with proper equals, hashCode, and lexicographic ordering.
 * Used as version IDs throughout the DGSI system.
 */
public final class ByteArray implements Comparable<ByteArray> {

    private final byte[] data;

    public ByteArray(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        this.data = data.clone();
    }

    /**
     * Creates a ByteArray from a UTF-8 string.
     */
    public static ByteArray fromString(String s) {
        return new ByteArray(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Returns the underlying byte array (defensive copy).
     */
    public byte[] data() {
        return data.clone();
    }

    /**
     * Returns the raw byte array reference (no copy). For internal use.
     */
    byte[] rawData() {
        return data;
    }

    /**
     * Returns the length of the byte array.
     */
    public int length() {
        return data.length;
    }

    /**
     * Returns true if this ByteArray starts with the given prefix.
     */
    public boolean startsWith(ByteArray prefix) {
        if (prefix.data.length > data.length) {
            return false;
        }
        for (int i = 0; i < prefix.data.length; i++) {
            if (data[i] != prefix.data[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compareTo(ByteArray other) {
        int minLen = Math.min(this.data.length, other.data.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.compareUnsigned(this.data[i], other.data[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(this.data.length, other.data.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ByteArray other)) return false;
        return Arrays.equals(this.data, other.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        // Try to render as UTF-8 string if all bytes are printable ASCII
        boolean printable = true;
        for (byte b : data) {
            if (b < 0x20 || b > 0x7E) {
                printable = false;
                break;
            }
        }
        if (printable) {
            return new String(data, java.nio.charset.StandardCharsets.UTF_8);
        }
        return "ByteArray[" + HexFormat.of().formatHex(data) + "]";
    }
}
