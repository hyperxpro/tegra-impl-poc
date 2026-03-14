package org.tegra.api;

import java.nio.ByteBuffer;

/**
 * Composite edge identifier encoding source and destination vertex IDs.
 */
public record EdgeId(long src, long dst) implements Comparable<EdgeId> {

    /**
     * Serializes this edge ID to a 16-byte big-endian key.
     */
    public byte[] toKey() {
        byte[] key = new byte[16];
        ByteBuffer.wrap(key).putLong(src).putLong(dst);
        return key;
    }

    /**
     * Deserializes an edge ID from a 16-byte big-endian key.
     */
    public static EdgeId fromKey(byte[] key) {
        ByteBuffer buf = ByteBuffer.wrap(key);
        return new EdgeId(buf.getLong(), buf.getLong());
    }

    @Override
    public int compareTo(EdgeId other) {
        int c = Long.compare(src, other.src);
        return c != 0 ? c : Long.compare(dst, other.dst);
    }
}
