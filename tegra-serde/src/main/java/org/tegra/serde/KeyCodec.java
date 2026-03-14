package org.tegra.serde;

import java.nio.ByteBuffer;

/**
 * Encodes and decodes vertex and edge keys to/from byte arrays
 * for use as ART keys. Encoding is big-endian for lexicographic ordering.
 * <p>
 * Vertex key format: 8 bytes (long, big-endian)
 * Edge key format: 8 bytes (srcId) + 8 bytes (dstId) + 2 bytes (discriminator) = 18 bytes
 */
public final class KeyCodec {

    /** Byte length of an encoded vertex key. */
    public static final int VERTEX_KEY_LENGTH = 8;

    /** Byte length of an encoded edge key. */
    public static final int EDGE_KEY_LENGTH = 18;

    private KeyCodec() {
        // utility class
    }

    /**
     * Encodes a vertex ID as an 8-byte big-endian key.
     */
    public static byte[] encodeVertexKey(long vertexId) {
        byte[] key = new byte[VERTEX_KEY_LENGTH];
        ByteBuffer.wrap(key).putLong(vertexId);
        return key;
    }

    /**
     * Decodes a vertex ID from an 8-byte big-endian key.
     */
    public static long decodeVertexKey(byte[] key) {
        if (key.length < VERTEX_KEY_LENGTH) {
            throw new IllegalArgumentException("Key too short for vertex: " + key.length);
        }
        return ByteBuffer.wrap(key).getLong();
    }

    /**
     * Encodes an edge key as 18 bytes: srcId(8) + dstId(8) + discriminator(2).
     */
    public static byte[] encodeEdgeKey(long srcId, long dstId, short discriminator) {
        byte[] key = new byte[EDGE_KEY_LENGTH];
        ByteBuffer buf = ByteBuffer.wrap(key);
        buf.putLong(srcId);
        buf.putLong(dstId);
        buf.putShort(discriminator);
        return key;
    }

    /**
     * Decodes an edge key from 18 bytes.
     */
    public static EdgeKey decodeEdgeKey(byte[] key) {
        if (key.length < EDGE_KEY_LENGTH) {
            throw new IllegalArgumentException("Key too short for edge: " + key.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(key);
        long srcId = buf.getLong();
        long dstId = buf.getLong();
        short discriminator = buf.getShort();
        return new EdgeKey(srcId, dstId, discriminator);
    }

    /**
     * Returns the source vertex prefix bytes (first 8 bytes) for edge prefix queries.
     * Used with ART prefix iteration to find all edges from a source vertex.
     */
    public static byte[] edgeSourcePrefix(long srcId) {
        return encodeVertexKey(srcId);
    }
}
