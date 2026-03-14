package org.tegra.store.version;

import org.tegra.pds.art.ArtNode;
import org.tegra.serde.EdgeData;
import org.tegra.serde.VertexData;

/**
 * A version entry in the DGSI version map.
 * Maps a version ID to a pair of pART roots (vertex tree and edge tree),
 * along with metadata for LRU eviction and mutation log pointers.
 *
 * @param vertexRoot          root of the vertex pART (may be null for empty graph)
 * @param edgeRoot            root of the edge pART (may be null for empty graph)
 * @param lastAccessTimestamp timestamp of last access (for LRU eviction)
 * @param logPointer          pointer to the mutation log file (nullable)
 */
public record VersionEntry(
        ArtNode<VertexData> vertexRoot,
        ArtNode<EdgeData> edgeRoot,
        long lastAccessTimestamp,
        ByteArray logPointer
) {

    /**
     * Returns a copy of this entry with the lastAccessTimestamp updated.
     */
    public VersionEntry withAccessTimestamp(long timestamp) {
        return new VersionEntry(vertexRoot, edgeRoot, timestamp, logPointer);
    }

    /**
     * Returns a copy of this entry with the log pointer set.
     */
    public VersionEntry withLogPointer(ByteArray logPointer) {
        return new VersionEntry(vertexRoot, edgeRoot, lastAccessTimestamp, logPointer);
    }
}
