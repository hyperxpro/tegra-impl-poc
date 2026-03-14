package org.tegra.store.version;

import java.util.List;

/**
 * Interface for version index operations.
 * Supports get, put, remove, and matching primitives on version IDs.
 */
public interface VersionIndex {

    /**
     * Returns the version entry for the given ID, or null if not found.
     */
    VersionEntry get(ByteArray id);

    /**
     * Returns all version IDs matching the given prefix.
     */
    List<ByteArray> matchPrefix(ByteArray prefix);

    /**
     * Returns all version IDs in the lexicographic range [start, end).
     */
    List<ByteArray> matchRange(ByteArray start, ByteArray end);

    /**
     * Stores a version entry with the given ID.
     */
    void put(ByteArray id, VersionEntry entry);

    /**
     * Removes the version entry for the given ID.
     */
    void remove(ByteArray id);
}
