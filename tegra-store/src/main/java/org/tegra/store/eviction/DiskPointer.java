package org.tegra.store.eviction;

import java.nio.file.Path;

/**
 * Points to a serialized subtree on disk.
 *
 * @param filePath the path to the disk file
 * @param offset   the byte offset within the file
 * @param length   the byte length of the serialized data
 */
public record DiskPointer(Path filePath, long offset, int length) {
}
