package org.tegra.pds.common;

/**
 * Represents a difference between two versions of a persistent data structure.
 *
 * @param key      the key where the difference occurs
 * @param oldValue the value in the old version (null for ADDED)
 * @param newValue the value in the new version (null for REMOVED)
 * @param type     the type of change
 * @param <K>      the key type
 * @param <V>      the value type
 */
public record DiffEntry<K, V>(K key, V oldValue, V newValue, ChangeType type) {

    public enum ChangeType {
        ADDED,
        REMOVED,
        MODIFIED
    }
}
