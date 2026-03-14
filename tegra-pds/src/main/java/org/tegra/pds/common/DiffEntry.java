package org.tegra.pds.common;

import java.util.Objects;

/**
 * Represents a single difference between two versions of a persistent data structure.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @param key       the key that changed
 * @param oldValue  the previous value, or null if the key was added
 * @param newValue  the new value, or null if the key was removed
 * @param changeType the kind of change
 */
public record DiffEntry<K, V>(
        K key,
        V oldValue,
        V newValue,
        ChangeType changeType
) {
    public DiffEntry {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(changeType, "changeType must not be null");
    }

    public static <K, V> DiffEntry<K, V> added(K key, V newValue) {
        return new DiffEntry<>(key, null, newValue, ChangeType.ADDED);
    }

    public static <K, V> DiffEntry<K, V> removed(K key, V oldValue) {
        return new DiffEntry<>(key, oldValue, null, ChangeType.REMOVED);
    }

    public static <K, V> DiffEntry<K, V> modified(K key, V oldValue, V newValue) {
        return new DiffEntry<>(key, oldValue, newValue, ChangeType.MODIFIED);
    }
}
