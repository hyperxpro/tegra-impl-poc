package org.tegra.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Append-only log of graph mutations for replay and audit.
 * <p>
 * Mutation entries are modeled as a sealed hierarchy so pattern matching
 * with {@code switch} expressions is exhaustive.
 *
 * @param <V> vertex property type
 * @param <E> edge property type
 */
public final class MutationLog<V, E> {

    /**
     * Sealed interface for mutation log entries.
     */
    public sealed interface MutationEntry<V, E> {

        record AddVertex<V, E>(long id, V properties) implements MutationEntry<V, E> {}

        record RemoveVertex<V, E>(long id) implements MutationEntry<V, E> {}

        record AddEdge<V, E>(long src, long dst, E properties) implements MutationEntry<V, E> {}

        record RemoveEdge<V, E>(long src, long dst) implements MutationEntry<V, E> {}
    }

    private final List<MutationEntry<V, E>> entries = new ArrayList<>();

    /**
     * Appends a mutation entry to the log.
     */
    public void log(MutationEntry<V, E> entry) {
        entries.add(entry);
    }

    /**
     * Returns an unmodifiable view of all logged entries.
     */
    public List<MutationEntry<V, E>> entries() {
        return Collections.unmodifiableList(entries);
    }

    /**
     * Returns the number of entries in the log.
     */
    public int size() {
        return entries.size();
    }

    /**
     * Clears all entries from the log.
     */
    public void clear() {
        entries.clear();
    }
}
