package org.tegra.store.log;

import org.tegra.store.version.ByteArray;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Append-only log of graph mutations.
 * Mutations are logged between committed snapshots to enable
 * reconstruction of intermediate graph states.
 * <p>
 * This implementation uses in-memory storage.
 * A production implementation would write to disk files.
 */
public final class MutationLog {

    private final ConcurrentHashMap<ByteArray, List<GraphMutation>> logs;

    public MutationLog() {
        this.logs = new ConcurrentHashMap<>();
    }

    /**
     * Opens a writer for the given version.
     */
    public MutationLogWriter openWriter(ByteArray versionId) {
        List<GraphMutation> log = logs.computeIfAbsent(versionId, k -> new ArrayList<>());
        return new MutationLogWriter(log);
    }

    /**
     * Opens a reader for the given version.
     */
    public MutationLogReader openReader(ByteArray versionId) {
        List<GraphMutation> log = logs.getOrDefault(versionId, List.of());
        return new MutationLogReader(log);
    }

    /**
     * Writer for appending mutations to a log.
     */
    public static final class MutationLogWriter {
        private final List<GraphMutation> log;

        MutationLogWriter(List<GraphMutation> log) {
            this.log = log;
        }

        /**
         * Appends a mutation to the log.
         */
        public synchronized void append(GraphMutation mutation) {
            log.add(mutation);
        }

        /**
         * Returns the number of mutations written.
         */
        public synchronized int size() {
            return log.size();
        }
    }

    /**
     * Reader for reading mutations from a log.
     */
    public static final class MutationLogReader {
        private final List<GraphMutation> log;
        private int position;

        MutationLogReader(List<GraphMutation> log) {
            this.log = new ArrayList<>(log); // snapshot
            this.position = 0;
        }

        /**
         * Returns true if there are more mutations to read.
         */
        public boolean hasNext() {
            return position < log.size();
        }

        /**
         * Returns the next mutation.
         */
        public GraphMutation next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException("No more mutations");
            }
            return log.get(position++);
        }

        /**
         * Returns all remaining mutations.
         */
        public List<GraphMutation> readAll() {
            List<GraphMutation> remaining = new ArrayList<>(log.subList(position, log.size()));
            position = log.size();
            return remaining;
        }

        /**
         * Returns the total number of mutations in this log.
         */
        public int size() {
            return log.size();
        }
    }
}
