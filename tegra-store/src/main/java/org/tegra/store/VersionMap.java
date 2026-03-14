package org.tegra.store;

import org.tegra.pds.hamt.PersistentHAMT;
import org.tegra.api.SnapshotId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Thread-safe mapping from {@link SnapshotId} to {@link VersionRoot}.
 * <p>
 * Backed by a {@link PersistentHAMT} to allow efficient structural sharing
 * across concurrent readers while mutations are serialized.
 *
 * @param <V> vertex property type
 * @param <E> edge property type
 */
public final class VersionMap<V, E> {

    private PersistentHAMT<SnapshotId, VersionRoot<V, E>> roots;

    public VersionMap() {
        this.roots = PersistentHAMT.empty();
    }

    /**
     * Retrieves the version root for the given snapshot ID.
     *
     * @return the version root, or {@code null} if not present
     */
    public synchronized VersionRoot<V, E> get(SnapshotId id) {
        return roots.get(id);
    }

    /**
     * Stores a version root under the given snapshot ID.
     *
     * @return the committed snapshot ID
     */
    public synchronized SnapshotId commit(SnapshotId id, VersionRoot<V, E> root) {
        roots = roots.put(id, root);
        return id;
    }

    /**
     * Removes the version root for the given snapshot ID.
     */
    public synchronized void remove(SnapshotId id) {
        roots = roots.remove(id);
    }

    /**
     * Returns {@code true} if a version root exists for the given ID.
     */
    public boolean contains(SnapshotId id) {
        return roots.get(id) != null;
    }

    /**
     * Returns the number of stored versions.
     */
    public int size() {
        return roots.size();
    }

    /**
     * Finds all snapshot IDs whose string representation starts with the given prefix.
     *
     * @param prefix the prefix to match
     * @return sorted list of matching snapshot IDs
     */
    public List<SnapshotId> findByPrefix(String prefix) {
        List<SnapshotId> result = new ArrayList<>();
        SnapshotId prefixId = SnapshotId.of(prefix);
        roots.forEach((id, root) -> {
            if (id.hasPrefix(prefixId)) {
                result.add(id);
            }
        });
        result.sort(Comparator.naturalOrder());
        return result;
    }
}
