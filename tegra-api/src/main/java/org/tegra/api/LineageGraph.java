package org.tegra.api;

import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tracks parent-child snapshot relationships across timelapses.
 * Each snapshot may have a parent (the version it was branched from)
 * and multiple children (versions branched from it).
 */
public final class LineageGraph {

    private final Map<ByteArray, ByteArray> parentMap;       // child -> parent
    private final Map<ByteArray, List<ByteArray>> childrenMap; // parent -> children

    public LineageGraph() {
        this.parentMap = new HashMap<>();
        this.childrenMap = new HashMap<>();
    }

    /**
     * Records a parent-child relationship between snapshots.
     *
     * @param parentId the parent snapshot ID
     * @param childId  the child snapshot ID
     */
    public void addRelationship(ByteArray parentId, ByteArray childId) {
        parentMap.put(childId, parentId);
        childrenMap.computeIfAbsent(parentId, k -> new ArrayList<>()).add(childId);
    }

    /**
     * Returns the parent of the given snapshot, or null if it is a root.
     */
    public ByteArray parent(ByteArray snapshotId) {
        return parentMap.get(snapshotId);
    }

    /**
     * Returns the children of the given snapshot.
     */
    public List<ByteArray> children(ByteArray snapshotId) {
        return childrenMap.getOrDefault(snapshotId, Collections.emptyList());
    }

    /**
     * Returns true if the snapshot has no parent (is a root).
     */
    public boolean isRoot(ByteArray snapshotId) {
        return !parentMap.containsKey(snapshotId);
    }

    /**
     * Returns all snapshot IDs tracked in the lineage.
     */
    public Set<ByteArray> allSnapshots() {
        Set<ByteArray> all = new java.util.HashSet<>(parentMap.keySet());
        all.addAll(childrenMap.keySet());
        return all;
    }

    /**
     * Returns the ancestry path from the given snapshot to the root.
     */
    public List<ByteArray> ancestryPath(ByteArray snapshotId) {
        List<ByteArray> path = new ArrayList<>();
        ByteArray current = snapshotId;
        while (current != null) {
            path.add(current);
            current = parentMap.get(current);
        }
        return path;
    }
}
