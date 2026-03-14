package org.tegra.api;

import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;
import org.tegra.pds.common.ChangeType;
import org.tegra.pds.common.DiffEntry;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.KeyCodec;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;
import org.tegra.store.version.VersionIdGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Core Timelapse API implementing the five operations from the TEGRA paper (Table 1):
 * save, retrieve, diff, expand, merge.
 */
public final class Timelapse {

    private final PartitionStore store;
    private final String graphId;
    private WorkingVersion currentWorking;
    private ByteArray currentBaseVersion;

    public Timelapse(PartitionStore store, String graphId) {
        this.store = store;
        this.graphId = graphId;
    }

    /**
     * Returns the underlying partition store.
     */
    public PartitionStore store() {
        return store;
    }

    /**
     * Returns the graph ID for this timelapse.
     */
    public String graphId() {
        return graphId;
    }

    /**
     * Sets the current working version (for mutations before save).
     */
    public void setCurrentWorking(WorkingVersion working, ByteArray baseVersionId) {
        this.currentWorking = working;
        this.currentBaseVersion = baseVersionId;
    }

    /**
     * Returns the current working version (if any).
     */
    public WorkingVersion currentWorking() {
        return currentWorking;
    }

    /**
     * Saves the current working version as a snapshot with the given ID.
     * Delegates to store.commit().
     *
     * @param id the version ID for the new snapshot
     * @return the version ID of the saved snapshot
     */
    public ByteArray save(ByteArray id) {
        if (currentWorking == null) {
            throw new IllegalStateException("No working version to save. Call branch() first.");
        }
        ByteArray committed = store.commit(currentWorking, id);
        currentWorking = null;
        currentBaseVersion = null;
        return committed;
    }

    /**
     * Saves the current working version with an auto-generated ID (using timestamp).
     *
     * @return the version ID of the saved snapshot
     */
    public ByteArray save() {
        ByteArray id = VersionIdGenerator.graphSnapshot(graphId, System.currentTimeMillis());
        return save(id);
    }

    /**
     * Retrieves a single snapshot by exact ID.
     *
     * @param id the version ID
     * @return the snapshot
     */
    public Snapshot retrieve(ByteArray id) {
        GraphView view = store.retrieve(id);
        return new Snapshot(view, id);
    }

    /**
     * Retrieves all snapshots matching a prefix.
     *
     * @param prefix the prefix to match
     * @return list of matching snapshots
     */
    public List<Snapshot> retrieveByPrefix(ByteArray prefix) {
        List<ByteArray> matchingIds = store.versionMap().matchPrefix(prefix);
        List<Snapshot> snapshots = new ArrayList<>();
        for (ByteArray id : matchingIds) {
            snapshots.add(retrieve(id));
        }
        return snapshots;
    }

    /**
     * Computes the difference between two snapshots.
     * Uses the pART diff mechanism that exploits structural sharing.
     *
     * @param a the first snapshot
     * @param b the second snapshot
     * @return the delta between the two snapshots
     */
    public Delta diff(Snapshot a, Snapshot b) {
        Set<Long> addedVertices = new HashSet<>();
        Set<Long> removedVertices = new HashSet<>();
        Set<Long> modifiedVertices = new HashSet<>();
        Set<EdgeKey> addedEdges = new HashSet<>();
        Set<EdgeKey> removedEdges = new HashSet<>();
        Set<EdgeKey> modifiedEdges = new HashSet<>();

        // Diff vertex trees
        ArtNode<VertexData> vRootA = a.graph().vertexRoot();
        ArtNode<VertexData> vRootB = b.graph().vertexRoot();

        PersistentART<VertexData> vertexArtA = PersistentART.fromRoot(vRootA);
        PersistentART<VertexData> vertexArtB = PersistentART.fromRoot(vRootB);

        List<DiffEntry<byte[], VertexData>> vertexDiffs = vertexArtA.diff(vertexArtB);
        for (DiffEntry<byte[], VertexData> entry : vertexDiffs) {
            long vertexId = KeyCodec.decodeVertexKey(entry.key());
            switch (entry.changeType()) {
                case ADDED -> addedVertices.add(vertexId);
                case REMOVED -> removedVertices.add(vertexId);
                case MODIFIED -> modifiedVertices.add(vertexId);
            }
        }

        // Diff edge trees
        ArtNode<EdgeData> eRootA = a.graph().edgeRoot();
        ArtNode<EdgeData> eRootB = b.graph().edgeRoot();

        PersistentART<EdgeData> edgeArtA = PersistentART.fromRoot(eRootA);
        PersistentART<EdgeData> edgeArtB = PersistentART.fromRoot(eRootB);

        List<DiffEntry<byte[], EdgeData>> edgeDiffs = edgeArtA.diff(edgeArtB);
        for (DiffEntry<byte[], EdgeData> entry : edgeDiffs) {
            EdgeKey edgeKey = KeyCodec.decodeEdgeKey(entry.key());
            switch (entry.changeType()) {
                case ADDED -> addedEdges.add(edgeKey);
                case REMOVED -> removedEdges.add(edgeKey);
                case MODIFIED -> modifiedEdges.add(edgeKey);
            }
        }

        return new Delta(addedVertices, removedVertices, modifiedVertices,
                addedEdges, removedEdges, modifiedEdges);
    }

    /**
     * Expands a set of candidate vertices to include their 1-hop neighbors.
     * Active vertices are the candidates, boundary vertices are their neighbors.
     *
     * @param candidates the candidate vertex IDs
     * @param snapshot   the snapshot to expand within
     * @return a SubgraphView with active and boundary vertices
     */
    public SubgraphView expand(Set<Long> candidates, Snapshot snapshot) {
        Set<Long> activeVertices = new HashSet<>(candidates);
        Set<Long> boundaryVertices = new HashSet<>();

        GraphView graph = snapshot.graph();
        for (long vertexId : candidates) {
            // Add outgoing edge destinations as boundary
            Iterator<EdgeData> outEdges = graph.outEdges(vertexId);
            while (outEdges.hasNext()) {
                EdgeData ed = outEdges.next();
                long dst = ed.edgeKey().dstId();
                if (!activeVertices.contains(dst)) {
                    boundaryVertices.add(dst);
                }
            }
            // Add incoming edge sources as boundary
            Iterator<EdgeData> inEdges = graph.inEdges(vertexId);
            while (inEdges.hasNext()) {
                EdgeData ed = inEdges.next();
                long src = ed.edgeKey().srcId();
                if (!activeVertices.contains(src)) {
                    boundaryVertices.add(src);
                }
            }
        }

        return new SubgraphView(activeVertices, boundaryVertices, graph);
    }

    /**
     * Creates a new snapshot as the union of two snapshots.
     * For vertices present in both, the merge function determines the result.
     *
     * @param a    first snapshot
     * @param b    second snapshot
     * @param func merge function for common vertices
     * @return the merged snapshot
     */
    public Snapshot merge(Snapshot a, Snapshot b, MergeFunction func) {
        // Collect all vertices from both snapshots
        Map<Long, VertexData> verticesA = new HashMap<>();
        Map<Long, VertexData> verticesB = new HashMap<>();

        Iterator<VertexData> itA = a.vertices();
        while (itA.hasNext()) {
            VertexData vd = itA.next();
            verticesA.put(vd.vertexId(), vd);
        }

        Iterator<VertexData> itB = b.vertices();
        while (itB.hasNext()) {
            VertexData vd = itB.next();
            verticesB.put(vd.vertexId(), vd);
        }

        // Create a new working version from an empty base
        ByteArray mergeVersionId = ByteArray.fromString(
                graphId + "_merge_" + System.currentTimeMillis());

        // Build merged vertex tree
        PersistentART<VertexData> mergedVertices = PersistentART.empty();

        // Add all vertices from A
        for (Map.Entry<Long, VertexData> entry : verticesA.entrySet()) {
            long id = entry.getKey();
            VertexData vdA = entry.getValue();
            VertexData vdB = verticesB.get(id);
            if (vdB != null) {
                // Present in both — merge
                VertexData merged = func.merge(vdA, vdB);
                mergedVertices = mergedVertices.insert(KeyCodec.encodeVertexKey(id), merged);
            } else {
                // Only in A
                mergedVertices = mergedVertices.insert(KeyCodec.encodeVertexKey(id), vdA);
            }
        }

        // Add vertices only in B
        for (Map.Entry<Long, VertexData> entry : verticesB.entrySet()) {
            if (!verticesA.containsKey(entry.getKey())) {
                mergedVertices = mergedVertices.insert(
                        KeyCodec.encodeVertexKey(entry.getKey()), entry.getValue());
            }
        }

        // Merge edges — union of both
        PersistentART<EdgeData> mergedEdges = PersistentART.empty();

        Iterator<EdgeData> edgesA = a.edges();
        while (edgesA.hasNext()) {
            EdgeData ed = edgesA.next();
            byte[] key = KeyCodec.encodeEdgeKey(
                    ed.edgeKey().srcId(), ed.edgeKey().dstId(), ed.edgeKey().discriminator());
            mergedEdges = mergedEdges.insert(key, ed);
        }

        Iterator<EdgeData> edgesB = b.edges();
        while (edgesB.hasNext()) {
            EdgeData ed = edgesB.next();
            byte[] key = KeyCodec.encodeEdgeKey(
                    ed.edgeKey().srcId(), ed.edgeKey().dstId(), ed.edgeKey().discriminator());
            // Only insert if not already present from A
            if (mergedEdges.lookup(key) == null) {
                mergedEdges = mergedEdges.insert(key, ed);
            }
        }

        // Commit merged result
        store.createInitialVersion(mergeVersionId);
        WorkingVersion working = store.branch(mergeVersionId);

        // Build a GraphView directly from the merged roots
        GraphView mergedView = new GraphView(mergedVertices.root(), mergedEdges.root(), mergeVersionId);

        // Commit using the merged trees
        store.evict(mergeVersionId); // remove the temporary empty version
        // Create a new version entry directly
        org.tegra.store.version.VersionEntry mergedEntry = new org.tegra.store.version.VersionEntry(
                mergedVertices.root(), mergedEdges.root(), System.currentTimeMillis(), null);
        store.versionMap().put(mergeVersionId, mergedEntry);

        return new Snapshot(mergedView, mergeVersionId);
    }
}
