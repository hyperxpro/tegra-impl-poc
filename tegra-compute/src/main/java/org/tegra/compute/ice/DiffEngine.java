package org.tegra.compute.ice;

import org.tegra.api.Delta;
import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;
import org.tegra.pds.common.DiffEntry;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.KeyCodec;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Computes the Delta between two GraphViews by diffing their underlying
 * pART trees. Exploits structural sharing: when two subtree roots are
 * the same reference, the entire subtree is skipped.
 */
public final class DiffEngine {

    /**
     * Computes the difference between the current and previous graph views.
     *
     * @param current  the current graph view
     * @param previous the previous graph view
     * @return the delta describing added, removed, and modified vertices and edges
     */
    public Delta computeDiff(GraphView current, GraphView previous) {
        Set<Long> addedVertices = new HashSet<>();
        Set<Long> removedVertices = new HashSet<>();
        Set<Long> modifiedVertices = new HashSet<>();
        Set<EdgeKey> addedEdges = new HashSet<>();
        Set<EdgeKey> removedEdges = new HashSet<>();
        Set<EdgeKey> modifiedEdges = new HashSet<>();

        // Diff vertex trees
        ArtNode<VertexData> vRootCurrent = current.vertexRoot();
        ArtNode<VertexData> vRootPrevious = previous.vertexRoot();

        // Structural sharing: if same root reference, no differences
        if (vRootCurrent != vRootPrevious) {
            PersistentART<VertexData> vertexArtCurrent = PersistentART.fromRoot(vRootCurrent);
            PersistentART<VertexData> vertexArtPrevious = PersistentART.fromRoot(vRootPrevious);

            List<DiffEntry<byte[], VertexData>> vertexDiffs = vertexArtCurrent.diff(vertexArtPrevious);
            for (DiffEntry<byte[], VertexData> entry : vertexDiffs) {
                long vertexId = KeyCodec.decodeVertexKey(entry.key());
                switch (entry.changeType()) {
                    case ADDED -> addedVertices.add(vertexId);
                    case REMOVED -> removedVertices.add(vertexId);
                    case MODIFIED -> modifiedVertices.add(vertexId);
                }
            }
        }

        // Diff edge trees
        ArtNode<EdgeData> eRootCurrent = current.edgeRoot();
        ArtNode<EdgeData> eRootPrevious = previous.edgeRoot();

        if (eRootCurrent != eRootPrevious) {
            PersistentART<EdgeData> edgeArtCurrent = PersistentART.fromRoot(eRootCurrent);
            PersistentART<EdgeData> edgeArtPrevious = PersistentART.fromRoot(eRootPrevious);

            List<DiffEntry<byte[], EdgeData>> edgeDiffs = edgeArtCurrent.diff(edgeArtPrevious);
            for (DiffEntry<byte[], EdgeData> entry : edgeDiffs) {
                EdgeKey edgeKey = KeyCodec.decodeEdgeKey(entry.key());
                switch (entry.changeType()) {
                    case ADDED -> addedEdges.add(edgeKey);
                    case REMOVED -> removedEdges.add(edgeKey);
                    case MODIFIED -> modifiedEdges.add(edgeKey);
                }
            }
        }

        return new Delta(addedVertices, removedVertices, modifiedVertices,
                addedEdges, removedEdges, modifiedEdges);
    }
}
