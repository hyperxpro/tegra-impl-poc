package org.tegra.compute;

import org.tegra.api.Edge;
import org.tegra.api.GraphSnapshot;
import org.tegra.api.MutableGraphView;
import org.tegra.pds.hamt.PersistentHAMT;
import org.tegra.api.SnapshotId;

import java.util.List;

/**
 * Test utility for building {@link GraphSnapshot} instances backed by the
 * actual PersistentHAMT implementation from tegra-api.
 *
 * @param <V> vertex value type
 * @param <E> edge value type
 */
public final class TestGraphBuilder<V, E> {

    private final MutableGraphView<V, E> mutable;
    private final SnapshotId snapshotId;

    @SuppressWarnings("unchecked")
    private TestGraphBuilder(SnapshotId snapshotId) {
        this.snapshotId = snapshotId;
        // Create an empty snapshot and get a mutable view from it
        GraphSnapshot<V, E> empty = GraphSnapshot.create(
                PersistentHAMT.empty(),
                PersistentHAMT.empty(),
                PersistentHAMT.empty(),
                snapshotId);
        this.mutable = empty.asMutable();
    }

    public static <V, E> TestGraphBuilder<V, E> create() {
        return new TestGraphBuilder<>(SnapshotId.of("test"));
    }

    public static <V, E> TestGraphBuilder<V, E> create(String id) {
        return new TestGraphBuilder<>(SnapshotId.of(id));
    }

    public TestGraphBuilder<V, E> addVertex(long id, V properties) {
        mutable.addVertex(id, properties);
        return this;
    }

    public TestGraphBuilder<V, E> addEdge(long src, long dst, E properties) {
        mutable.addEdge(src, dst, properties);
        return this;
    }

    public GraphSnapshot<V, E> build() {
        return mutable.toSnapshot(snapshotId);
    }
}
