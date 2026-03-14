package org.tegra.store;

import org.junit.jupiter.api.Test;
import org.tegra.api.Edge;
import org.tegra.api.GraphSnapshot;
import org.tegra.pds.hamt.PersistentHAMT;
import org.tegra.api.SnapshotId;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotStoreTest {

    @Test
    void testCreateAndGetSnapshot() {
        SnapshotStore<String, String> store = new SnapshotStore<>();
        SnapshotId id = SnapshotId.of("snap1");
        VersionRoot<String, String> root = createRootWithVertex(1L, "Alice");

        GraphSnapshot<String, String> snapshot = store.createSnapshot(id, root);
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.id()).isEqualTo(id);
        assertThat(snapshot.vertexCount()).isEqualTo(1);

        GraphSnapshot<String, String> retrieved = store.getSnapshot(id);
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.id()).isEqualTo(id);
        assertThat(retrieved.vertexCount()).isEqualTo(1);
    }

    @Test
    void testFindSnapshots() {
        SnapshotStore<String, String> store = new SnapshotStore<>();
        store.createSnapshot(SnapshotId.of("graph1_100"), createEmptyRoot());
        store.createSnapshot(SnapshotId.of("graph1_200"), createEmptyRoot());
        store.createSnapshot(SnapshotId.of("graph2_100"), createEmptyRoot());

        List<SnapshotId> found = store.findSnapshots("graph1");
        assertThat(found).hasSize(2);
        assertThat(found).extracting(SnapshotId::asString)
                .containsExactly("graph1_100", "graph1_200");
    }

    @Test
    void testSnapshotCount() {
        SnapshotStore<String, String> store = new SnapshotStore<>();
        assertThat(store.snapshotCount()).isZero();

        store.createSnapshot(SnapshotId.of("snap1"), createEmptyRoot());
        assertThat(store.snapshotCount()).isEqualTo(1);

        store.createSnapshot(SnapshotId.of("snap2"), createEmptyRoot());
        assertThat(store.snapshotCount()).isEqualTo(2);
    }

    @Test
    void testEviction() {
        int maxSnapshots = 3;
        SnapshotStore<String, String> store = new SnapshotStore<>(EvictionPolicy.lru(), maxSnapshots);

        // Create more snapshots than the maximum
        store.createSnapshot(SnapshotId.of("snap1"), createEmptyRoot());
        store.createSnapshot(SnapshotId.of("snap2"), createEmptyRoot());
        store.createSnapshot(SnapshotId.of("snap3"), createEmptyRoot());
        assertThat(store.snapshotCount()).isEqualTo(3);

        // Adding one more should trigger eviction of the oldest
        store.createSnapshot(SnapshotId.of("snap4"), createEmptyRoot());
        assertThat(store.snapshotCount()).isEqualTo(3);

        // The oldest (snap1) should have been evicted
        assertThat(store.getSnapshot(SnapshotId.of("snap1"))).isNull();
        // The newer ones should still be present
        assertThat(store.getSnapshot(SnapshotId.of("snap4"))).isNotNull();
    }

    private VersionRoot<String, String> createEmptyRoot() {
        return new VersionRoot<>(
                PersistentHAMT.empty(),
                PersistentHAMT.<Long, List<Edge<String>>>empty(),
                PersistentHAMT.<Long, List<Edge<String>>>empty(),
                Instant.now()
        );
    }

    private VersionRoot<String, String> createRootWithVertex(long id, String props) {
        PersistentHAMT<Long, String> vertices = PersistentHAMT.<Long, String>empty().put(id, props);
        return new VersionRoot<>(
                vertices,
                PersistentHAMT.<Long, List<Edge<String>>>empty(),
                PersistentHAMT.<Long, List<Edge<String>>>empty(),
                Instant.now()
        );
    }
}
