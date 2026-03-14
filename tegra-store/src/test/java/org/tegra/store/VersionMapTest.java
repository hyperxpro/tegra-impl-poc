package org.tegra.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.api.Edge;
import org.tegra.pds.hamt.PersistentHAMT;
import org.tegra.api.SnapshotId;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VersionMapTest {

    private VersionMap<String, String> versionMap;

    @BeforeEach
    void setUp() {
        versionMap = new VersionMap<>();
    }

    @Test
    void testEmptyVersionMap() {
        assertThat(versionMap.size()).isZero();
        assertThat(versionMap.get(SnapshotId.of("nonexistent"))).isNull();
    }

    @Test
    void testCommitAndGet() {
        SnapshotId id = SnapshotId.of("snap1");
        VersionRoot<String, String> root = createRoot();

        versionMap.commit(id, root);

        assertThat(versionMap.get(id)).isNotNull();
        assertThat(versionMap.get(id).timestamp()).isEqualTo(root.timestamp());
        assertThat(versionMap.size()).isEqualTo(1);
    }

    @Test
    void testContains() {
        SnapshotId id = SnapshotId.of("snap1");
        assertThat(versionMap.contains(id)).isFalse();

        versionMap.commit(id, createRoot());
        assertThat(versionMap.contains(id)).isTrue();
    }

    @Test
    void testFindByPrefix() {
        versionMap.commit(SnapshotId.of("graph1_100"), createRoot());
        versionMap.commit(SnapshotId.of("graph1_200"), createRoot());
        versionMap.commit(SnapshotId.of("graph2_100"), createRoot());

        List<SnapshotId> result = versionMap.findByPrefix("graph1");
        assertThat(result).hasSize(2);
        assertThat(result).extracting(SnapshotId::asString)
                .containsExactly("graph1_100", "graph1_200");
    }

    @Test
    void testMultipleVersions() {
        for (int i = 0; i < 10; i++) {
            versionMap.commit(SnapshotId.of("snap" + i), createRoot());
        }
        assertThat(versionMap.size()).isEqualTo(10);
    }

    @Test
    void testOverwriteVersion() {
        SnapshotId id = SnapshotId.of("snap1");
        Instant first = Instant.ofEpochSecond(1000);
        Instant second = Instant.ofEpochSecond(2000);

        versionMap.commit(id, createRootWithTimestamp(first));
        assertThat(versionMap.get(id).timestamp()).isEqualTo(first);

        versionMap.commit(id, createRootWithTimestamp(second));
        assertThat(versionMap.get(id).timestamp()).isEqualTo(second);
        assertThat(versionMap.size()).isEqualTo(1);
    }

    private VersionRoot<String, String> createRoot() {
        return createRootWithTimestamp(Instant.now());
    }

    private VersionRoot<String, String> createRootWithTimestamp(Instant timestamp) {
        return new VersionRoot<>(
                PersistentHAMT.empty(),
                PersistentHAMT.<Long, List<Edge<String>>>empty(),
                PersistentHAMT.<Long, List<Edge<String>>>empty(),
                timestamp
        );
    }
}
