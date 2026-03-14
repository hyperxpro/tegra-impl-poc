package org.tegra.api;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotIdTest {

    @Test
    void testCreation() {
        Instant ts = Instant.ofEpochSecond(1000);
        SnapshotId id = SnapshotId.of("graph1", ts);
        assertThat(id.asString()).isEqualTo("graph1_1000");
    }

    @Test
    void testFromString() {
        SnapshotId id = SnapshotId.of("my-snapshot");
        assertThat(id.asString()).isEqualTo("my-snapshot");
    }

    @Test
    void testOfIteration() {
        SnapshotId base = SnapshotId.of("graph1_1000");
        SnapshotId iter = SnapshotId.ofIteration(base, "pagerank", 3);
        assertThat(iter.asString()).isEqualTo("graph1_1000_pagerank_3");
    }

    @Test
    void testHasPrefix() {
        SnapshotId full = SnapshotId.of("graph1_1000_pagerank_3");
        SnapshotId prefix = SnapshotId.of("graph1");
        assertThat(full.hasPrefix(prefix)).isTrue();

        SnapshotId noMatch = SnapshotId.of("graph2");
        assertThat(full.hasPrefix(noMatch)).isFalse();

        // A prefix longer than the ID should not match
        SnapshotId longer = SnapshotId.of("graph1_1000_pagerank_3_extra");
        assertThat(full.hasPrefix(longer)).isFalse();
    }

    @Test
    void testCompareTo() {
        SnapshotId a = SnapshotId.of("aaa");
        SnapshotId b = SnapshotId.of("bbb");
        assertThat(a.compareTo(b)).isLessThan(0);
        assertThat(b.compareTo(a)).isGreaterThan(0);
        assertThat(a.compareTo(SnapshotId.of("aaa"))).isEqualTo(0);
    }

    @Test
    void testEqualsAndHashCode() {
        SnapshotId id1 = SnapshotId.of("test");
        SnapshotId id2 = SnapshotId.of("test");
        SnapshotId id3 = SnapshotId.of("other");

        assertThat(id1).isEqualTo(id2);
        assertThat(id1).isNotEqualTo(id3);
        assertThat(id1.hashCode()).isEqualTo(id2.hashCode());
    }

    @Test
    void testAsString() {
        SnapshotId id = SnapshotId.of("hello-world");
        assertThat(id.asString()).isEqualTo("hello-world");
        assertThat(id.toString()).isEqualTo("hello-world");
    }
}
