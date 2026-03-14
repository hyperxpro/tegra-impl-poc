package org.tegra.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.store.version.ByteArray;
import org.tegra.store.version.VersionEntry;
import org.tegra.store.version.VersionMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for VersionMap: CRUD, prefix match, range match.
 */
class VersionMapTest {

    private VersionMap versionMap;

    @BeforeEach
    void setUp() {
        versionMap = new VersionMap();
    }

    @Test
    void putAndGet() {
        ByteArray id = ByteArray.fromString("TWTR_100");
        VersionEntry entry = new VersionEntry(null, null, 100L, null);

        versionMap.put(id, entry);

        assertThat(versionMap.get(id)).isNotNull();
        assertThat(versionMap.get(id).lastAccessTimestamp()).isEqualTo(100L);
    }

    @Test
    void getReturnsNullForMissing() {
        ByteArray id = ByteArray.fromString("missing");
        assertThat(versionMap.get(id)).isNull();
    }

    @Test
    void remove() {
        ByteArray id = ByteArray.fromString("TWTR_100");
        VersionEntry entry = new VersionEntry(null, null, 100L, null);

        versionMap.put(id, entry);
        versionMap.remove(id);

        assertThat(versionMap.get(id)).isNull();
    }

    @Test
    void size() {
        assertThat(versionMap.size()).isZero();

        versionMap.put(ByteArray.fromString("v1"), new VersionEntry(null, null, 1L, null));
        versionMap.put(ByteArray.fromString("v2"), new VersionEntry(null, null, 2L, null));

        assertThat(versionMap.size()).isEqualTo(2);
    }

    @Test
    void containsKey() {
        ByteArray id = ByteArray.fromString("TWTR_100");
        versionMap.put(id, new VersionEntry(null, null, 100L, null));

        assertThat(versionMap.containsKey(id)).isTrue();
        assertThat(versionMap.containsKey(ByteArray.fromString("other"))).isFalse();
    }

    @Test
    void matchPrefixReturnsSortedResults() {
        versionMap.put(ByteArray.fromString("TWTR_100"), new VersionEntry(null, null, 1L, null));
        versionMap.put(ByteArray.fromString("TWTR_200"), new VersionEntry(null, null, 2L, null));
        versionMap.put(ByteArray.fromString("TWTR_300"), new VersionEntry(null, null, 3L, null));
        versionMap.put(ByteArray.fromString("FB_100"), new VersionEntry(null, null, 4L, null));

        List<ByteArray> matches = versionMap.matchPrefix(ByteArray.fromString("TWTR_"));

        assertThat(matches).hasSize(3);
        assertThat(matches.get(0)).isEqualTo(ByteArray.fromString("TWTR_100"));
        assertThat(matches.get(1)).isEqualTo(ByteArray.fromString("TWTR_200"));
        assertThat(matches.get(2)).isEqualTo(ByteArray.fromString("TWTR_300"));
    }

    @Test
    void matchPrefixEmptyResult() {
        versionMap.put(ByteArray.fromString("TWTR_100"), new VersionEntry(null, null, 1L, null));

        List<ByteArray> matches = versionMap.matchPrefix(ByteArray.fromString("FB_"));

        assertThat(matches).isEmpty();
    }

    @Test
    void matchRange() {
        versionMap.put(ByteArray.fromString("TWTR_100"), new VersionEntry(null, null, 1L, null));
        versionMap.put(ByteArray.fromString("TWTR_200"), new VersionEntry(null, null, 2L, null));
        versionMap.put(ByteArray.fromString("TWTR_300"), new VersionEntry(null, null, 3L, null));
        versionMap.put(ByteArray.fromString("TWTR_400"), new VersionEntry(null, null, 4L, null));

        List<ByteArray> matches = versionMap.matchRange(
                ByteArray.fromString("TWTR_200"),
                ByteArray.fromString("TWTR_400"));

        assertThat(matches).hasSize(2);
        assertThat(matches.get(0)).isEqualTo(ByteArray.fromString("TWTR_200"));
        assertThat(matches.get(1)).isEqualTo(ByteArray.fromString("TWTR_300"));
    }

    @Test
    void matchRangeEmptyResult() {
        versionMap.put(ByteArray.fromString("TWTR_100"), new VersionEntry(null, null, 1L, null));

        List<ByteArray> matches = versionMap.matchRange(
                ByteArray.fromString("TWTR_200"),
                ByteArray.fromString("TWTR_300"));

        assertThat(matches).isEmpty();
    }

    @Test
    void allKeysReturnsSorted() {
        versionMap.put(ByteArray.fromString("c"), new VersionEntry(null, null, 3L, null));
        versionMap.put(ByteArray.fromString("a"), new VersionEntry(null, null, 1L, null));
        versionMap.put(ByteArray.fromString("b"), new VersionEntry(null, null, 2L, null));

        List<ByteArray> keys = versionMap.allKeys();

        assertThat(keys).hasSize(3);
        assertThat(keys.get(0)).isEqualTo(ByteArray.fromString("a"));
        assertThat(keys.get(1)).isEqualTo(ByteArray.fromString("b"));
        assertThat(keys.get(2)).isEqualTo(ByteArray.fromString("c"));
    }
}
