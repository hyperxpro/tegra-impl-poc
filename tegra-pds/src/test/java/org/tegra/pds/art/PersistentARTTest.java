package org.tegra.pds.art;

import org.junit.jupiter.api.Test;
import org.tegra.pds.common.ChangeType;
import org.tegra.pds.common.DiffEntry;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PersistentART: CRUD, node transitions, prefix compression, path-copying, prefix iteration.
 */
class PersistentARTTest {

    // --- Helper methods ---

    private static byte[] key(long value) {
        byte[] k = new byte[8];
        ByteBuffer.wrap(k).putLong(value);
        return k;
    }

    private static byte[] key(String s) {
        return s.getBytes();
    }

    private static byte[] key(int... bytes) {
        byte[] k = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            k[i] = (byte) bytes[i];
        }
        return k;
    }

    // --- Basic CRUD ---

    @Test
    void emptyArtHasSizeZero() {
        PersistentART<String> art = PersistentART.empty();
        assertThat(art.size()).isZero();
        assertThat(art.isEmpty()).isTrue();
        assertThat(art.lookup(key(1))).isNull();
    }

    @Test
    void insertAndLookup() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1), "one");

        assertThat(art.size()).isEqualTo(1);
        assertThat(art.lookup(key(1))).isEqualTo("one");
        assertThat(art.lookup(key(2))).isNull();
    }

    @Test
    void insertMultiple() {
        PersistentART<Integer> art = PersistentART.empty();
        for (int i = 0; i < 100; i++) {
            art = art.insert(key(i), i);
        }

        assertThat(art.size()).isEqualTo(100);
        for (int i = 0; i < 100; i++) {
            assertThat(art.lookup(key(i))).isEqualTo(i);
        }
    }

    @Test
    void insertOverwrite() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1), "old")
                .insert(key(1), "new");

        assertThat(art.size()).isEqualTo(1);
        assertThat(art.lookup(key(1))).isEqualTo("new");
    }

    @Test
    void removeExisting() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1), "one")
                .insert(key(2), "two")
                .insert(key(3), "three");

        PersistentART<String> removed = art.remove(key(2));

        assertThat(removed.size()).isEqualTo(2);
        assertThat(removed.lookup(key(1))).isEqualTo("one");
        assertThat(removed.lookup(key(2))).isNull();
        assertThat(removed.lookup(key(3))).isEqualTo("three");
    }

    @Test
    void removeNonExistent() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1), "one");

        PersistentART<String> same = art.remove(key(999));
        assertThat(same).isSameAs(art);
    }

    @Test
    void removeLastEntry() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1), "one");

        PersistentART<String> empty = art.remove(key(1));
        assertThat(empty.isEmpty()).isTrue();
    }

    @Test
    void removeFromEmpty() {
        PersistentART<String> art = PersistentART.empty();
        PersistentART<String> same = art.remove(key(1));
        assertThat(same).isSameAs(art);
    }

    // --- Path-Copying / Structural Sharing ---

    @Test
    void pathCopyingPreservesOldVersion() {
        PersistentART<Integer> v1 = PersistentART.<Integer>empty()
                .insert(key(1), 1)
                .insert(key(2), 2);

        PersistentART<Integer> v2 = v1.insert(key(3), 3);

        // v1 unchanged
        assertThat(v1.size()).isEqualTo(2);
        assertThat(v1.lookup(key(3))).isNull();

        // v2 has new entry
        assertThat(v2.size()).isEqualTo(3);
        assertThat(v2.lookup(key(3))).isEqualTo(3);
    }

    @Test
    void pathCopyingOnRemove() {
        PersistentART<Integer> v1 = PersistentART.<Integer>empty()
                .insert(key(1), 1)
                .insert(key(2), 2)
                .insert(key(3), 3);

        PersistentART<Integer> v2 = v1.remove(key(2));

        assertThat(v1.size()).isEqualTo(3);
        assertThat(v1.lookup(key(2))).isEqualTo(2);

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.lookup(key(2))).isNull();
    }

    // --- Node Type Transitions ---

    @Test
    void node4ToNode16Transition() {
        // Insert 5 entries to force Node4 -> Node16 growth
        PersistentART<Integer> art = PersistentART.empty();
        for (int i = 0; i < 5; i++) {
            art = art.insert(key(0, i), i);
        }

        assertThat(art.size()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            assertThat(art.lookup(key(0, i))).isEqualTo(i);
        }
    }

    @Test
    void node16ToNode48Transition() {
        // Insert 17 entries with same first byte to force transitions
        PersistentART<Integer> art = PersistentART.empty();
        for (int i = 0; i < 17; i++) {
            art = art.insert(key(0, i), i);
        }

        assertThat(art.size()).isEqualTo(17);
        for (int i = 0; i < 17; i++) {
            assertThat(art.lookup(key(0, i))).isEqualTo(i);
        }
    }

    @Test
    void node48ToNode256Transition() {
        // Insert 49 entries with same first byte
        PersistentART<Integer> art = PersistentART.empty();
        for (int i = 0; i < 49; i++) {
            art = art.insert(key(0, i), i);
        }

        assertThat(art.size()).isEqualTo(49);
        for (int i = 0; i < 49; i++) {
            assertThat(art.lookup(key(0, i))).isEqualTo(i);
        }
    }

    @Test
    void reverseTransitionsOnRemove() {
        // Build up to Node256
        PersistentART<Integer> art = PersistentART.empty();
        for (int i = 0; i < 60; i++) {
            art = art.insert(key(0, i), i);
        }
        assertThat(art.size()).isEqualTo(60);

        // Remove down below Node256 threshold
        for (int i = 59; i >= 48; i--) {
            art = art.remove(key(0, i));
        }
        assertThat(art.size()).isEqualTo(48);

        // Remove down below Node48 threshold
        for (int i = 47; i >= 16; i--) {
            art = art.remove(key(0, i));
        }
        assertThat(art.size()).isEqualTo(16);

        // Remove down below Node16 threshold
        for (int i = 15; i >= 4; i--) {
            art = art.remove(key(0, i));
        }
        assertThat(art.size()).isEqualTo(4);

        // Verify remaining entries
        for (int i = 0; i < 4; i++) {
            assertThat(art.lookup(key(0, i))).isEqualTo(i);
        }
    }

    // --- Prefix Compression ---

    @Test
    void prefixCompressionOnSharedPaths() {
        // Insert keys that share a long common prefix
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key("prefix_a"), "a")
                .insert(key("prefix_b"), "b")
                .insert(key("prefix_c"), "c");

        assertThat(art.size()).isEqualTo(3);
        assertThat(art.lookup(key("prefix_a"))).isEqualTo("a");
        assertThat(art.lookup(key("prefix_b"))).isEqualTo("b");
        assertThat(art.lookup(key("prefix_c"))).isEqualTo("c");
    }

    @Test
    void prefixCompressionWithDivergentKeys() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key("abc"), "1")
                .insert(key("abd"), "2")
                .insert(key("xyz"), "3");

        assertThat(art.size()).isEqualTo(3);
        assertThat(art.lookup(key("abc"))).isEqualTo("1");
        assertThat(art.lookup(key("abd"))).isEqualTo("2");
        assertThat(art.lookup(key("xyz"))).isEqualTo("3");
    }

    // --- forEach ---

    @Test
    void forEachVisitsAllEntries() {
        PersistentART<Integer> art = PersistentART.empty();
        Map<Long, Integer> expected = new HashMap<>();

        for (int i = 0; i < 50; i++) {
            art = art.insert(key(i), i * 10);
            expected.put((long) i, i * 10);
        }

        Map<Long, Integer> collected = new HashMap<>();
        art.forEach((k, v) -> {
            long kLong = ByteBuffer.wrap(k).getLong();
            collected.put(kLong, v);
        });

        assertThat(collected).isEqualTo(expected);
    }

    // --- Prefix Iteration ---

    @Test
    void prefixIteratorFindsMatchingEntries() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1, 0, 1), "a")
                .insert(key(1, 0, 2), "b")
                .insert(key(1, 1, 1), "c")
                .insert(key(2, 0, 1), "d")
                .insert(key(2, 0, 2), "e");

        // Find all entries with prefix [1] (use new int[] to ensure varargs overload)
        List<String> results = new ArrayList<>();
        art.prefixIterator(new byte[]{1}, (k, v) -> results.add(v));

        assertThat(results).containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    void prefixIteratorOnEmptyTree() {
        PersistentART<String> art = PersistentART.empty();
        List<String> results = new ArrayList<>();
        art.prefixIterator(new byte[]{1}, (k, v) -> results.add(v));
        assertThat(results).isEmpty();
    }

    @Test
    void prefixIteratorNoMatch() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1, 0), "a")
                .insert(key(1, 1), "b");

        List<String> results = new ArrayList<>();
        art.prefixIterator(new byte[]{2}, (k, v) -> results.add(v));
        assertThat(results).isEmpty();
    }

    // --- Diff ---

    @Test
    void diffDetectsChanges() {
        PersistentART<String> v1 = PersistentART.<String>empty()
                .insert(key(1), "one")
                .insert(key(2), "two");

        PersistentART<String> v2 = v1
                .remove(key(1))
                .insert(key(2), "TWO")
                .insert(key(3), "three");

        List<DiffEntry<byte[], String>> diffs = v1.diff(v2);

        assertThat(diffs).hasSize(3);

        DiffEntry<byte[], String> removed = diffs.stream()
                .filter(d -> d.changeType() == ChangeType.REMOVED)
                .findFirst().orElseThrow();
        assertThat(removed.oldValue()).isEqualTo("one");

        DiffEntry<byte[], String> modified = diffs.stream()
                .filter(d -> d.changeType() == ChangeType.MODIFIED)
                .findFirst().orElseThrow();
        assertThat(modified.oldValue()).isEqualTo("two");
        assertThat(modified.newValue()).isEqualTo("TWO");

        DiffEntry<byte[], String> added = diffs.stream()
                .filter(d -> d.changeType() == ChangeType.ADDED)
                .findFirst().orElseThrow();
        assertThat(added.newValue()).isEqualTo("three");
    }

    @Test
    void diffSameInstanceIsEmpty() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key(1), "one");
        assertThat(art.diff(art)).isEmpty();
    }

    // --- Large Scale ---

    @Test
    void largeScaleInsertAndRemove() {
        PersistentART<Integer> art = PersistentART.empty();
        int n = 5_000;

        for (int i = 0; i < n; i++) {
            art = art.insert(key(i), i);
        }
        assertThat(art.size()).isEqualTo(n);

        for (int i = 0; i < n; i++) {
            assertThat(art.lookup(key(i))).isEqualTo(i);
        }

        // Remove even entries
        for (int i = 0; i < n; i += 2) {
            art = art.remove(key(i));
        }
        assertThat(art.size()).isEqualTo(n / 2);

        for (int i = 0; i < n; i++) {
            if (i % 2 == 0) {
                assertThat(art.lookup(key(i))).isNull();
            } else {
                assertThat(art.lookup(key(i))).isEqualTo(i);
            }
        }
    }

    @Test
    void variableLengthKeys() {
        PersistentART<String> art = PersistentART.<String>empty()
                .insert(key("a"), "1")
                .insert(key("ab"), "2")
                .insert(key("abc"), "3")
                .insert(key("abcd"), "4");

        assertThat(art.size()).isEqualTo(4);
        assertThat(art.lookup(key("a"))).isEqualTo("1");
        assertThat(art.lookup(key("ab"))).isEqualTo("2");
        assertThat(art.lookup(key("abc"))).isEqualTo("3");
        assertThat(art.lookup(key("abcd"))).isEqualTo("4");
    }
}
