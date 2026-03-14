package org.tegra.pds.art;

import org.junit.jupiter.api.Test;
import org.tegra.pds.common.DiffEntry;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class PersistentARTTest {

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void testEmptyTree() {
        PersistentART<Integer> tree = PersistentART.empty();
        assertThat(tree.size()).isZero();
        assertThat(tree.get(key("any"))).isNull();
    }

    @Test
    void testPutAndGet() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("apple"), 1)
                .put(key("banana"), 2)
                .put(key("cherry"), 3);

        assertThat(tree.size()).isEqualTo(3);
        assertThat(tree.get(key("apple"))).isEqualTo(1);
        assertThat(tree.get(key("banana"))).isEqualTo(2);
        assertThat(tree.get(key("cherry"))).isEqualTo(3);
        assertThat(tree.get(key("date"))).isNull();
    }

    @Test
    void testPutOverwrite() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("key"), 1);
        assertThat(tree.get(key("key"))).isEqualTo(1);

        PersistentART<Integer> tree2 = tree.put(key("key"), 2);
        assertThat(tree2.get(key("key"))).isEqualTo(2);
        // Original unchanged
        assertThat(tree.get(key("key"))).isEqualTo(1);
    }

    @Test
    void testRemove() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("a"), 1)
                .put(key("b"), 2)
                .put(key("c"), 3);

        PersistentART<Integer> removed = tree.remove(key("b"));
        assertThat(removed.size()).isEqualTo(2);
        assertThat(removed.get(key("a"))).isEqualTo(1);
        assertThat(removed.get(key("b"))).isNull();
        assertThat(removed.get(key("c"))).isEqualTo(3);

        // Original unchanged
        assertThat(tree.size()).isEqualTo(3);
    }

    @Test
    void testStructuralSharing() {
        PersistentART<Integer> tree1 = PersistentART.<Integer>empty()
                .put(key("a"), 1)
                .put(key("b"), 2);

        PersistentART<Integer> tree2 = tree1.put(key("c"), 3);

        assertThat(tree2).isNotSameAs(tree1);
        assertThat(tree1.size()).isEqualTo(2);
        assertThat(tree1.get(key("c"))).isNull();
        assertThat(tree2.size()).isEqualTo(3);
    }

    @Test
    void testPrefixScan() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("app"), 1)
                .put(key("apple"), 2)
                .put(key("application"), 3)
                .put(key("banana"), 4)
                .put(key("band"), 5);

        List<Map.Entry<byte[], Integer>> results = tree.prefixScan(key("app"));
        assertThat(results).hasSize(3);

        Set<Integer> values = new HashSet<>();
        for (var entry : results) {
            values.add(entry.getValue());
        }
        assertThat(values).containsExactlyInAnyOrder(1, 2, 3);

        List<Map.Entry<byte[], Integer>> banResults = tree.prefixScan(key("ban"));
        assertThat(banResults).hasSize(2);
    }

    @Test
    void testNodeGrowth() {
        // Force Node4 -> Node16 -> Node48 -> Node256
        PersistentART<Integer> tree = PersistentART.empty();

        // Keys that differ at the first byte after common prefix to force node growth
        for (int i = 0; i < 260; i++) {
            byte[] k = new byte[]{(byte) i, 0x01};
            // For i > 255 we'd overflow, but 0-255 covers all byte values
            if (i < 256) {
                tree = tree.put(k, i);
            }
        }

        // Verify all values are retrievable
        for (int i = 0; i < 256; i++) {
            byte[] k = new byte[]{(byte) i, 0x01};
            assertThat(tree.get(k)).isEqualTo(i);
        }
        assertThat(tree.size()).isEqualTo(256);
    }

    @Test
    void testNodeShrink() {
        PersistentART<Integer> tree = PersistentART.empty();

        // Insert enough to grow, then remove to shrink
        for (int i = 0; i < 50; i++) {
            byte[] k = new byte[]{(byte) i, 0x01};
            tree = tree.put(k, i);
        }
        assertThat(tree.size()).isEqualTo(50);

        for (int i = 0; i < 40; i++) {
            byte[] k = new byte[]{(byte) i, 0x01};
            tree = tree.remove(k);
        }
        assertThat(tree.size()).isEqualTo(10);

        // Remaining values still accessible
        for (int i = 40; i < 50; i++) {
            byte[] k = new byte[]{(byte) i, 0x01};
            assertThat(tree.get(k)).isEqualTo(i);
        }
    }

    @Test
    void testPrefixCompression() {
        // Keys with long shared prefixes
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("prefix_shared_a"), 1)
                .put(key("prefix_shared_b"), 2)
                .put(key("prefix_shared_c"), 3);

        assertThat(tree.size()).isEqualTo(3);
        assertThat(tree.get(key("prefix_shared_a"))).isEqualTo(1);
        assertThat(tree.get(key("prefix_shared_b"))).isEqualTo(2);
        assertThat(tree.get(key("prefix_shared_c"))).isEqualTo(3);
        assertThat(tree.get(key("prefix_shared_d"))).isNull();
    }

    @Test
    void testLargeInsertions() {
        PersistentART<Integer> tree = PersistentART.empty();
        for (int i = 0; i < 2000; i++) {
            tree = tree.put(key("key-" + String.format("%05d", i)), i);
        }
        assertThat(tree.size()).isEqualTo(2000);

        for (int i = 0; i < 2000; i++) {
            assertThat(tree.get(key("key-" + String.format("%05d", i)))).isEqualTo(i);
        }
    }

    @Test
    void testDiff() {
        PersistentART<Integer> tree1 = PersistentART.<Integer>empty()
                .put(key("a"), 1)
                .put(key("b"), 2)
                .put(key("c"), 3);

        PersistentART<Integer> tree2 = PersistentART.<Integer>empty()
                .put(key("b"), 20) // modified
                .put(key("c"), 3)  // same
                .put(key("d"), 4); // added

        List<DiffEntry<byte[], Integer>> diffs = tree1.diff(tree2);

        assertThat(diffs).hasSize(3);

        var removed = diffs.stream()
                .filter(d -> d.type() == DiffEntry.ChangeType.REMOVED)
                .toList();
        assertThat(removed).hasSize(1);
        assertThat(removed.get(0).key()).isEqualTo(key("a"));

        var modified = diffs.stream()
                .filter(d -> d.type() == DiffEntry.ChangeType.MODIFIED)
                .toList();
        assertThat(modified).hasSize(1);
        assertThat(modified.get(0).key()).isEqualTo(key("b"));
        assertThat(modified.get(0).oldValue()).isEqualTo(2);
        assertThat(modified.get(0).newValue()).isEqualTo(20);

        var added = diffs.stream()
                .filter(d -> d.type() == DiffEntry.ChangeType.ADDED)
                .toList();
        assertThat(added).hasSize(1);
        assertThat(added.get(0).key()).isEqualTo(key("d"));
    }

    @Test
    void testForEach() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("x"), 1)
                .put(key("y"), 2)
                .put(key("z"), 3);

        Map<String, Integer> collected = new HashMap<>();
        tree.forEach((k, v) -> collected.put(new String(k, StandardCharsets.UTF_8), v));

        assertThat(collected).hasSize(3);
        assertThat(collected).containsEntry("x", 1);
        assertThat(collected).containsEntry("y", 2);
        assertThat(collected).containsEntry("z", 3);
    }

    @Test
    void testByteKeyOrdering() {
        // Byte keys should be iterable in byte order when using forEach on sorted nodes
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(new byte[]{0x03}, 3)
                .put(new byte[]{0x01}, 1)
                .put(new byte[]{0x02}, 2);

        List<Integer> values = new ArrayList<>();
        tree.forEach((k, v) -> values.add(v));

        // Node4 maintains sorted order
        assertThat(values).containsExactly(1, 2, 3);
    }

    @Test
    void testRemoveToEmpty() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("only"), 1);
        tree = tree.remove(key("only"));
        assertThat(tree.size()).isZero();
        assertThat(tree.get(key("only"))).isNull();
    }

    @Test
    void testRemoveNonExistent() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("a"), 1);
        PersistentART<Integer> result = tree.remove(key("nonexistent"));
        assertThat(result).isSameAs(tree);
    }

    @Test
    void testEmptyPrefixScan() {
        PersistentART<Integer> tree = PersistentART.<Integer>empty()
                .put(key("a"), 1)
                .put(key("b"), 2);

        // Empty prefix should match everything
        List<Map.Entry<byte[], Integer>> all = tree.prefixScan(new byte[0]);
        assertThat(all).hasSize(2);
    }
}
