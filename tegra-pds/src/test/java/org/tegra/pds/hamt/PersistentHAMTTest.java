package org.tegra.pds.hamt;

import org.junit.jupiter.api.Test;
import org.tegra.pds.common.DiffEntry;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PersistentHAMTTest {

    @Test
    void testEmptyMap() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.empty();
        assertThat(map.size()).isZero();
        assertThat(map.get("any")).isNull();
        assertThat(map.containsKey("any")).isFalse();
    }

    @Test
    void testPutAndGet() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("one", 1)
                .put("two", 2)
                .put("three", 3);

        assertThat(map.size()).isEqualTo(3);
        assertThat(map.get("one")).isEqualTo(1);
        assertThat(map.get("two")).isEqualTo(2);
        assertThat(map.get("three")).isEqualTo(3);
        assertThat(map.get("four")).isNull();
    }

    @Test
    void testPutOverwrite() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("key", 1);
        assertThat(map.get("key")).isEqualTo(1);

        PersistentHAMT<String, Integer> map2 = map.put("key", 2);
        assertThat(map2.get("key")).isEqualTo(2);
        // Original unchanged
        assertThat(map.get("key")).isEqualTo(1);
    }

    @Test
    void testRemove() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2)
                .put("c", 3);

        PersistentHAMT<String, Integer> removed = map.remove("b");
        assertThat(removed.size()).isEqualTo(2);
        assertThat(removed.get("a")).isEqualTo(1);
        assertThat(removed.get("b")).isNull();
        assertThat(removed.get("c")).isEqualTo(3);

        // Original unchanged
        assertThat(map.size()).isEqualTo(3);
        assertThat(map.get("b")).isEqualTo(2);
    }

    @Test
    void testRemoveNonExistent() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("a", 1);
        PersistentHAMT<String, Integer> result = map.remove("nonexistent");
        assertThat(result).isSameAs(map);
    }

    @Test
    void testStructuralSharing() {
        PersistentHAMT<String, Integer> map1 = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2);

        PersistentHAMT<String, Integer> map2 = map1.put("c", 3);

        // They should be different instances
        assertThat(map2).isNotSameAs(map1);
        // Original unchanged
        assertThat(map1.size()).isEqualTo(2);
        assertThat(map1.get("c")).isNull();
        // New version has all entries
        assertThat(map2.size()).isEqualTo(3);
        assertThat(map2.get("a")).isEqualTo(1);
        assertThat(map2.get("b")).isEqualTo(2);
        assertThat(map2.get("c")).isEqualTo(3);
    }

    @Test
    void testLargeInsertions() {
        PersistentHAMT<Integer, String> map = PersistentHAMT.empty();
        for (int i = 0; i < 2000; i++) {
            map = map.put(i, "value-" + i);
        }
        assertThat(map.size()).isEqualTo(2000);

        for (int i = 0; i < 2000; i++) {
            assertThat(map.get(i)).isEqualTo("value-" + i);
        }
    }

    @Test
    void testCollisionHandling() {
        // Create keys with the same hashCode
        String key1 = "Aa"; // "Aa".hashCode() == "BB".hashCode() in Java
        String key2 = "BB";
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());

        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put(key1, 1)
                .put(key2, 2);

        assertThat(map.size()).isEqualTo(2);
        assertThat(map.get(key1)).isEqualTo(1);
        assertThat(map.get(key2)).isEqualTo(2);

        // Remove one colliding key
        PersistentHAMT<String, Integer> removed = map.remove(key1);
        assertThat(removed.size()).isEqualTo(1);
        assertThat(removed.get(key1)).isNull();
        assertThat(removed.get(key2)).isEqualTo(2);
    }

    @Test
    void testDiff() {
        PersistentHAMT<String, Integer> map1 = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2)
                .put("c", 3);

        PersistentHAMT<String, Integer> map2 = PersistentHAMT.<String, Integer>empty()
                .put("b", 20) // modified
                .put("c", 3)  // same
                .put("d", 4); // added

        List<DiffEntry<String, Integer>> diffs = map1.diff(map2);

        assertThat(diffs).hasSize(3);

        var removed = diffs.stream()
                .filter(d -> d.type() == DiffEntry.ChangeType.REMOVED)
                .toList();
        assertThat(removed).hasSize(1);
        assertThat(removed.get(0).key()).isEqualTo("a");

        var modified = diffs.stream()
                .filter(d -> d.type() == DiffEntry.ChangeType.MODIFIED)
                .toList();
        assertThat(modified).hasSize(1);
        assertThat(modified.get(0).key()).isEqualTo("b");
        assertThat(modified.get(0).oldValue()).isEqualTo(2);
        assertThat(modified.get(0).newValue()).isEqualTo(20);

        var added = diffs.stream()
                .filter(d -> d.type() == DiffEntry.ChangeType.ADDED)
                .toList();
        assertThat(added).hasSize(1);
        assertThat(added.get(0).key()).isEqualTo("d");
    }

    @Test
    void testTransientBatchMutation() {
        PersistentHAMT<Integer, String> initial = PersistentHAMT.<Integer, String>empty()
                .put(1, "one")
                .put(2, "two");

        TransientHAMT<Integer, String> transient_ = initial.asTransient();
        for (int i = 3; i <= 100; i++) {
            transient_.put(i, "val-" + i);
        }
        transient_.remove(1);

        PersistentHAMT<Integer, String> result = transient_.persistent();
        assertThat(result.size()).isEqualTo(99);
        assertThat(result.get(1)).isNull();
        assertThat(result.get(2)).isEqualTo("two");
        assertThat(result.get(50)).isEqualTo("val-50");

        // Original unchanged
        assertThat(initial.size()).isEqualTo(2);
        assertThat(initial.get(1)).isEqualTo("one");

        // Transient should not be usable after persistent()
        assertThatThrownBy(() -> transient_.put(200, "x"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testForEach() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2)
                .put("c", 3);

        Map<String, Integer> collected = new HashMap<>();
        map.forEach(collected::put);

        assertThat(collected).hasSize(3);
        assertThat(collected).containsEntry("a", 1);
        assertThat(collected).containsEntry("b", 2);
        assertThat(collected).containsEntry("c", 3);
    }

    @Test
    void testContainsKey() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("exists", 42);

        assertThat(map.containsKey("exists")).isTrue();
        assertThat(map.containsKey("missing")).isFalse();
    }

    @Test
    void testNullValue() {
        PersistentHAMT<String, String> map = PersistentHAMT.<String, String>empty()
                .put("key", null);

        // Note: containsKey uses get() which returns null for both missing keys and null values
        // This is a known trade-off in this simple implementation
        assertThat(map.get("key")).isNull();
        assertThat(map.size()).isEqualTo(1);

        // Verify the entry exists via forEach
        Map<String, String> collected = new HashMap<>();
        map.forEach(collected::put);
        assertThat(collected).containsKey("key");
        assertThat(collected.get("key")).isNull();
    }

    @Test
    void testDiffSameMap() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("a", 1);
        assertThat(map.diff(map)).isEmpty();
    }

    @Test
    void testRemoveAll() {
        PersistentHAMT<String, Integer> map = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2);

        map = map.remove("a").remove("b");
        assertThat(map.size()).isZero();
        assertThat(map.get("a")).isNull();
        assertThat(map.get("b")).isNull();
    }

    @Test
    void testManyCollisions() {
        // Create multiple keys with the same hash code
        // "Aa" and "BB" have the same hashCode, as do "AaBB" and "BBAa", etc.
        PersistentHAMT<CollisionKey, Integer> map = PersistentHAMT.empty();
        for (int i = 0; i < 10; i++) {
            map = map.put(new CollisionKey(i), i);
        }
        assertThat(map.size()).isEqualTo(10);
        for (int i = 0; i < 10; i++) {
            assertThat(map.get(new CollisionKey(i))).isEqualTo(i);
        }
    }

    /** A key that always returns the same hashCode, forcing collisions. */
    record CollisionKey(int id) {
        @Override
        public int hashCode() {
            return 42; // all instances collide
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof CollisionKey ck && ck.id == this.id;
        }
    }
}
