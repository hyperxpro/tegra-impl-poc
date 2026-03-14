package org.tegra.pds.hamt;

import org.junit.jupiter.api.Test;
import org.tegra.pds.common.ChangeType;
import org.tegra.pds.common.DiffEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PersistentHAMT: CRUD operations, structural sharing, diff, and transient batch.
 */
class PersistentHAMTTest {

    @Test
    void emptyHamtHasSizeZero() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.empty();
        assertThat(hamt.size()).isZero();
        assertThat(hamt.isEmpty()).isTrue();
        assertThat(hamt.get("key")).isNull();
    }

    @Test
    void putAndGet() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.empty();
        hamt = hamt.put("hello", 42);

        assertThat(hamt.size()).isEqualTo(1);
        assertThat(hamt.get("hello")).isEqualTo(42);
        assertThat(hamt.get("world")).isNull();
    }

    @Test
    void putMultipleKeys() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.empty();
        for (int i = 0; i < 100; i++) {
            hamt = hamt.put("key" + i, i);
        }

        assertThat(hamt.size()).isEqualTo(100);
        for (int i = 0; i < 100; i++) {
            assertThat(hamt.get("key" + i)).isEqualTo(i);
        }
    }

    @Test
    void putOverwritesExistingKey() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.<String, Integer>empty()
                .put("key", 1)
                .put("key", 2);

        assertThat(hamt.size()).isEqualTo(1);
        assertThat(hamt.get("key")).isEqualTo(2);
    }

    @Test
    void putSameValueReturnsSameInstance() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.<String, Integer>empty()
                .put("key", 42);

        PersistentHAMT<String, Integer> same = hamt.put("key", 42);
        assertThat(same).isSameAs(hamt);
    }

    @Test
    void removeExistingKey() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2)
                .put("c", 3);

        PersistentHAMT<String, Integer> removed = hamt.remove("b");

        assertThat(removed.size()).isEqualTo(2);
        assertThat(removed.get("a")).isEqualTo(1);
        assertThat(removed.get("b")).isNull();
        assertThat(removed.get("c")).isEqualTo(3);
    }

    @Test
    void removeNonExistentKeyReturnsSameInstance() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.<String, Integer>empty()
                .put("key", 42);

        PersistentHAMT<String, Integer> same = hamt.remove("nonexistent");
        assertThat(same).isSameAs(hamt);
    }

    @Test
    void removeLastKeyReturnsEmptyHamt() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.<String, Integer>empty()
                .put("key", 42);

        PersistentHAMT<String, Integer> empty = hamt.remove("key");
        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.size()).isZero();
    }

    @Test
    void containsKey() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.<String, Integer>empty()
                .put("present", 1);

        assertThat(hamt.containsKey("present")).isTrue();
        assertThat(hamt.containsKey("absent")).isFalse();
    }

    @Test
    void structuralSharingOnPut() {
        // Old version should remain unchanged after put
        PersistentHAMT<String, Integer> v1 = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2);

        PersistentHAMT<String, Integer> v2 = v1.put("c", 3);

        // v1 is unchanged
        assertThat(v1.size()).isEqualTo(2);
        assertThat(v1.get("c")).isNull();

        // v2 has the new entry
        assertThat(v2.size()).isEqualTo(3);
        assertThat(v2.get("c")).isEqualTo(3);

        // Both share "a" and "b"
        assertThat(v1.get("a")).isEqualTo(1);
        assertThat(v2.get("a")).isEqualTo(1);
    }

    @Test
    void structuralSharingOnRemove() {
        PersistentHAMT<String, Integer> v1 = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2)
                .put("c", 3);

        PersistentHAMT<String, Integer> v2 = v1.remove("b");

        // v1 is unchanged
        assertThat(v1.size()).isEqualTo(3);
        assertThat(v1.get("b")).isEqualTo(2);

        // v2 has the entry removed
        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get("b")).isNull();
    }

    @Test
    void forEach() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.empty();
        for (int i = 0; i < 50; i++) {
            hamt = hamt.put("key" + i, i);
        }

        Map<String, Integer> collected = new HashMap<>();
        hamt.forEach(collected::put);

        assertThat(collected).hasSize(50);
        for (int i = 0; i < 50; i++) {
            assertThat(collected).containsEntry("key" + i, i);
        }
    }

    @Test
    void diffDetectsAddedEntries() {
        PersistentHAMT<String, Integer> v1 = PersistentHAMT.<String, Integer>empty()
                .put("a", 1);
        PersistentHAMT<String, Integer> v2 = v1.put("b", 2);

        List<DiffEntry<String, Integer>> diffs = v1.diff(v2);

        assertThat(diffs).hasSize(1);
        assertThat(diffs.get(0).changeType()).isEqualTo(ChangeType.ADDED);
        assertThat(diffs.get(0).key()).isEqualTo("b");
        assertThat(diffs.get(0).newValue()).isEqualTo(2);
    }

    @Test
    void diffDetectsRemovedEntries() {
        PersistentHAMT<String, Integer> v1 = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2);
        PersistentHAMT<String, Integer> v2 = v1.remove("b");

        List<DiffEntry<String, Integer>> diffs = v1.diff(v2);

        assertThat(diffs).hasSize(1);
        assertThat(diffs.get(0).changeType()).isEqualTo(ChangeType.REMOVED);
        assertThat(diffs.get(0).key()).isEqualTo("b");
    }

    @Test
    void diffDetectsModifiedEntries() {
        PersistentHAMT<String, Integer> v1 = PersistentHAMT.<String, Integer>empty()
                .put("a", 1);
        PersistentHAMT<String, Integer> v2 = v1.put("a", 99);

        List<DiffEntry<String, Integer>> diffs = v1.diff(v2);

        assertThat(diffs).hasSize(1);
        assertThat(diffs.get(0).changeType()).isEqualTo(ChangeType.MODIFIED);
        assertThat(diffs.get(0).key()).isEqualTo("a");
        assertThat(diffs.get(0).oldValue()).isEqualTo(1);
        assertThat(diffs.get(0).newValue()).isEqualTo(99);
    }

    @Test
    void diffSameInstanceReturnsEmpty() {
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.<String, Integer>empty()
                .put("a", 1);

        assertThat(hamt.diff(hamt)).isEmpty();
    }

    @Test
    void handlesManyCollisions() {
        // Use keys that all have the same hashCode (by design)
        // Java's String hashCode gives "Aa" and "BB" the same hash
        PersistentHAMT<String, Integer> hamt = PersistentHAMT.empty();
        hamt = hamt.put("Aa", 1);
        hamt = hamt.put("BB", 2);

        assertThat(hamt.size()).isEqualTo(2);
        assertThat(hamt.get("Aa")).isEqualTo(1);
        assertThat(hamt.get("BB")).isEqualTo(2);

        // Remove one colliding key
        hamt = hamt.remove("Aa");
        assertThat(hamt.size()).isEqualTo(1);
        assertThat(hamt.get("Aa")).isNull();
        assertThat(hamt.get("BB")).isEqualTo(2);
    }

    @Test
    void largeScalePutAndRemove() {
        PersistentHAMT<Integer, String> hamt = PersistentHAMT.empty();
        int n = 10_000;

        for (int i = 0; i < n; i++) {
            hamt = hamt.put(i, "val" + i);
        }
        assertThat(hamt.size()).isEqualTo(n);

        // Verify all entries
        for (int i = 0; i < n; i++) {
            assertThat(hamt.get(i)).isEqualTo("val" + i);
        }

        // Remove half
        for (int i = 0; i < n; i += 2) {
            hamt = hamt.remove(i);
        }
        assertThat(hamt.size()).isEqualTo(n / 2);

        // Verify remaining
        for (int i = 0; i < n; i++) {
            if (i % 2 == 0) {
                assertThat(hamt.get(i)).isNull();
            } else {
                assertThat(hamt.get(i)).isEqualTo("val" + i);
            }
        }
    }

    @Test
    void transientBatchInsert() {
        TransientHAMT<String, Integer> transient_ = TransientHAMT.empty();
        for (int i = 0; i < 1000; i++) {
            transient_.put("key" + i, i);
        }

        PersistentHAMT<String, Integer> hamt = transient_.persist();

        assertThat(hamt.size()).isEqualTo(1000);
        for (int i = 0; i < 1000; i++) {
            assertThat(hamt.get("key" + i)).isEqualTo(i);
        }
    }

    @Test
    void transientBatchFromExisting() {
        PersistentHAMT<String, Integer> initial = PersistentHAMT.<String, Integer>empty()
                .put("a", 1)
                .put("b", 2);

        TransientHAMT<String, Integer> transient_ = TransientHAMT.from(initial);
        transient_.put("c", 3);
        transient_.put("d", 4);
        transient_.remove("a");

        PersistentHAMT<String, Integer> result = transient_.persist();

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get("a")).isNull();
        assertThat(result.get("b")).isEqualTo(2);
        assertThat(result.get("c")).isEqualTo(3);
        assertThat(result.get("d")).isEqualTo(4);

        // Original is unchanged
        assertThat(initial.size()).isEqualTo(2);
        assertThat(initial.get("a")).isEqualTo(1);
    }
}
