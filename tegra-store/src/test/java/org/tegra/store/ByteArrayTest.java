package org.tegra.store;

import org.junit.jupiter.api.Test;
import org.tegra.store.version.ByteArray;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ByteArray: equality, ordering, hashing, startsWith.
 */
class ByteArrayTest {

    @Test
    void equalByteArraysAreEqual() {
        ByteArray a = new ByteArray(new byte[]{1, 2, 3});
        ByteArray b = new ByteArray(new byte[]{1, 2, 3});

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differentByteArraysAreNotEqual() {
        ByteArray a = new ByteArray(new byte[]{1, 2, 3});
        ByteArray b = new ByteArray(new byte[]{1, 2, 4});

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void differentLengthsAreNotEqual() {
        ByteArray a = new ByteArray(new byte[]{1, 2});
        ByteArray b = new ByteArray(new byte[]{1, 2, 3});

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void lexicographicOrderingShorterFirst() {
        ByteArray a = new ByteArray(new byte[]{1, 2});
        ByteArray b = new ByteArray(new byte[]{1, 2, 3});

        assertThat(a.compareTo(b)).isLessThan(0);
        assertThat(b.compareTo(a)).isGreaterThan(0);
    }

    @Test
    void lexicographicOrderingByContent() {
        ByteArray a = new ByteArray(new byte[]{1, 2, 3});
        ByteArray b = new ByteArray(new byte[]{1, 2, 4});

        assertThat(a.compareTo(b)).isLessThan(0);
        assertThat(b.compareTo(a)).isGreaterThan(0);
    }

    @Test
    void compareToZeroForEqual() {
        ByteArray a = new ByteArray(new byte[]{1, 2, 3});
        ByteArray b = new ByteArray(new byte[]{1, 2, 3});

        assertThat(a.compareTo(b)).isZero();
    }

    @Test
    void worksAsHashMapKey() {
        ByteArray key1 = new ByteArray(new byte[]{10, 20, 30});
        ByteArray key2 = new ByteArray(new byte[]{10, 20, 30});

        Map<ByteArray, String> map = new HashMap<>();
        map.put(key1, "value");

        assertThat(map.get(key2)).isEqualTo("value");
    }

    @Test
    void startsWithPrefix() {
        ByteArray full = ByteArray.fromString("TWTR_1234567890");
        ByteArray prefix = ByteArray.fromString("TWTR_");

        assertThat(full.startsWith(prefix)).isTrue();
    }

    @Test
    void doesNotStartWithNonPrefix() {
        ByteArray full = ByteArray.fromString("TWTR_1234567890");
        ByteArray notPrefix = ByteArray.fromString("FB_");

        assertThat(full.startsWith(notPrefix)).isFalse();
    }

    @Test
    void fromStringProducesUtf8() {
        ByteArray ba = ByteArray.fromString("hello");
        assertThat(ba.length()).isEqualTo(5);
        assertThat(ba.toString()).isEqualTo("hello");
    }

    @Test
    void emptyByteArray() {
        ByteArray empty = new ByteArray(new byte[0]);
        assertThat(empty.length()).isZero();
    }

    @Test
    void dataReturnsDefensiveCopy() {
        byte[] original = {1, 2, 3};
        ByteArray ba = new ByteArray(original);

        // Modify original — ByteArray should not be affected
        original[0] = 99;
        assertThat(ba.data()[0]).isEqualTo((byte) 1);

        // Modify returned data — ByteArray should not be affected
        byte[] copy = ba.data();
        copy[0] = 99;
        assertThat(ba.data()[0]).isEqualTo((byte) 1);
    }

    @Test
    void unsignedLexicographicComparison() {
        // 0xFF should be greater than 0x01 in unsigned comparison
        ByteArray a = new ByteArray(new byte[]{(byte) 0x01});
        ByteArray b = new ByteArray(new byte[]{(byte) 0xFF});

        assertThat(a.compareTo(b)).isLessThan(0);
    }
}
