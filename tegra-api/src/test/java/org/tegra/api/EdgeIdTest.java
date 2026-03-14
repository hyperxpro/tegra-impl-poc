package org.tegra.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EdgeIdTest {

    @Test
    void testToKeyAndFromKeyRoundtrip() {
        EdgeId original = new EdgeId(42L, 99L);
        byte[] key = original.toKey();

        assertThat(key).hasSize(16);

        EdgeId restored = EdgeId.fromKey(key);
        assertThat(restored).isEqualTo(original);
        assertThat(restored.src()).isEqualTo(42L);
        assertThat(restored.dst()).isEqualTo(99L);
    }

    @Test
    void testToKeyAndFromKeyWithLargeValues() {
        EdgeId original = new EdgeId(Long.MAX_VALUE, Long.MIN_VALUE);
        byte[] key = original.toKey();
        EdgeId restored = EdgeId.fromKey(key);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    void testCompareTo() {
        EdgeId e1 = new EdgeId(1, 2);
        EdgeId e2 = new EdgeId(1, 3);
        EdgeId e3 = new EdgeId(2, 1);

        // Same src, different dst
        assertThat(e1.compareTo(e2)).isLessThan(0);
        assertThat(e2.compareTo(e1)).isGreaterThan(0);

        // Different src
        assertThat(e1.compareTo(e3)).isLessThan(0);
        assertThat(e3.compareTo(e1)).isGreaterThan(0);

        // Equal
        assertThat(e1.compareTo(new EdgeId(1, 2))).isEqualTo(0);
    }
}
