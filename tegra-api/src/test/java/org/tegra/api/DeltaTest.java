package org.tegra.api;

import org.junit.jupiter.api.Test;
import org.tegra.serde.EdgeKey;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Delta: known diffs produce expected deltas.
 */
class DeltaTest {

    @Test
    void emptyDelta() {
        Delta delta = new Delta(Set.of(), Set.of(), Set.of(), Set.of(), Set.of(), Set.of());

        assertThat(delta.isEmpty()).isTrue();
        assertThat(delta.affectedVertices()).isEmpty();
    }

    @Test
    void addedVerticesAreAffected() {
        Delta delta = new Delta(Set.of(1L, 2L), Set.of(), Set.of(), Set.of(), Set.of(), Set.of());

        assertThat(delta.isEmpty()).isFalse();
        assertThat(delta.affectedVertices()).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void removedVerticesAreAffected() {
        Delta delta = new Delta(Set.of(), Set.of(3L), Set.of(), Set.of(), Set.of(), Set.of());

        assertThat(delta.affectedVertices()).containsExactly(3L);
    }

    @Test
    void modifiedVerticesAreAffected() {
        Delta delta = new Delta(Set.of(), Set.of(), Set.of(4L, 5L), Set.of(), Set.of(), Set.of());

        assertThat(delta.affectedVertices()).containsExactlyInAnyOrder(4L, 5L);
    }

    @Test
    void edgeEndpointsAreAffected() {
        EdgeKey ek1 = new EdgeKey(10L, 20L, (short) 0);
        EdgeKey ek2 = new EdgeKey(30L, 40L, (short) 0);

        Delta delta = new Delta(Set.of(), Set.of(), Set.of(),
                Set.of(ek1), Set.of(ek2), Set.of());

        Set<Long> affected = delta.affectedVertices();
        assertThat(affected).containsExactlyInAnyOrder(10L, 20L, 30L, 40L);
    }

    @Test
    void affectedVerticesDeduplicates() {
        EdgeKey ek = new EdgeKey(1L, 2L, (short) 0);

        Delta delta = new Delta(Set.of(1L), Set.of(2L), Set.of(),
                Set.of(ek), Set.of(), Set.of());

        // 1L appears in addedVertices and as edge source
        // 2L appears in removedVertices and as edge destination
        Set<Long> affected = delta.affectedVertices();
        assertThat(affected).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void allFieldsContribute() {
        EdgeKey addedEdge = new EdgeKey(10L, 11L, (short) 0);
        EdgeKey removedEdge = new EdgeKey(20L, 21L, (short) 0);
        EdgeKey modifiedEdge = new EdgeKey(30L, 31L, (short) 0);

        Delta delta = new Delta(
                Set.of(1L),
                Set.of(2L),
                Set.of(3L),
                Set.of(addedEdge),
                Set.of(removedEdge),
                Set.of(modifiedEdge)
        );

        assertThat(delta.isEmpty()).isFalse();
        Set<Long> affected = delta.affectedVertices();
        assertThat(affected).containsExactlyInAnyOrder(1L, 2L, 3L, 10L, 11L, 20L, 21L, 30L, 31L);
    }
}
