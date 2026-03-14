package org.tegra.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.store.MutationLog.MutationEntry;

import static org.assertj.core.api.Assertions.assertThat;

class MutationLogTest {

    private MutationLog<String, String> log;

    @BeforeEach
    void setUp() {
        log = new MutationLog<>();
    }

    @Test
    void testLogAndRetrieve() {
        log.log(new MutationEntry.AddVertex<>(1L, "Alice"));
        log.log(new MutationEntry.AddVertex<>(2L, "Bob"));
        log.log(new MutationEntry.AddEdge<>(1L, 2L, "knows"));
        log.log(new MutationEntry.RemoveEdge<>(1L, 2L));
        log.log(new MutationEntry.RemoveVertex<>(2L));

        assertThat(log.entries()).hasSize(5);
        assertThat(log.entries().getFirst()).isInstanceOf(MutationEntry.AddVertex.class);

        MutationEntry.AddVertex<String, String> first =
                (MutationEntry.AddVertex<String, String>) log.entries().getFirst();
        assertThat(first.id()).isEqualTo(1L);
        assertThat(first.properties()).isEqualTo("Alice");

        assertThat(log.entries().get(2)).isInstanceOf(MutationEntry.AddEdge.class);
        assertThat(log.entries().get(3)).isInstanceOf(MutationEntry.RemoveEdge.class);
        assertThat(log.entries().get(4)).isInstanceOf(MutationEntry.RemoveVertex.class);
    }

    @Test
    void testClear() {
        log.log(new MutationEntry.AddVertex<>(1L, "Alice"));
        log.log(new MutationEntry.AddVertex<>(2L, "Bob"));
        assertThat(log.size()).isEqualTo(2);

        log.clear();
        assertThat(log.size()).isZero();
        assertThat(log.entries()).isEmpty();
    }

    @Test
    void testSize() {
        assertThat(log.size()).isZero();

        log.log(new MutationEntry.AddVertex<>(1L, "Alice"));
        assertThat(log.size()).isEqualTo(1);

        log.log(new MutationEntry.AddEdge<>(1L, 2L, "knows"));
        assertThat(log.size()).isEqualTo(2);
    }
}
