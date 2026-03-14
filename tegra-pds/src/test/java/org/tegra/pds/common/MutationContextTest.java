package org.tegra.pds.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for MutationContext: lifecycle, thread-local behavior, edit checks.
 */
class MutationContextTest {

    @Test
    void beginCreatesActiveContext() {
        MutationContext ctx = MutationContext.begin();
        try {
            assertThat(ctx.isActive()).isTrue();
            assertThat(ctx.id()).isPositive();
            assertThat(MutationContext.current()).isSameAs(ctx);
        } finally {
            ctx.close();
        }
    }

    @Test
    void closeDeactivatesContext() {
        MutationContext ctx = MutationContext.begin();
        ctx.close();

        assertThat(ctx.isActive()).isFalse();
        assertThat(MutationContext.current()).isNull();
    }

    @Test
    void cannotBeginWhileActive() {
        MutationContext ctx = MutationContext.begin();
        try {
            assertThatThrownBy(MutationContext::begin)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already active");
        } finally {
            ctx.close();
        }
    }

    @Test
    void canBeginAfterClose() {
        MutationContext ctx1 = MutationContext.begin();
        ctx1.close();

        MutationContext ctx2 = MutationContext.begin();
        try {
            assertThat(ctx2.isActive()).isTrue();
            assertThat(ctx2.id()).isNotEqualTo(ctx1.id());
        } finally {
            ctx2.close();
        }
    }

    @Test
    void canEditMatchingContext() {
        MutationContext ctx = MutationContext.begin();
        try {
            assertThat(ctx.canEdit(ctx.id())).isTrue();
        } finally {
            ctx.close();
        }
    }

    @Test
    void cannotEditDifferentContext() {
        MutationContext ctx = MutationContext.begin();
        try {
            assertThat(ctx.canEdit(ctx.id() + 999)).isFalse();
        } finally {
            ctx.close();
        }
    }

    @Test
    void cannotEditAfterClose() {
        MutationContext ctx = MutationContext.begin();
        long id = ctx.id();
        ctx.close();

        assertThat(ctx.canEdit(id)).isFalse();
    }

    @Test
    void currentIsNullWhenNoContext() {
        assertThat(MutationContext.current()).isNull();
    }

    @Test
    void uniqueIdsAcrossContexts() {
        MutationContext ctx1 = MutationContext.begin();
        long id1 = ctx1.id();
        ctx1.close();

        MutationContext ctx2 = MutationContext.begin();
        long id2 = ctx2.id();
        ctx2.close();

        assertThat(id1).isNotEqualTo(id2);
    }

    @Test
    void threadIsolation() throws InterruptedException {
        MutationContext mainCtx = MutationContext.begin();
        final long[] otherThreadId = new long[1];
        final boolean[] otherThreadHasContext = new boolean[1];

        Thread thread = new Thread(() -> {
            otherThreadHasContext[0] = MutationContext.current() != null;
            MutationContext otherCtx = MutationContext.begin();
            otherThreadId[0] = otherCtx.id();
            otherCtx.close();
        });
        thread.start();
        thread.join();

        try {
            assertThat(otherThreadHasContext[0]).isFalse();
            assertThat(otherThreadId[0]).isNotEqualTo(mainCtx.id());
        } finally {
            mainCtx.close();
        }
    }
}
