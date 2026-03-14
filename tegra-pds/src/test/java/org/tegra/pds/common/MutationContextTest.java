package org.tegra.pds.common;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class MutationContextTest {

    @Test
    void testIsEditable() {
        MutationContext ctx = MutationContext.create();
        assertThat(ctx.isEditable()).isTrue();
    }

    @Test
    void testFreeze() {
        MutationContext ctx = MutationContext.create();
        assertThat(ctx.isEditable()).isTrue();
        ctx.freeze();
        assertThat(ctx.isEditable()).isFalse();
    }

    @Test
    void testNotEditableFromDifferentThread() throws InterruptedException {
        MutationContext ctx = MutationContext.create();
        AtomicBoolean editableFromOtherThread = new AtomicBoolean(true);

        Thread t = new Thread(() -> editableFromOtherThread.set(ctx.isEditable()));
        t.start();
        t.join();

        assertThat(editableFromOtherThread.get()).isFalse();
    }

    @Test
    void testEpochIsUnique() {
        MutationContext ctx1 = MutationContext.create();
        MutationContext ctx2 = MutationContext.create();
        assertThat(ctx1.epoch()).isNotEqualTo(ctx2.epoch());
    }
}
