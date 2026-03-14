package org.tegra.pds.common;

/**
 * Controls transient mutation optimization during batch commits.
 * <p>
 * When a MutationContext is active and the current thread is the owner,
 * nodes may be mutated in-place rather than copied, providing significant
 * performance improvements for batch operations.
 * <p>
 * Once {@link #freeze()} is called, the context becomes invalid and all
 * subsequent mutations will use path-copying.
 */
public final class MutationContext {

    private volatile Thread owner;
    private final long epoch;

    private static long epochCounter = 0;

    private MutationContext(Thread owner) {
        this.owner = owner;
        this.epoch = ++epochCounter;
    }

    /**
     * Creates a new MutationContext owned by the current thread.
     */
    public static MutationContext create() {
        return new MutationContext(Thread.currentThread());
    }

    /**
     * Returns true if the current thread is the owner and the context
     * has not been frozen.
     */
    public boolean isEditable() {
        return owner == Thread.currentThread();
    }

    /**
     * Freezes this context, making it no longer editable.
     * After freezing, all operations will use path-copying.
     */
    public void freeze() {
        owner = null;
    }

    /**
     * Returns the epoch for this context, used to tag nodes that belong
     * to this transient session.
     */
    public long epoch() {
        return epoch;
    }
}
