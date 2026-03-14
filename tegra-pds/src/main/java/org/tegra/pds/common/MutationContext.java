package org.tegra.pds.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-local transient optimization context for batch operations on persistent data structures.
 * <p>
 * When active, nodes may be mutated in-place if they belong to the current context
 * (i.e., they were created within this mutation batch). This avoids redundant path-copying
 * during batch inserts/removes.
 * <p>
 * Each context has a unique ID. Nodes created within a context store that ID and can
 * be safely mutated in-place as long as the context is still active.
 */
public final class MutationContext {

    private static final AtomicLong ID_GEN = new AtomicLong(1);

    private static final ThreadLocal<MutationContext> CURRENT = new ThreadLocal<>();

    private final long id;
    private volatile boolean active;

    private MutationContext() {
        this.id = ID_GEN.getAndIncrement();
        this.active = true;
    }

    /**
     * Begins a new transient mutation context on the current thread.
     * Must be closed via {@link #close()} when the batch is complete.
     *
     * @return the new context
     * @throws IllegalStateException if a context is already active on this thread
     */
    public static MutationContext begin() {
        if (CURRENT.get() != null) {
            throw new IllegalStateException("A MutationContext is already active on this thread");
        }
        MutationContext ctx = new MutationContext();
        CURRENT.set(ctx);
        return ctx;
    }

    /**
     * Returns the currently active context on this thread, or null if none.
     */
    public static MutationContext current() {
        return CURRENT.get();
    }

    /**
     * Returns the unique ID of this context.
     */
    public long id() {
        return id;
    }

    /**
     * Returns true if this context is still active (not yet closed).
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Checks whether a node owned by the given context ID can be mutated in-place.
     *
     * @param ownerContextId the context ID stored in the node
     * @return true if the node belongs to this context and can be mutated in-place
     */
    public boolean canEdit(long ownerContextId) {
        return active && ownerContextId == id;
    }

    /**
     * Closes this context, making it inactive. Nodes created within this context
     * will no longer be mutable in-place.
     */
    public void close() {
        active = false;
        CURRENT.remove();
    }
}
