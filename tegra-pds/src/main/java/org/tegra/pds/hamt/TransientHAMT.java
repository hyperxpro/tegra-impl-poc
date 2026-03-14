package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.Objects;

/**
 * A transient (mutable) version of PersistentHAMT for efficient batch operations.
 * <p>
 * Uses a {@link MutationContext} to allow in-place mutation of nodes owned by
 * the current transient session, avoiding unnecessary copying during batch inserts.
 * <p>
 * After all mutations are complete, call {@link #persistent()} to obtain an
 * immutable PersistentHAMT. The transient cannot be used after that.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class TransientHAMT<K, V> {

    private HamtNode<K, V> root;
    private int size;
    private MutationContext ctx;

    TransientHAMT(HamtNode<K, V> root, int size) {
        this.root = root;
        this.size = size;
        this.ctx = MutationContext.create();
    }

    private void ensureEditable() {
        if (ctx == null || !ctx.isEditable()) {
            throw new IllegalStateException("Transient has been frozen");
        }
    }

    /**
     * Adds or updates a key-value mapping in-place.
     */
    public TransientHAMT<K, V> put(K key, V value) {
        ensureEditable();
        int hash = Objects.hashCode(key);
        HamtNode<K, V> newRoot = root.put(key, value, hash, 0, ctx);
        if (newRoot != root) {
            root = newRoot;
        }
        // Recompute size (we could optimize this with a flag, but correctness first)
        size = root.size();
        return this;
    }

    /**
     * Removes a key in-place.
     */
    public TransientHAMT<K, V> remove(K key) {
        ensureEditable();
        int hash = Objects.hashCode(key);
        HamtNode<K, V> newRoot = root.remove(key, hash, 0, ctx);
        if (newRoot != root) {
            root = newRoot;
        }
        size = root.size();
        return this;
    }

    /**
     * Returns the value associated with the given key.
     */
    public V get(K key) {
        ensureEditable();
        int hash = Objects.hashCode(key);
        return root.get(key, hash, 0);
    }

    /**
     * Returns the number of key-value pairs.
     */
    public int size() {
        return size;
    }

    /**
     * Freezes this transient and returns an immutable PersistentHAMT.
     * The transient cannot be used after this call.
     */
    public PersistentHAMT<K, V> persistent() {
        ensureEditable();
        ctx.freeze();
        ctx = null;
        return new PersistentHAMT<>(root, size);
    }
}
