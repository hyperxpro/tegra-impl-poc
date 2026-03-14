package org.tegra.pds.hamt;

import org.tegra.pds.common.MutationContext;

import java.util.function.BiConsumer;

/**
 * Mutable batch variant of the HAMT using {@link MutationContext} for transient optimization.
 * <p>
 * This allows efficient batch operations by mutating nodes in-place when safe
 * (i.e., when they were created within the same mutation context). Once the batch
 * is complete, call {@link #persist()} to obtain a persistent (immutable) HAMT.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class TransientHAMT<K, V> {

    private HamtNode<K, V> root;
    private MutationContext context;
    private boolean persisted;

    private TransientHAMT(HamtNode<K, V> root) {
        this.root = root;
        this.context = MutationContext.begin();
        this.persisted = false;
    }

    /**
     * Creates a transient HAMT from the given persistent HAMT.
     */
    public static <K, V> TransientHAMT<K, V> from(PersistentHAMT<K, V> persistent) {
        return new TransientHAMT<>(persistent.root());
    }

    /**
     * Creates an empty transient HAMT.
     */
    public static <K, V> TransientHAMT<K, V> empty() {
        return new TransientHAMT<>(EmptyNode.instance());
    }

    /**
     * Inserts or updates a key-value pair in-place.
     */
    public TransientHAMT<K, V> put(K key, V value) {
        ensureEditable();
        int hash = key.hashCode();
        root = root.put(key, value, hash, 0, context);
        return this;
    }

    /**
     * Removes a key in-place.
     */
    public TransientHAMT<K, V> remove(K key) {
        ensureEditable();
        int hash = key.hashCode();
        root = root.remove(key, hash, 0, context);
        return this;
    }

    /**
     * Looks up a value.
     */
    public V get(K key) {
        ensureEditable();
        return root.get(key, key.hashCode(), 0);
    }

    /**
     * Returns the current size.
     */
    public int size() {
        ensureEditable();
        return root.size();
    }

    /**
     * Applies the given action to each key-value pair.
     */
    public void forEach(BiConsumer<K, V> action) {
        ensureEditable();
        root.forEach(action);
    }

    /**
     * Finalizes the transient HAMT, returning a persistent (immutable) version.
     * The transient HAMT becomes unusable after this call.
     */
    public PersistentHAMT<K, V> persist() {
        ensureEditable();
        context.close();
        persisted = true;
        return new PersistentHAMT<K, V>(root, root.size());
    }

    private void ensureEditable() {
        if (persisted) {
            throw new IllegalStateException("TransientHAMT has already been persisted");
        }
    }
}
