package org.tegra.serde;

import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializes and deserializes PersistentART trees.
 * <p>
 * Serialization writes all leaf entries (key-value pairs) in iteration order.
 * Deserialization rebuilds the tree by inserting entries via the PersistentART API.
 * <p>
 * Keys are serialized/deserialized through PersistentART's public API which
 * handles internal null-terminator bytes transparently.
 * <p>
 * Wire format:
 * <pre>
 *   [entry_count: int]
 *   for each entry:
 *     [key_length: int] [key_bytes]
 *     [value_payload: varies by valueSerializer]
 * </pre>
 *
 * @param <V> the value type stored in leaves
 */
public final class ArtNodeSerializer<V> implements TegraSerializer<ArtNode<V>> {

    private final TegraSerializer<V> valueSerializer;

    public ArtNodeSerializer(TegraSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void serialize(ArtNode<V> node, DataOutput out) throws IOException {
        // When called with a raw ArtNode, wrap in PersistentART to get proper key handling
        PersistentART<V> art = PersistentART.fromRoot(node);
        serializeTree(art, out);
    }

    @Override
    public ArtNode<V> deserialize(DataInput in) throws IOException {
        PersistentART<V> art = deserializeTree(in);
        return art.root();
    }

    @Override
    public int estimateSize(ArtNode<V> node) {
        if (node == null) return 4;
        PersistentART<V> art = PersistentART.fromRoot(node);
        int[] estimate = {4}; // entry count
        art.forEach((key, value) -> {
            estimate[0] += 4 + key.length;
            estimate[0] += valueSerializer.estimateSize(value);
        });
        return estimate[0];
    }

    /**
     * Serializes a PersistentART tree.
     */
    public void serialize(PersistentART<V> art, DataOutput out) throws IOException {
        serializeTree(art, out);
    }

    /**
     * Deserializes a PersistentART tree.
     */
    public PersistentART<V> deserializeTree(DataInput in) throws IOException {
        int count = in.readInt();
        if (count == 0) {
            return PersistentART.empty();
        }

        PersistentART<V> art = PersistentART.empty();
        for (int i = 0; i < count; i++) {
            int keyLen = in.readInt();
            byte[] key = new byte[keyLen];
            in.readFully(key);
            V value = valueSerializer.deserialize(in);
            art = art.insert(key, value);
        }
        return art;
    }

    private void serializeTree(PersistentART<V> art, DataOutput out) throws IOException {
        if (art == null || art.isEmpty()) {
            out.writeInt(0);
            return;
        }

        // Collect entries using forEach (which strips terminators)
        List<LeafEntry<V>> entries = new ArrayList<>();
        art.forEach((key, value) -> entries.add(new LeafEntry<>(key, value)));

        out.writeInt(entries.size());
        for (LeafEntry<V> entry : entries) {
            out.writeInt(entry.key.length);
            out.write(entry.key);
            valueSerializer.serialize(entry.value, out);
        }
    }

    private record LeafEntry<V>(byte[] key, V value) {
    }
}
