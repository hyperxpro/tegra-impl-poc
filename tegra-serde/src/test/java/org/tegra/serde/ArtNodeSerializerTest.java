package org.tegra.serde;

import org.junit.jupiter.api.Test;
import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ArtNodeSerializer: roundtrip subtree serialize/deserialize.
 */
class ArtNodeSerializerTest {

    /**
     * Simple string serializer for tests.
     */
    private static final TegraSerializer<String> STRING_SERIALIZER = new TegraSerializer<>() {
        @Override
        public void serialize(String value, DataOutput out) throws IOException {
            byte[] bytes = value.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        @Override
        public String deserialize(DataInput in) throws IOException {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            return new String(bytes);
        }

        @Override
        public int estimateSize(String value) {
            return 4 + value.length();
        }
    };

    private final ArtNodeSerializer<String> serializer = new ArtNodeSerializer<>(STRING_SERIALIZER);

    private static byte[] key(long value) {
        byte[] k = new byte[8];
        ByteBuffer.wrap(k).putLong(value);
        return k;
    }

    @Test
    void roundtripEmptyTree() throws IOException {
        PersistentART<String> tree = PersistentART.empty();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        serializer.serialize(tree, dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        PersistentART<String> deserialized = serializer.deserializeTree(dis);

        assertThat(deserialized.size()).isZero();
        assertThat(deserialized.isEmpty()).isTrue();
    }

    @Test
    void roundtripSingleEntry() throws IOException {
        PersistentART<String> tree = PersistentART.<String>empty()
                .insert(key(42), "hello");

        PersistentART<String> deserialized = roundtrip(tree);

        assertThat(deserialized.size()).isEqualTo(1);
        assertThat(deserialized.lookup(key(42))).isEqualTo("hello");
    }

    @Test
    void roundtripMultipleEntries() throws IOException {
        PersistentART<String> tree = PersistentART.empty();
        for (int i = 0; i < 100; i++) {
            tree = tree.insert(key(i), "val" + i);
        }

        PersistentART<String> deserialized = roundtrip(tree);

        assertThat(deserialized.size()).isEqualTo(100);
        for (int i = 0; i < 100; i++) {
            assertThat(deserialized.lookup(key(i))).isEqualTo("val" + i);
        }
    }

    @Test
    void roundtripPreservesAllNodeTypes() throws IOException {
        // Build a tree large enough to have Node4, Node16, Node48, Node256
        PersistentART<String> tree = PersistentART.empty();
        for (int i = 0; i < 300; i++) {
            tree = tree.insert(key(i), "v" + i);
        }

        PersistentART<String> deserialized = roundtrip(tree);

        assertThat(deserialized.size()).isEqualTo(300);
        for (int i = 0; i < 300; i++) {
            assertThat(deserialized.lookup(key(i))).isEqualTo("v" + i);
        }
    }

    @Test
    void roundtripWithVariableLengthKeys() throws IOException {
        PersistentART<String> tree = PersistentART.<String>empty()
                .insert("a".getBytes(), "1")
                .insert("ab".getBytes(), "2")
                .insert("abc".getBytes(), "3")
                .insert("abcd".getBytes(), "4")
                .insert("xyz".getBytes(), "5");

        PersistentART<String> deserialized = roundtrip(tree);

        assertThat(deserialized.size()).isEqualTo(5);
        assertThat(deserialized.lookup("a".getBytes())).isEqualTo("1");
        assertThat(deserialized.lookup("ab".getBytes())).isEqualTo("2");
        assertThat(deserialized.lookup("abc".getBytes())).isEqualTo("3");
        assertThat(deserialized.lookup("abcd".getBytes())).isEqualTo("4");
        assertThat(deserialized.lookup("xyz".getBytes())).isEqualTo("5");
    }

    @Test
    void roundtripNodeDirectly() throws IOException {
        PersistentART<String> tree = PersistentART.<String>empty()
                .insert(key(1), "one")
                .insert(key(2), "two");

        // Serialize via the ArtNodeSerializer (which uses PersistentART internally)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        serializer.serialize(tree, dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        PersistentART<String> deserialized = serializer.deserializeTree(dis);

        assertThat(deserialized).isNotNull();
        assertThat(deserialized.size()).isEqualTo(2);
        assertThat(deserialized.lookup(key(1))).isEqualTo("one");
        assertThat(deserialized.lookup(key(2))).isEqualTo("two");
    }

    @Test
    void estimateSizeIsPositive() {
        PersistentART<String> tree = PersistentART.<String>empty()
                .insert(key(1), "one")
                .insert(key(2), "two");

        assertThat(serializer.estimateSize(tree.root())).isPositive();
    }

    @Test
    void estimateSizeNullIsSmall() {
        assertThat(serializer.estimateSize(null)).isEqualTo(4);
    }

    private PersistentART<String> roundtrip(PersistentART<String> tree) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        serializer.serialize(tree, dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        return serializer.deserializeTree(dis);
    }
}
