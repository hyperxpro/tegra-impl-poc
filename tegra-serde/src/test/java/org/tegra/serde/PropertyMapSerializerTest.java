package org.tegra.serde;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PropertyMapSerializer: roundtrip all property types.
 */
class PropertyMapSerializerTest {

    private final PropertyMapSerializer serializer = new PropertyMapSerializer();

    private Map<String, PropertyValue> roundtrip(Map<String, PropertyValue> input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        serializer.serialize(input, dos);
        dos.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        return serializer.deserialize(dis);
    }

    @Test
    void emptyMap() throws IOException {
        Map<String, PropertyValue> map = Map.of();
        Map<String, PropertyValue> result = roundtrip(map);
        assertThat(result).isEmpty();
    }

    @Test
    void longProperty() throws IOException {
        Map<String, PropertyValue> map = Map.of("age", new PropertyValue.LongProperty(42L));
        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(1);
        assertThat(result.get("age")).isInstanceOf(PropertyValue.LongProperty.class);
        assertThat(((PropertyValue.LongProperty) result.get("age")).value()).isEqualTo(42L);
    }

    @Test
    void doubleProperty() throws IOException {
        Map<String, PropertyValue> map = Map.of("weight", new PropertyValue.DoubleProperty(3.14));
        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(1);
        assertThat(result.get("weight")).isInstanceOf(PropertyValue.DoubleProperty.class);
        assertThat(((PropertyValue.DoubleProperty) result.get("weight")).value()).isEqualTo(3.14);
    }

    @Test
    void stringProperty() throws IOException {
        Map<String, PropertyValue> map = Map.of("name", new PropertyValue.StringProperty("Alice"));
        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(1);
        assertThat(result.get("name")).isInstanceOf(PropertyValue.StringProperty.class);
        assertThat(((PropertyValue.StringProperty) result.get("name")).value()).isEqualTo("Alice");
    }

    @Test
    void boolProperty() throws IOException {
        Map<String, PropertyValue> map = Map.of("active", new PropertyValue.BoolProperty(true));
        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(1);
        assertThat(result.get("active")).isInstanceOf(PropertyValue.BoolProperty.class);
        assertThat(((PropertyValue.BoolProperty) result.get("active")).value()).isTrue();
    }

    @Test
    void byteArrayProperty() throws IOException {
        byte[] data = {1, 2, 3, 4, 5};
        Map<String, PropertyValue> map = Map.of("data", new PropertyValue.ByteArrayProperty(data));
        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(1);
        assertThat(result.get("data")).isInstanceOf(PropertyValue.ByteArrayProperty.class);
        assertThat(((PropertyValue.ByteArrayProperty) result.get("data")).value()).isEqualTo(data);
    }

    @Test
    void listProperty() throws IOException {
        List<PropertyValue> list = List.of(
                new PropertyValue.LongProperty(1),
                new PropertyValue.StringProperty("two"),
                new PropertyValue.BoolProperty(false)
        );
        Map<String, PropertyValue> map = Map.of("items", new PropertyValue.ListProperty(list));
        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(1);
        PropertyValue.ListProperty lp = (PropertyValue.ListProperty) result.get("items");
        assertThat(lp.values()).hasSize(3);
        assertThat(lp.values().get(0)).isInstanceOf(PropertyValue.LongProperty.class);
        assertThat(((PropertyValue.LongProperty) lp.values().get(0)).value()).isEqualTo(1);
        assertThat(lp.values().get(1)).isInstanceOf(PropertyValue.StringProperty.class);
        assertThat(((PropertyValue.StringProperty) lp.values().get(1)).value()).isEqualTo("two");
        assertThat(lp.values().get(2)).isInstanceOf(PropertyValue.BoolProperty.class);
    }

    @Test
    void mapProperty() throws IOException {
        Map<String, PropertyValue> inner = new LinkedHashMap<>();
        inner.put("nested_key", new PropertyValue.LongProperty(99));
        inner.put("nested_str", new PropertyValue.StringProperty("hello"));

        Map<String, PropertyValue> map = new LinkedHashMap<>();
        map.put("nested", new PropertyValue.MapProperty(inner));

        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(1);
        PropertyValue.MapProperty mp = (PropertyValue.MapProperty) result.get("nested");
        assertThat(mp.entries()).hasSize(2);
        assertThat(((PropertyValue.LongProperty) mp.entries().get("nested_key")).value()).isEqualTo(99);
        assertThat(((PropertyValue.StringProperty) mp.entries().get("nested_str")).value()).isEqualTo("hello");
    }

    @Test
    void mixedPropertyTypes() throws IOException {
        Map<String, PropertyValue> map = new LinkedHashMap<>();
        map.put("id", new PropertyValue.LongProperty(12345L));
        map.put("score", new PropertyValue.DoubleProperty(98.6));
        map.put("name", new PropertyValue.StringProperty("Test"));
        map.put("active", new PropertyValue.BoolProperty(true));
        map.put("tags", new PropertyValue.ListProperty(List.of(
                new PropertyValue.StringProperty("tag1"),
                new PropertyValue.StringProperty("tag2")
        )));

        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(result).hasSize(5);
        assertThat(((PropertyValue.LongProperty) result.get("id")).value()).isEqualTo(12345L);
        assertThat(((PropertyValue.DoubleProperty) result.get("score")).value()).isEqualTo(98.6);
        assertThat(((PropertyValue.StringProperty) result.get("name")).value()).isEqualTo("Test");
        assertThat(((PropertyValue.BoolProperty) result.get("active")).value()).isTrue();
        assertThat(((PropertyValue.ListProperty) result.get("tags")).values()).hasSize(2);
    }

    @Test
    void unicodeStringProperty() throws IOException {
        Map<String, PropertyValue> map = Map.of(
                "greeting", new PropertyValue.StringProperty("Hello, \u4e16\u754c! \ud83c\udf0d")
        );
        Map<String, PropertyValue> result = roundtrip(map);

        assertThat(((PropertyValue.StringProperty) result.get("greeting")).value())
                .isEqualTo("Hello, \u4e16\u754c! \ud83c\udf0d");
    }

    @Test
    void estimateSizeIsPositive() {
        Map<String, PropertyValue> map = Map.of(
                "key1", new PropertyValue.LongProperty(42L),
                "key2", new PropertyValue.StringProperty("value")
        );
        assertThat(serializer.estimateSize(map)).isPositive();
    }

    @Test
    void vertexDataRoundtrip() throws IOException {
        VertexDataSerializer vds = new VertexDataSerializer();
        Map<String, PropertyValue> props = new LinkedHashMap<>();
        props.put("name", new PropertyValue.StringProperty("Alice"));
        props.put("age", new PropertyValue.LongProperty(30));

        VertexData original = new VertexData(42L, props);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        vds.serialize(original, dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        VertexData deserialized = vds.deserialize(dis);

        assertThat(deserialized.vertexId()).isEqualTo(42L);
        assertThat(((PropertyValue.StringProperty) deserialized.properties().get("name")).value()).isEqualTo("Alice");
        assertThat(((PropertyValue.LongProperty) deserialized.properties().get("age")).value()).isEqualTo(30);
    }

    @Test
    void edgeDataRoundtrip() throws IOException {
        EdgeDataSerializer eds = new EdgeDataSerializer();
        Map<String, PropertyValue> props = new LinkedHashMap<>();
        props.put("weight", new PropertyValue.DoubleProperty(1.5));

        EdgeData original = new EdgeData(new EdgeKey(1L, 2L, (short) 0), props);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        eds.serialize(original, dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        EdgeData deserialized = eds.deserialize(dis);

        assertThat(deserialized.edgeKey().srcId()).isEqualTo(1L);
        assertThat(deserialized.edgeKey().dstId()).isEqualTo(2L);
        assertThat(deserialized.edgeKey().discriminator()).isEqualTo((short) 0);
        assertThat(((PropertyValue.DoubleProperty) deserialized.properties().get("weight")).value()).isEqualTo(1.5);
    }
}
