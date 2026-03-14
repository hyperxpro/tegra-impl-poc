package org.tegra.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes and deserializes property maps (Map&lt;String, PropertyValue&gt;)
 * using tagged union encoding.
 * <p>
 * Wire format:
 * <pre>
 *   [entry_count: int]
 *   for each entry:
 *     [key_length: short] [key_bytes: UTF-8]
 *     [value_tag: byte] [value_payload: varies]
 * </pre>
 */
public final class PropertyMapSerializer implements TegraSerializer<Map<String, PropertyValue>> {

    @Override
    public void serialize(Map<String, PropertyValue> map, DataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, PropertyValue> entry : map.entrySet()) {
            writeString(entry.getKey(), out);
            writePropertyValue(entry.getValue(), out);
        }
    }

    @Override
    public Map<String, PropertyValue> deserialize(DataInput in) throws IOException {
        int size = in.readInt();
        Map<String, PropertyValue> map = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = readString(in);
            PropertyValue value = readPropertyValue(in);
            map.put(key, value);
        }
        return map;
    }

    @Override
    public int estimateSize(Map<String, PropertyValue> map) {
        int size = 4; // entry count
        for (Map.Entry<String, PropertyValue> entry : map.entrySet()) {
            size += 2 + entry.getKey().length() * 3; // key (short len + UTF-8 bytes, worst case)
            size += 1 + estimatePropertyValueSize(entry.getValue()); // tag + value
        }
        return size;
    }

    // --- Property Value Serialization ---

    private void writePropertyValue(PropertyValue pv, DataOutput out) throws IOException {
        out.writeByte(pv.tag());
        switch (pv) {
            case PropertyValue.LongProperty lp -> out.writeLong(lp.value());
            case PropertyValue.DoubleProperty dp -> out.writeDouble(dp.value());
            case PropertyValue.StringProperty sp -> writeString(sp.value(), out);
            case PropertyValue.BoolProperty bp -> out.writeBoolean(bp.value());
            case PropertyValue.ByteArrayProperty bap -> {
                out.writeInt(bap.value().length);
                out.write(bap.value());
            }
            case PropertyValue.ListProperty lp -> {
                out.writeInt(lp.values().size());
                for (PropertyValue v : lp.values()) {
                    writePropertyValue(v, out);
                }
            }
            case PropertyValue.MapProperty mp -> {
                out.writeInt(mp.entries().size());
                for (Map.Entry<String, PropertyValue> entry : mp.entries().entrySet()) {
                    writeString(entry.getKey(), out);
                    writePropertyValue(entry.getValue(), out);
                }
            }
        }
    }

    private PropertyValue readPropertyValue(DataInput in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case PropertyValue.TAG_LONG -> new PropertyValue.LongProperty(in.readLong());
            case PropertyValue.TAG_DOUBLE -> new PropertyValue.DoubleProperty(in.readDouble());
            case PropertyValue.TAG_STRING -> new PropertyValue.StringProperty(readString(in));
            case PropertyValue.TAG_BOOL -> new PropertyValue.BoolProperty(in.readBoolean());
            case PropertyValue.TAG_BYTE_ARRAY -> {
                int len = in.readInt();
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                yield new PropertyValue.ByteArrayProperty(bytes);
            }
            case PropertyValue.TAG_LIST -> {
                int size = in.readInt();
                List<PropertyValue> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(readPropertyValue(in));
                }
                yield new PropertyValue.ListProperty(list);
            }
            case PropertyValue.TAG_MAP -> {
                int size = in.readInt();
                Map<String, PropertyValue> map = new LinkedHashMap<>(size);
                for (int i = 0; i < size; i++) {
                    String key = readString(in);
                    PropertyValue value = readPropertyValue(in);
                    map.put(key, value);
                }
                yield new PropertyValue.MapProperty(map);
            }
            default -> throw new IOException("Unknown PropertyValue tag: " + tag);
        };
    }

    private int estimatePropertyValueSize(PropertyValue pv) {
        return switch (pv) {
            case PropertyValue.LongProperty _ -> 8;
            case PropertyValue.DoubleProperty _ -> 8;
            case PropertyValue.StringProperty sp -> 2 + sp.value().length() * 3;
            case PropertyValue.BoolProperty _ -> 1;
            case PropertyValue.ByteArrayProperty bap -> 4 + bap.value().length;
            case PropertyValue.ListProperty lp -> {
                int s = 4;
                for (PropertyValue v : lp.values()) {
                    s += 1 + estimatePropertyValueSize(v);
                }
                yield s;
            }
            case PropertyValue.MapProperty mp -> {
                int s = 4;
                for (Map.Entry<String, PropertyValue> entry : mp.entries().entrySet()) {
                    s += 2 + entry.getKey().length() * 3 + 1 + estimatePropertyValueSize(entry.getValue());
                }
                yield s;
            }
        };
    }

    // --- String helpers ---

    private void writeString(String s, DataOutput out) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private String readString(DataInput in) throws IOException {
        int len = in.readUnsignedShort();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
