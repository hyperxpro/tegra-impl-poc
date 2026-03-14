package org.tegra.serde;

import java.util.List;
import java.util.Map;

/**
 * Sealed hierarchy for property values in the Tegra graph model.
 * <p>
 * Uses tagged union encoding for compact binary serialization.
 * Each variant has a unique type tag used during serialization.
 */
public sealed interface PropertyValue
        permits PropertyValue.LongProperty,
                PropertyValue.DoubleProperty,
                PropertyValue.StringProperty,
                PropertyValue.BoolProperty,
                PropertyValue.ByteArrayProperty,
                PropertyValue.ListProperty,
                PropertyValue.MapProperty {

    /** Type tags for binary encoding. */
    byte TAG_LONG = 1;
    byte TAG_DOUBLE = 2;
    byte TAG_STRING = 3;
    byte TAG_BOOL = 4;
    byte TAG_BYTE_ARRAY = 5;
    byte TAG_LIST = 6;
    byte TAG_MAP = 7;

    /**
     * Returns the type tag for this property value.
     */
    byte tag();

    record LongProperty(long value) implements PropertyValue {
        @Override
        public byte tag() {
            return TAG_LONG;
        }
    }

    record DoubleProperty(double value) implements PropertyValue {
        @Override
        public byte tag() {
            return TAG_DOUBLE;
        }
    }

    record StringProperty(String value) implements PropertyValue {
        @Override
        public byte tag() {
            return TAG_STRING;
        }
    }

    record BoolProperty(boolean value) implements PropertyValue {
        @Override
        public byte tag() {
            return TAG_BOOL;
        }
    }

    record ByteArrayProperty(byte[] value) implements PropertyValue {
        @Override
        public byte tag() {
            return TAG_BYTE_ARRAY;
        }
    }

    record ListProperty(List<PropertyValue> values) implements PropertyValue {
        @Override
        public byte tag() {
            return TAG_LIST;
        }
    }

    record MapProperty(Map<String, PropertyValue> entries) implements PropertyValue {
        @Override
        public byte tag() {
            return TAG_MAP;
        }
    }
}
