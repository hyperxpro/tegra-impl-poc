package org.tegra.store.graph;

import org.tegra.pds.hamt.PersistentHAMT;
import org.tegra.serde.PropertyValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Persistent property map backed by a PersistentHAMT.
 * Provides a graph-specific API over the generic HAMT.
 * All mutations return new PropertyMap instances (structural sharing).
 */
public final class PropertyMap implements Iterable<Map.Entry<String, PropertyValue>> {

    private final PersistentHAMT<String, PropertyValue> hamt;

    private PropertyMap(PersistentHAMT<String, PropertyValue> hamt) {
        this.hamt = hamt;
    }

    /**
     * Returns an empty PropertyMap.
     */
    public static PropertyMap empty() {
        return new PropertyMap(PersistentHAMT.empty());
    }

    /**
     * Creates a PropertyMap from an existing java.util.Map.
     */
    public static PropertyMap fromMap(Map<String, PropertyValue> map) {
        PersistentHAMT<String, PropertyValue> h = PersistentHAMT.empty();
        for (Map.Entry<String, PropertyValue> entry : map.entrySet()) {
            h = h.put(entry.getKey(), entry.getValue());
        }
        return new PropertyMap(h);
    }

    /**
     * Returns a new PropertyMap with the given key-value pair added or updated.
     */
    public PropertyMap put(String key, PropertyValue value) {
        PersistentHAMT<String, PropertyValue> newHamt = hamt.put(key, value);
        if (newHamt == hamt) {
            return this;
        }
        return new PropertyMap(newHamt);
    }

    /**
     * Returns the value for the given key, or null if not found.
     */
    public PropertyValue get(String key) {
        return hamt.get(key);
    }

    /**
     * Returns a new PropertyMap with the given key removed.
     */
    public PropertyMap remove(String key) {
        PersistentHAMT<String, PropertyValue> newHamt = hamt.remove(key);
        if (newHamt == hamt) {
            return this;
        }
        return new PropertyMap(newHamt);
    }

    /**
     * Returns the number of properties.
     */
    public int size() {
        return hamt.size();
    }

    /**
     * Returns true if the map contains no properties.
     */
    public boolean isEmpty() {
        return hamt.isEmpty();
    }

    /**
     * Returns true if the map contains the given key.
     */
    public boolean containsKey(String key) {
        return hamt.containsKey(key);
    }

    /**
     * Returns an iterator over the entries.
     */
    @Override
    public Iterator<Map.Entry<String, PropertyValue>> iterator() {
        List<Map.Entry<String, PropertyValue>> entries = new ArrayList<>();
        hamt.forEach((k, v) -> entries.add(Map.entry(k, v)));
        return entries.iterator();
    }

    /**
     * Converts to a standard java.util.Map.
     */
    public Map<String, PropertyValue> toMap() {
        java.util.LinkedHashMap<String, PropertyValue> map = new java.util.LinkedHashMap<>();
        hamt.forEach(map::put);
        return map;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PropertyMap other)) return false;
        if (this.size() != other.size()) return false;
        // Compare all entries
        Map<String, PropertyValue> thisMap = this.toMap();
        Map<String, PropertyValue> otherMap = other.toMap();
        return thisMap.equals(otherMap);
    }

    @Override
    public int hashCode() {
        return toMap().hashCode();
    }

    @Override
    public String toString() {
        return "PropertyMap" + toMap();
    }
}
