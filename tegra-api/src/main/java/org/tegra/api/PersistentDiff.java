package org.tegra.api;

import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Efficient diff exploiting structural sharing in persistent data structures.
 * When two subtree roots are the same object reference, they are identical
 * and the entire subtree can be skipped.
 */
public final class PersistentDiff {

    private PersistentDiff() {
        // utility class
    }

    /**
     * Computes the set of keys that differ between two ART trees.
     * Uses reference equality (==) to skip shared subtrees.
     *
     * @param rootA the first tree root
     * @param rootB the second tree root
     * @param <V>   the value type
     * @return set of byte-array keys that differ (wrapped in ByteArrayWrapper for proper equality)
     */
    public static <V> Set<ByteArrayWrapper> diff(ArtNode<V> rootA, ArtNode<V> rootB) {
        // Identity check — structural sharing optimization
        if (rootA == rootB) {
            return Set.of();
        }

        Set<ByteArrayWrapper> result = new HashSet<>();

        // Collect all entries from both trees
        Map<ByteArrayWrapper, V> mapA = new LinkedHashMap<>();
        Map<ByteArrayWrapper, V> mapB = new LinkedHashMap<>();

        if (rootA != null) {
            PersistentART<V> artA = PersistentART.fromRoot(rootA);
            artA.forEach((k, v) -> mapA.put(new ByteArrayWrapper(k), v));
        }
        if (rootB != null) {
            PersistentART<V> artB = PersistentART.fromRoot(rootB);
            artB.forEach((k, v) -> mapB.put(new ByteArrayWrapper(k), v));
        }

        // Keys in A but not B, or modified
        for (var entry : mapA.entrySet()) {
            V otherValue = mapB.get(entry.getKey());
            if (otherValue == null && !mapB.containsKey(entry.getKey())) {
                result.add(entry.getKey()); // removed
            } else if (otherValue != null && !otherValue.equals(entry.getValue())) {
                result.add(entry.getKey()); // modified
            }
        }

        // Keys in B but not A
        for (var entry : mapB.entrySet()) {
            if (!mapA.containsKey(entry.getKey())) {
                result.add(entry.getKey()); // added
            }
        }

        return result;
    }

    /**
     * Wrapper for byte[] that provides proper equals/hashCode.
     */
    public record ByteArrayWrapper(byte[] data) {
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof ByteArrayWrapper other)) return false;
            return Arrays.equals(data, other.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }

        @Override
        public String toString() {
            return "ByteArrayWrapper[" + Arrays.toString(data) + "]";
        }
    }
}
