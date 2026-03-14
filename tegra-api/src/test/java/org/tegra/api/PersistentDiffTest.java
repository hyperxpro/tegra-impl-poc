package org.tegra.api;

import org.junit.jupiter.api.Test;
import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;
import org.tegra.serde.KeyCodec;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PersistentDiff: structural sharing is exploited (identity check),
 * diffs correctly identify changed keys.
 */
class PersistentDiffTest {

    @Test
    void identicalRootsReturnEmptyDiff() {
        PersistentART<VertexData> art = PersistentART.empty();
        art = art.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of()));
        art = art.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of()));

        ArtNode<VertexData> root = art.root();

        // Same reference — structural sharing means no diff needed
        Set<PersistentDiff.ByteArrayWrapper> diffs = PersistentDiff.diff(root, root);

        assertThat(diffs).isEmpty();
    }

    @Test
    void bothNullRootsReturnEmptyDiff() {
        Set<PersistentDiff.ByteArrayWrapper> diffs = PersistentDiff.diff(null, null);
        assertThat(diffs).isEmpty();
    }

    @Test
    void addedKeysDetected() {
        PersistentART<VertexData> artA = PersistentART.empty();
        artA = artA.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of()));

        PersistentART<VertexData> artB = artA.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of()));

        Set<PersistentDiff.ByteArrayWrapper> diffs =
                PersistentDiff.diff(artA.root(), artB.root());

        assertThat(diffs).hasSize(1);
        // The diff should contain the key for vertex 2
        PersistentDiff.ByteArrayWrapper key2 =
                new PersistentDiff.ByteArrayWrapper(KeyCodec.encodeVertexKey(2L));
        assertThat(diffs).contains(key2);
    }

    @Test
    void removedKeysDetected() {
        PersistentART<VertexData> artA = PersistentART.empty();
        artA = artA.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of()));
        artA = artA.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of()));

        PersistentART<VertexData> artB = artA.remove(KeyCodec.encodeVertexKey(2L));

        Set<PersistentDiff.ByteArrayWrapper> diffs =
                PersistentDiff.diff(artA.root(), artB.root());

        assertThat(diffs).hasSize(1);
        PersistentDiff.ByteArrayWrapper key2 =
                new PersistentDiff.ByteArrayWrapper(KeyCodec.encodeVertexKey(2L));
        assertThat(diffs).contains(key2);
    }

    @Test
    void modifiedValuesDetected() {
        PersistentART<VertexData> artA = PersistentART.empty();
        artA = artA.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(10))));

        PersistentART<VertexData> artB = PersistentART.empty();
        artB = artB.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(20))));

        Set<PersistentDiff.ByteArrayWrapper> diffs =
                PersistentDiff.diff(artA.root(), artB.root());

        assertThat(diffs).hasSize(1);
    }

    @Test
    void nullVsNonNullRootDetectsAllKeys() {
        PersistentART<VertexData> art = PersistentART.empty();
        art = art.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of()));
        art = art.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of()));

        // null -> non-null: all keys are "added"
        Set<PersistentDiff.ByteArrayWrapper> diffs =
                PersistentDiff.diff(null, art.root());
        assertThat(diffs).hasSize(2);

        // non-null -> null: all keys are "removed"
        Set<PersistentDiff.ByteArrayWrapper> diffs2 =
                PersistentDiff.diff(art.root(), null);
        assertThat(diffs2).hasSize(2);
    }

    @Test
    void unchangedKeysNotIncluded() {
        PersistentART<VertexData> artA = PersistentART.empty();
        artA = artA.insert(KeyCodec.encodeVertexKey(1L),
                new VertexData(1L, Map.of("v", new PropertyValue.LongProperty(1))));
        artA = artA.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of("v", new PropertyValue.LongProperty(2))));

        // Only modify key 2
        PersistentART<VertexData> artB = artA.insert(KeyCodec.encodeVertexKey(2L),
                new VertexData(2L, Map.of("v", new PropertyValue.LongProperty(200))));

        Set<PersistentDiff.ByteArrayWrapper> diffs =
                PersistentDiff.diff(artA.root(), artB.root());

        // Only key 2 should be in the diff (key 1 is unchanged)
        assertThat(diffs).hasSize(1);
        PersistentDiff.ByteArrayWrapper key2 =
                new PersistentDiff.ByteArrayWrapper(KeyCodec.encodeVertexKey(2L));
        assertThat(diffs).contains(key2);
    }
}
