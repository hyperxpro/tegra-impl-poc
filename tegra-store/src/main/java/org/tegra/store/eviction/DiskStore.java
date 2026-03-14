package org.tegra.store.eviction;

import org.tegra.pds.art.ArtNode;
import org.tegra.pds.art.PersistentART;
import org.tegra.serde.ArtNodeSerializer;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeDataSerializer;
import org.tegra.serde.TegraSerializer;
import org.tegra.serde.VertexData;
import org.tegra.serde.VertexDataSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Subtree serialization to disk files using tegra-serde.
 * Writes serialized pART subtrees to disk for evicted versions.
 */
public final class DiskStore {

    private final Path directory;
    private final ArtNodeSerializer<VertexData> vertexSerializer;
    private final ArtNodeSerializer<EdgeData> edgeSerializer;

    public DiskStore(Path directory) {
        this.directory = directory;
        this.vertexSerializer = new ArtNodeSerializer<>(new VertexDataSerializer());
        this.edgeSerializer = new ArtNodeSerializer<>(new EdgeDataSerializer());
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create disk store directory", e);
        }
    }

    /**
     * Writes a vertex tree to disk and returns a pointer.
     */
    public DiskPointer writeVertexTree(ArtNode<VertexData> root, String fileName) {
        return writeTree(root, vertexSerializer, fileName);
    }

    /**
     * Writes an edge tree to disk and returns a pointer.
     */
    public DiskPointer writeEdgeTree(ArtNode<EdgeData> root, String fileName) {
        return writeTree(root, edgeSerializer, fileName);
    }

    /**
     * Reads a vertex tree from disk.
     */
    public ArtNode<VertexData> readVertexTree(DiskPointer pointer) {
        return readTree(pointer, vertexSerializer);
    }

    /**
     * Reads an edge tree from disk.
     */
    public ArtNode<EdgeData> readEdgeTree(DiskPointer pointer) {
        return readTree(pointer, edgeSerializer);
    }

    private <V> DiskPointer writeTree(ArtNode<V> root, ArtNodeSerializer<V> serializer, String fileName) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            PersistentART<V> art = PersistentART.fromRoot(root);
            serializer.serialize(art, dos);
            dos.flush();
            byte[] data = baos.toByteArray();
            Path filePath = directory.resolve(fileName);
            Files.write(filePath, data);
            return new DiskPointer(filePath, 0, data.length);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write tree to disk", e);
        }
    }

    private <V> ArtNode<V> readTree(DiskPointer pointer, ArtNodeSerializer<V> serializer) {
        try {
            byte[] data = Files.readAllBytes(pointer.filePath());
            ByteArrayInputStream bais = new ByteArrayInputStream(data, (int) pointer.offset(), pointer.length());
            DataInputStream dis = new DataInputStream(bais);
            PersistentART<V> art = serializer.deserializeTree(dis);
            return art.root();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read tree from disk", e);
        }
    }
}
