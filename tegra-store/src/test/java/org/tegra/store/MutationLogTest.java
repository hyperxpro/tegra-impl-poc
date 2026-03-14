package org.tegra.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;
import org.tegra.store.log.GraphMutation;
import org.tegra.store.log.MutationLog;
import org.tegra.store.version.ByteArray;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MutationLog: append, read, sealed type roundtrip.
 */
class MutationLogTest {

    private MutationLog mutationLog;

    @BeforeEach
    void setUp() {
        mutationLog = new MutationLog();
    }

    @Test
    void appendAndReadMutations() {
        ByteArray versionId = ByteArray.fromString("v1");
        MutationLog.MutationLogWriter writer = mutationLog.openWriter(versionId);

        VertexData vertex = new VertexData(1L, Map.of("name", new PropertyValue.StringProperty("Alice")));
        writer.append(new GraphMutation.AddVertex(vertex));
        writer.append(new GraphMutation.RemoveVertex(2L));

        assertThat(writer.size()).isEqualTo(2);

        MutationLog.MutationLogReader reader = mutationLog.openReader(versionId);
        assertThat(reader.size()).isEqualTo(2);
        assertThat(reader.hasNext()).isTrue();

        GraphMutation m1 = reader.next();
        assertThat(m1).isInstanceOf(GraphMutation.AddVertex.class);
        assertThat(((GraphMutation.AddVertex) m1).vertexData().vertexId()).isEqualTo(1L);

        GraphMutation m2 = reader.next();
        assertThat(m2).isInstanceOf(GraphMutation.RemoveVertex.class);
        assertThat(((GraphMutation.RemoveVertex) m2).vertexId()).isEqualTo(2L);

        assertThat(reader.hasNext()).isFalse();
    }

    @Test
    void readAllMutations() {
        ByteArray versionId = ByteArray.fromString("v1");
        MutationLog.MutationLogWriter writer = mutationLog.openWriter(versionId);

        writer.append(new GraphMutation.AddVertex(new VertexData(1L, Map.of())));
        writer.append(new GraphMutation.AddVertex(new VertexData(2L, Map.of())));
        writer.append(new GraphMutation.AddVertex(new VertexData(3L, Map.of())));

        MutationLog.MutationLogReader reader = mutationLog.openReader(versionId);
        List<GraphMutation> all = reader.readAll();

        assertThat(all).hasSize(3);
        assertThat(reader.hasNext()).isFalse();
    }

    @Test
    void readEmptyLog() {
        ByteArray versionId = ByteArray.fromString("empty");
        MutationLog.MutationLogReader reader = mutationLog.openReader(versionId);

        assertThat(reader.hasNext()).isFalse();
        assertThat(reader.size()).isZero();
    }

    @Test
    void allMutationTypes() {
        ByteArray versionId = ByteArray.fromString("v1");
        MutationLog.MutationLogWriter writer = mutationLog.openWriter(versionId);

        VertexData vertex = new VertexData(1L, Map.of());
        EdgeKey edgeKey = new EdgeKey(1L, 2L, (short) 0);
        EdgeData edge = new EdgeData(edgeKey, Map.of());

        writer.append(new GraphMutation.AddVertex(vertex));
        writer.append(new GraphMutation.RemoveVertex(1L));
        writer.append(new GraphMutation.AddEdge(edge));
        writer.append(new GraphMutation.RemoveEdge(1L, 2L, (short) 0));
        writer.append(new GraphMutation.UpdateVertexProperty(1L, "name",
                new PropertyValue.StringProperty("Alice")));
        writer.append(new GraphMutation.UpdateEdgeProperty(1L, 2L, (short) 0, "weight",
                new PropertyValue.DoubleProperty(1.5)));

        MutationLog.MutationLogReader reader = mutationLog.openReader(versionId);
        List<GraphMutation> all = reader.readAll();

        assertThat(all).hasSize(6);

        // Verify sealed type pattern matching works
        for (GraphMutation mutation : all) {
            switch (mutation) {
                case GraphMutation.AddVertex av -> assertThat(av.vertexData()).isNotNull();
                case GraphMutation.RemoveVertex rv -> assertThat(rv.vertexId()).isEqualTo(1L);
                case GraphMutation.AddEdge ae -> assertThat(ae.edgeData()).isNotNull();
                case GraphMutation.RemoveEdge re -> {
                    assertThat(re.srcId()).isEqualTo(1L);
                    assertThat(re.dstId()).isEqualTo(2L);
                }
                case GraphMutation.UpdateVertexProperty uvp -> {
                    assertThat(uvp.propertyKey()).isEqualTo("name");
                }
                case GraphMutation.UpdateEdgeProperty uep -> {
                    assertThat(uep.propertyKey()).isEqualTo("weight");
                }
            }
        }
    }
}
