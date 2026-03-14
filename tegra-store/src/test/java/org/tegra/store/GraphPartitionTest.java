package org.tegra.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.api.Edge;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GraphPartitionTest {

    private GraphPartition<String, String> partition;

    @BeforeEach
    void setUp() {
        partition = new GraphPartition<>();
    }

    @Test
    void testEmptyPartition() {
        assertThat(partition.vertexCount()).isZero();
        assertThat(partition.edgeCount()).isZero();
        assertThat(partition.getVertex(1L)).isNull();
        assertThat(partition.getOutEdges(1L)).isEmpty();
        assertThat(partition.getInEdges(1L)).isEmpty();
    }

    @Test
    void testAddAndGetVertex() {
        partition.addVertex(1L, "Alice");
        partition.addVertex(2L, "Bob");

        assertThat(partition.getVertex(1L)).isEqualTo("Alice");
        assertThat(partition.getVertex(2L)).isEqualTo("Bob");
        assertThat(partition.vertexCount()).isEqualTo(2);
    }

    @Test
    void testRemoveVertex() {
        partition.addVertex(1L, "Alice");
        partition.addVertex(2L, "Bob");
        partition.addEdge(1L, 2L, "knows");

        partition.removeVertex(1L);

        assertThat(partition.getVertex(1L)).isNull();
        assertThat(partition.vertexCount()).isEqualTo(1);
        assertThat(partition.getOutEdges(1L)).isEmpty();
        assertThat(partition.getInEdges(2L)).isEmpty();
        assertThat(partition.edgeCount()).isZero();
    }

    @Test
    void testAddAndGetEdges() {
        partition.addVertex(1L, "Alice");
        partition.addVertex(2L, "Bob");
        partition.addVertex(3L, "Charlie");
        partition.addEdge(1L, 2L, "knows");
        partition.addEdge(1L, 3L, "follows");

        List<Edge<String>> outEdges = partition.getOutEdges(1L);
        assertThat(outEdges).hasSize(2);
        assertThat(outEdges).extracting(Edge::dst).containsExactlyInAnyOrder(2L, 3L);

        List<Edge<String>> inEdges = partition.getInEdges(2L);
        assertThat(inEdges).hasSize(1);
        assertThat(inEdges.getFirst().src()).isEqualTo(1L);
    }

    @Test
    void testRemoveEdge() {
        partition.addVertex(1L, "Alice");
        partition.addVertex(2L, "Bob");
        partition.addEdge(1L, 2L, "knows");

        partition.removeEdge(1L, 2L);

        assertThat(partition.getOutEdges(1L)).isEmpty();
        assertThat(partition.getInEdges(2L)).isEmpty();
        assertThat(partition.edgeCount()).isZero();
    }

    @Test
    void testSnapshot() {
        partition.addVertex(1L, "Alice");
        partition.addVertex(2L, "Bob");
        partition.addEdge(1L, 2L, "knows");

        VersionRoot<String, String> root = partition.snapshot();

        assertThat(root).isNotNull();
        assertThat(root.vertexData().size()).isEqualTo(2);
        assertThat(root.outEdges().get(1L)).isNotNull();
        assertThat(root.timestamp()).isNotNull();
    }

    @Test
    void testVertexCount() {
        assertThat(partition.vertexCount()).isZero();
        partition.addVertex(1L, "A");
        assertThat(partition.vertexCount()).isEqualTo(1);
        partition.addVertex(2L, "B");
        assertThat(partition.vertexCount()).isEqualTo(2);
        partition.removeVertex(1L);
        assertThat(partition.vertexCount()).isEqualTo(1);
    }

    @Test
    void testEdgeCount() {
        partition.addVertex(1L, "A");
        partition.addVertex(2L, "B");
        partition.addVertex(3L, "C");

        assertThat(partition.edgeCount()).isZero();
        partition.addEdge(1L, 2L, "e1");
        assertThat(partition.edgeCount()).isEqualTo(1);
        partition.addEdge(2L, 3L, "e2");
        assertThat(partition.edgeCount()).isEqualTo(2);
        partition.removeEdge(1L, 2L);
        assertThat(partition.edgeCount()).isEqualTo(1);
    }
}
