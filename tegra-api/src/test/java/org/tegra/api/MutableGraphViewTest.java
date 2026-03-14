package org.tegra.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MutableGraphViewTest {

    private MutableGraphView<String, Double> view;

    @BeforeEach
    void setUp() {
        Timelapse<String, Double> tl = Timelapse.create("test");
        view = tl.emptySnapshot().asMutable();
    }

    @Test
    void testAddVertex() {
        view.addVertex(1L, "Alice");
        assertThat(view.vertex(1L)).isPresent();
        assertThat(view.vertex(1L).get().properties()).isEqualTo("Alice");
    }

    @Test
    void testRemoveVertex() {
        view.addVertex(1L, "Alice");
        view.addVertex(2L, "Bob");
        view.addEdge(1L, 2L, 1.0);

        view.removeVertex(1L);

        assertThat(view.vertex(1L)).isEmpty();
        assertThat(view.vertexCount()).isEqualTo(1);
        // Edge from removed vertex should also be gone
        assertThat(view.outEdges(1L).toList()).isEmpty();
        assertThat(view.inEdges(2L).toList()).isEmpty();
    }

    @Test
    void testAddEdge() {
        view.addVertex(1L, "A");
        view.addVertex(2L, "B");
        view.addEdge(1L, 2L, 3.14);

        assertThat(view.edgeCount()).isEqualTo(1);
        assertThat(view.outEdges(1L).toList()).hasSize(1);
        assertThat(view.inEdges(2L).toList()).hasSize(1);

        Edge<Double> edge = view.outEdges(1L).findFirst().orElseThrow();
        assertThat(edge.src()).isEqualTo(1L);
        assertThat(edge.dst()).isEqualTo(2L);
        assertThat(edge.properties()).isEqualTo(3.14);
    }

    @Test
    void testRemoveEdge() {
        view.addVertex(1L, "A");
        view.addVertex(2L, "B");
        view.addEdge(1L, 2L, 1.0);

        assertThat(view.edgeCount()).isEqualTo(1);

        view.removeEdge(1L, 2L);

        assertThat(view.edgeCount()).isEqualTo(0);
        assertThat(view.outEdges(1L).toList()).isEmpty();
        assertThat(view.inEdges(2L).toList()).isEmpty();
    }

    @Test
    void testSetVertexProperty() {
        view.addVertex(1L, "original");
        view.setVertexProperty(1L, "updated");

        assertThat(view.vertex(1L).get().properties()).isEqualTo("updated");
    }

    @Test
    void testSetVertexPropertyOnNonExistentVertex() {
        assertThatThrownBy(() -> view.setVertexProperty(99L, "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("99");
    }

    @Test
    void testVertexCount() {
        assertThat(view.vertexCount()).isEqualTo(0);

        view.addVertex(1L, "A");
        assertThat(view.vertexCount()).isEqualTo(1);

        view.addVertex(2L, "B");
        assertThat(view.vertexCount()).isEqualTo(2);

        view.removeVertex(1L);
        assertThat(view.vertexCount()).isEqualTo(1);
    }

    @Test
    void testEdgeCount() {
        view.addVertex(1L, "A");
        view.addVertex(2L, "B");
        view.addVertex(3L, "C");

        assertThat(view.edgeCount()).isEqualTo(0);

        view.addEdge(1L, 2L, 1.0);
        assertThat(view.edgeCount()).isEqualTo(1);

        view.addEdge(2L, 3L, 2.0);
        assertThat(view.edgeCount()).isEqualTo(2);

        view.removeEdge(1L, 2L);
        assertThat(view.edgeCount()).isEqualTo(1);
    }
}
