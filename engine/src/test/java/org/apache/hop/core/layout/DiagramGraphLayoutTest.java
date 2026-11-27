/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.layout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.diagram.DiagramLayoutAlgorithm;
import org.junit.jupiter.api.Test;

public class DiagramGraphLayoutTest {

  @Test
  void gridUsesStableRowMajorOrderAndCompleteFootprints() {
    final var nodes = List.of("c", "a", "e", "b", "d");
    final var graph = new Graph(nodes, List.of(List.of("a", "b")));
    final var footprints =
        Map.of(
            "c", new NodeBounds(-100, -20, 250, 60),
            "a", new NodeBounds(-10, 0, 30, 80),
            "e", new NodeBounds(15, -40, 100, 100),
            "b", new NodeBounds(-150, 10, 400, 50),
            "d", new NodeBounds(0, 0, 0, 0));

    final var positions =
        DiagramGraphLayout.layout(graph, footprints::get, DiagramLayoutAlgorithm.GRID);

    assertEquals(nodes.size(), positions.size());
    assertEquals(new LayoutPosition(150, 70), positions.get("c"));
    assertEquals(new LayoutPosition(500, 50), positions.get("a"));
    assertEquals(new LayoutPosition(915, 90), positions.get("e"));
    assertEquals(new LayoutPosition(200, 180), positions.get("b"));
    assertEquals(new LayoutPosition(490, 190), positions.get("d"));
    assertClearance(graph.nodes(), positions, footprints);
    assertEquals(
        orderedPositions(graph, positions),
        orderedPositions(
            graph, DiagramGraphLayout.layout(graph, footprints::get, DiagramLayoutAlgorithm.GRID)));
    assertEquals(
        orderedPositions(graph, positions),
        orderedPositions(
            graph,
            DiagramGraphLayout.layout(
                new Graph(nodes, List.of()), footprints::get, DiagramLayoutAlgorithm.GRID)));
    assertThrows(
        UnsupportedOperationException.class, () -> positions.put("new", new LayoutPosition(0, 0)));
    assertEquals(List.of("c", "a", "e", "b", "d"), graph.nodes());
    assertEquals(List.of(List.of("a", "b")), graph.edges());
  }

  @Test
  void layeredReusesExistingIndexedLayout() {
    final var graph =
        new Graph(
            List.of("root", "right", "left", "join"),
            List.of(
                List.of("root", "right"),
                List.of("root", "left"),
                List.of("right", "join"),
                List.of("left", "join")));
    final var positions =
        DiagramGraphLayout.layout(
            graph, node -> new NodeBounds(0, 0, 32, 32), DiagramLayoutAlgorithm.LAYERED);
    final var expected = new LayoutPosition[4];
    LayeredGraphLayout.layout(
        4,
        List.of(new int[] {0, 1}, new int[] {0, 2}, new int[] {1, 3}, new int[] {2, 3}),
        new LayeredGraphLayout.Options().setGridSize(1),
        (index, x, y) -> expected[index] = new LayoutPosition(x, y));
    assertEquals(graph.nodes().size(), positions.size());
    for (int i = 0; i < graph.nodes().size(); i++) {
      assertEquals(expected[i], positions.get(graph.nodes().get(i)));
    }
    for (final var edge : graph.edges()) {
      assertTrue(
          positions.get(graph.source(edge)).getX() < positions.get(graph.target(edge)).getX());
    }
  }

  @Test
  void layeredHandlesWideLabelsCyclesAndDisconnectedNodesDeterministically() {
    final var graph =
        new Graph(
            List.of("a", "b", "c", "isolated", "other"),
            List.of(
                List.of("a", "b"),
                List.of("a", "b"),
                List.of("b", "c"),
                List.of("c", "a"),
                List.of("b", "b")));
    final var footprints =
        Map.of(
            "a", new NodeBounds(-300, -80, 700, 220),
            "b", new NodeBounds(-5, 10, 40, 700),
            "c", new NodeBounds(60, -350, 200, 400),
            "isolated", new NodeBounds(-50, -10, 100, 40),
            "other", new NodeBounds(0, 0, 32, 32));
    final var positions =
        DiagramGraphLayout.layout(graph, footprints::get, DiagramLayoutAlgorithm.LAYERED);
    assertEquals(graph.nodes().size(), positions.size());
    assertClearance(graph.nodes(), positions, footprints);
    for (int i = 0; i < 5; i++) {
      assertEquals(
          orderedPositions(graph, positions),
          orderedPositions(
              graph,
              DiagramGraphLayout.layout(graph, footprints::get, DiagramLayoutAlgorithm.LAYERED)));
    }
  }

  @Test
  void emptyAndSingleNodeGraphsWorkForBothAlgorithms() {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      assertTrue(
          DiagramGraphLayout.layout(
                  new Graph(List.of(), List.of()),
                  node -> {
                    throw new AssertionError("Empty graph must not ask for bounds");
                  },
                  algorithm)
              .isEmpty());
      assertEquals(
          new LayoutPosition(70, 80),
          DiagramGraphLayout.layout(
                  new Graph(List.of("only"), List.of()),
                  node -> new NodeBounds(-20, -30, 100, 50),
                  algorithm)
              .get("only"));
    }
  }

  @Test
  void equalButDistinctNodesHaveIndependentPositionsAndIdentityMappedEdges() {
    final var first = new String("same-name");
    final var second = new String("same-name");
    final var outside = new String("same-name");
    final var graph = new Graph(List.of(first, second), List.of(List.of(second, first)));
    assertEquals(first, second);

    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      final var positions =
          DiagramGraphLayout.layout(graph, node -> new NodeBounds(0, 0, 32, 32), algorithm);
      assertEquals(2, positions.size());
      assertNotEquals(positions.get(first), positions.get(second));
      assertNull(positions.get(outside));
      if (algorithm == DiagramLayoutAlgorithm.GRID) {
        assertEquals(new LayoutPosition(50, 50), positions.get(first));
        assertEquals(new LayoutPosition(122, 50), positions.get(second));
      } else {
        assertTrue(positions.get(second).getX() < positions.get(first).getX());
      }
      assertEquals(
          orderedPositions(graph, positions),
          orderedPositions(
              graph,
              DiagramGraphLayout.layout(graph, node -> new NodeBounds(0, 0, 32, 32), algorithm)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> positions.put(first, new LayoutPosition(0, 0)));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DiagramGraphLayout.layout(
                  new Graph(List.of(first, first), List.of()),
                  node -> new NodeBounds(0, 0, 32, 32),
                  algorithm));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DiagramGraphLayout.layout(
                  new Graph(graph.nodes(), List.of(List.of(outside, first))),
                  node -> new NodeBounds(0, 0, 32, 32),
                  algorithm));
    }
  }

  @Test
  void rejectsInvalidGraphAndBounds() {
    assertThrows(IllegalArgumentException.class, () -> new NodeBounds(0, 0, -1, 32));
    assertThrows(IllegalArgumentException.class, () -> new NodeBounds(0, 0, 32, -1));
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DiagramGraphLayout.layout(
                  new Graph(List.of("a", "a"), List.of()),
                  node -> new NodeBounds(0, 0, 32, 32),
                  algorithm));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DiagramGraphLayout.layout(
                  new Graph(List.of("a"), List.of(List.of("a", "missing"))),
                  node -> new NodeBounds(0, 0, 32, 32),
                  algorithm));
      assertThrows(
          NullPointerException.class,
          () ->
              DiagramGraphLayout.layout(
                  new Graph(List.of("a"), List.of()), node -> null, algorithm));
    }
  }

  private void assertClearance(
      final List<String> nodes,
      final Map<String, LayoutPosition> positions,
      final Map<String, NodeBounds> footprints) {
    for (int i = 0; i < nodes.size(); i++) {
      final var a = positions.get(nodes.get(i));
      final var aBounds = footprints.get(nodes.get(i));
      final long ax = (long) a.getX() + aBounds.getX();
      final long ay = (long) a.getY() + aBounds.getY();
      assertTrue(ax >= DiagramGraphLayout.DEFAULT_MARGIN);
      assertTrue(ay >= DiagramGraphLayout.DEFAULT_MARGIN);
      for (int j = i + 1; j < nodes.size(); j++) {
        final var b = positions.get(nodes.get(j));
        final var bBounds = footprints.get(nodes.get(j));
        final long bx = (long) b.getX() + bBounds.getX();
        final long by = (long) b.getY() + bBounds.getY();
        final int gap = DiagramGraphLayout.DEFAULT_GAP;
        assertTrue(
            ax + aBounds.getWidth() + gap <= bx
                || bx + bBounds.getWidth() + gap <= ax
                || ay + aBounds.getHeight() + gap <= by
                || by + bBounds.getHeight() + gap <= ay,
            "Footprints overlap or lack clearance: " + nodes.get(i) + ", " + nodes.get(j));
      }
    }
  }

  private List<LayoutPosition> orderedPositions(
      final Graph graph, final Map<String, LayoutPosition> positions) {
    return graph.nodes().stream().map(positions::get).toList();
  }

  private record Graph(List<String> nodes, List<List<String>> edges)
      implements LayoutGraph<String, List<String>> {
    @Override
    public String source(final List<String> edge) {
      return edge.get(0);
    }

    @Override
    public String target(final List<String> edge) {
      return edge.get(1);
    }
  }
}
