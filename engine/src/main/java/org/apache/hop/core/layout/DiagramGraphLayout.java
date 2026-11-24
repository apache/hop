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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.hop.core.diagram.DiagramLayoutAlgorithm;

/** Pure layout boundary: computes positions without changing nodes, edges, or diagram models. */
public final class DiagramGraphLayout {

  /** Minimum empty space between complete footprints, in diagram units. */
  public static final int DEFAULT_GAP = 40;

  /** Minimum top and left footprint coordinate, in diagram units. */
  public static final int DEFAULT_MARGIN = 50;

  private DiagramGraphLayout() {}

  /**
   * Computes immutable anchor positions keyed by node identity, not equality.
   *
   * <p>Coordinates are deterministic for the input node and edge order. The returned map has no
   * defined iteration order; use {@link LayoutGraph#nodes()} to visit positions in input order.
   *
   * <p>GRID uses a square-ish, row-major grid with {@code ceil(sqrt(nodeCount))} columns. Each
   * footprint's top-left is aligned to its cell, accounting for negative label/decorations offsets.
   * Edges do not affect GRID positions. LAYERED delegates to {@link LayeredGraphLayout} with
   * left-to-right flow, conservative footprint spacing, and snapping disabled so it cannot reduce
   * the requested clearance. Neither algorithm requires preexisting node positions.
   *
   * @throws NullPointerException if any argument, node, edge, or bounds is null
   * @throws IllegalArgumentException if the same node instance occurs more than once or an edge
   *     endpoint is not a node instance in the graph
   * @throws ArithmeticException if footprint spacing or computed coordinates exceed integer range
   */
  public static <N, E> Map<N, LayoutPosition> layout(
      final LayoutGraph<N, E> graph,
      final Function<N, NodeBounds> bounds,
      final DiagramLayoutAlgorithm algorithm) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(bounds, "bounds");
    Objects.requireNonNull(algorithm, "algorithm");
    final var nodes = List.copyOf(graph.nodes());
    final var indices = new IdentityHashMap<N, Integer>();
    final var footprints = new ArrayList<NodeBounds>(nodes.size());
    for (int i = 0; i < nodes.size(); i++) {
      final var node = nodes.get(i);
      if (indices.put(node, i) != null) {
        throw new IllegalArgumentException("Layout graph contains duplicate nodes");
      }
      footprints.add(Objects.requireNonNull(bounds.apply(node), "node bounds"));
    }
    final var edges = new ArrayList<int[]>();
    for (final var edge : graph.edges()) {
      Objects.requireNonNull(edge, "edge");
      final var source = indices.get(graph.source(edge));
      final var target = indices.get(graph.target(edge));
      if (source == null || target == null) {
        throw new IllegalArgumentException("Layout edge endpoint is outside the graph");
      }
      edges.add(new int[] {source, target});
    }
    if (nodes.isEmpty()) {
      return Collections.emptyMap();
    }
    final LayoutPosition[] positions =
        switch (algorithm) {
          case GRID -> grid(footprints);
          case LAYERED -> layered(footprints, edges);
        };
    final var result = new IdentityHashMap<N, LayoutPosition>();
    for (int i = 0; i < nodes.size(); i++) {
      result.put(nodes.get(i), positions[i]);
    }
    return Collections.unmodifiableMap(result);
  }

  private static LayoutPosition[] grid(final List<NodeBounds> bounds) {
    int width = 0;
    int height = 0;
    for (final var footprint : bounds) {
      width = Math.max(width, footprint.getWidth());
      height = Math.max(height, footprint.getHeight());
    }
    final long columnSpacing = (long) width + DEFAULT_GAP;
    final long rowSpacing = (long) height + DEFAULT_GAP;
    final int columns = (int) Math.ceil(Math.sqrt(bounds.size()));
    final var positions = new LayoutPosition[bounds.size()];
    for (int i = 0; i < bounds.size(); i++) {
      final var footprint = bounds.get(i);
      positions[i] =
          new LayoutPosition(
              Math.toIntExact(DEFAULT_MARGIN + (i % columns) * columnSpacing - footprint.getX()),
              Math.toIntExact(DEFAULT_MARGIN + (i / columns) * rowSpacing - footprint.getY()));
    }
    return positions;
  }

  private static LayoutPosition[] layered(final List<NodeBounds> bounds, final List<int[]> edges) {
    int minX = Integer.MAX_VALUE;
    int minY = Integer.MAX_VALUE;
    long maxX = Long.MIN_VALUE;
    long maxY = Long.MIN_VALUE;
    for (final var footprint : bounds) {
      minX = Math.min(minX, footprint.getX());
      minY = Math.min(minY, footprint.getY());
      maxX = Math.max(maxX, (long) footprint.getX() + footprint.getWidth());
      maxY = Math.max(maxY, (long) footprint.getY() + footprint.getHeight());
    }
    final int xSpacing =
        Math.toIntExact(Math.max(LayeredGraphLayout.DEFAULT_X_SPACING, maxX - minX + DEFAULT_GAP));
    final int ySpacing =
        Math.toIntExact(Math.max(LayeredGraphLayout.DEFAULT_Y_SPACING, maxY - minY + DEFAULT_GAP));
    // The delegate uses int arithmetic; reject layouts whose worst-case bands could overflow.
    Math.toIntExact(DEFAULT_MARGIN + 2L * bounds.size() * Math.max(xSpacing, ySpacing));
    final var options =
        new LayeredGraphLayout.Options()
            .setLayerSpacing(xSpacing)
            .setNodeSpacing(ySpacing)
            .setGridSize(1);
    final var positions = new LayoutPosition[bounds.size()];
    final int offsetX = minX;
    final int offsetY = minY;
    LayeredGraphLayout.layout(
        bounds.size(),
        edges,
        options,
        (index, x, y) ->
            positions[index] =
                new LayoutPosition(
                    Math.toIntExact((long) x - offsetX), Math.toIntExact((long) y - offsetY)));
    return positions;
  }
}
