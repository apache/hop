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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reusable layered (Sugiyama-style) DAG auto-layout core, decoupled from any particular Hop model.
 *
 * <p>It operates on an abstract graph: a node count {@code n} (nodes indexed 0..n-1), a list of
 * directed edges as {@code int[]{fromIndex, toIndex}}, and a {@link PositionSink} callback used to
 * report the computed (x, y) coordinate for each node index. Both {@link
 * org.apache.hop.pipeline.PipelineMetaLayout} and {@link
 * org.apache.hop.workflow.WorkflowMetaLayout} are thin adapters over this class.
 *
 * <p>Algorithm:
 *
 * <ol>
 *   <li>Build a directed graph from the supplied edges (deduplicated, self-loops dropped).
 *   <li>Cycle removal: a DFS detects back-edges (edges pointing at a node currently on the DFS
 *       stack). Back-edges are ignored for ranking only.
 *   <li>Layer (rank/column) assignment via longest-path: roots (no forward incoming edge) get rank
 *       0; every other node = max(rank(pred)) + 1.
 *   <li>Ordering within a layer: barycenter sweeps (down then up, a few iterations) to reduce edge
 *       crossings.
 *   <li>Coordinate assignment: x = MARGIN_X + rank * X_SPACING, y depends on the order within the
 *       layer. Weakly-connected components are stacked in non-overlapping vertical bands.
 * </ol>
 *
 * <p>Robust against empty graphs, a single node, cycles and disconnected nodes.
 */
public final class LayeredGraphLayout {

  public static final int DEFAULT_X_SPACING = 150;
  public static final int DEFAULT_Y_SPACING = 120;
  public static final int DEFAULT_ITERATIONS = 4;
  public static final int MARGIN_X = 50;
  public static final int MARGIN_Y = 50;

  /** Hop snaps icon positions to a 16px grid. Keep spacing a multiple of this. */
  private static final int GRID = 16;

  /** The direction in which the graph flows from roots towards leaves. */
  public enum Direction {
    LEFT_RIGHT,
    RIGHT_LEFT,
    TOP_BOTTOM,
    BOTTOM_TOP
  }

  /** Tunable layout options. Defaults reproduce the original left-to-right behaviour. */
  public static final class Options {
    private Direction direction = Direction.LEFT_RIGHT;
    private int layerSpacing = DEFAULT_X_SPACING;
    private int nodeSpacing = DEFAULT_Y_SPACING;
    private int crossingIterations = DEFAULT_ITERATIONS;
    private boolean moveNotes = true;

    public Direction getDirection() {
      return direction;
    }

    public Options setDirection(Direction direction) {
      this.direction = direction == null ? Direction.LEFT_RIGHT : direction;
      return this;
    }

    /** Distance between consecutive layers, along the flow direction. */
    public int getLayerSpacing() {
      return layerSpacing;
    }

    public Options setLayerSpacing(int layerSpacing) {
      this.layerSpacing = layerSpacing;
      return this;
    }

    /** Distance between nodes within the same layer, perpendicular to the flow direction. */
    public int getNodeSpacing() {
      return nodeSpacing;
    }

    public Options setNodeSpacing(int nodeSpacing) {
      this.nodeSpacing = nodeSpacing;
      return this;
    }

    /** Number of barycenter crossing-reduction sweeps. */
    public int getCrossingIterations() {
      return crossingIterations;
    }

    public Options setCrossingIterations(int crossingIterations) {
      this.crossingIterations = crossingIterations;
      return this;
    }

    /**
     * Whether notes should be moved along with the node they sit closest to. The core layout
     * ignores this flag; the {@code *MetaLayout} adapters honour it.
     */
    public boolean isMoveNotes() {
      return moveNotes;
    }

    public Options setMoveNotes(boolean moveNotes) {
      this.moveNotes = moveNotes;
      return this;
    }
  }

  /**
   * Index of the node nearest to {@code (x, y)} whose distance is within {@code maxDistance}, or -1
   * if no node is close enough. Used to bind free-floating items (e.g. notes) to a node.
   *
   * @param x the reference x coordinate
   * @param y the reference y coordinate
   * @param nodeX the node x coordinates
   * @param nodeY the node y coordinates (same length as {@code nodeX})
   * @param maxDistance the maximum distance to consider a node a match
   * @return the index of the nearest in-range node, or -1
   */
  public static int nearestNode(int x, int y, int[] nodeX, int[] nodeY, double maxDistance) {
    int best = -1;
    double bestDistSq = maxDistance * maxDistance;
    for (int i = 0; i < nodeX.length; i++) {
      double dx = (double) x - nodeX[i];
      double dy = (double) y - nodeY[i];
      double distSq = dx * dx + dy * dy;
      if (distSq <= bestDistSq) {
        bestDistSq = distSq;
        best = i;
      }
    }
    return best;
  }

  /** Callback to receive the computed position for each node index. */
  public interface PositionSink {
    void setPosition(int nodeIndex, int x, int y);
  }

  private LayeredGraphLayout() {}

  /** Layout with default options. */
  public static void layout(int n, List<int[]> edges, PositionSink sink) {
    layout(n, edges, new Options(), sink);
  }

  /**
   * Layout with explicit spacing (left-to-right). Kept for backward compatibility.
   *
   * @param n number of nodes, indexed 0..n-1
   * @param edges directed edges as {@code int[]{fromIndex, toIndex}}
   * @param xSpacing horizontal distance between layers
   * @param ySpacing vertical distance between nodes in a layer
   * @param sink callback receiving the computed (x, y) per node index
   */
  public static void layout(
      int n, List<int[]> edges, int xSpacing, int ySpacing, PositionSink sink) {
    layout(n, edges, new Options().setLayerSpacing(xSpacing).setNodeSpacing(ySpacing), sink);
  }

  /**
   * Layout with explicit options.
   *
   * @param n number of nodes, indexed 0..n-1
   * @param edges directed edges as {@code int[]{fromIndex, toIndex}}
   * @param options the layout tuning options (never null)
   * @param sink callback receiving the computed (x, y) per node index
   */
  public static void layout(int n, List<int[]> edges, Options options, PositionSink sink) {
    if (n <= 0 || sink == null) {
      return;
    }
    if (options == null) {
      options = new Options();
    }

    // Build adjacency (directed, deduplicated).
    List<Set<Integer>> out = new ArrayList<>();
    List<Set<Integer>> in = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(new HashSet<>());
      in.add(new HashSet<>());
    }
    if (edges != null) {
      for (int[] e : edges) {
        if (e == null || e.length < 2) {
          continue;
        }
        int fi = e[0];
        int ti = e[1];
        if (fi < 0 || ti < 0 || fi >= n || ti >= n || fi == ti) {
          continue;
        }
        out.get(fi).add(ti);
        in.get(ti).add(fi);
      }
    }

    // Cycle removal: find back-edges via DFS so layering uses only forward edges.
    Set<Long> backEdges = findBackEdges(n, out);

    // Forward adjacency (excluding back-edges).
    List<List<Integer>> fOut = new ArrayList<>();
    List<List<Integer>> fIn = new ArrayList<>();
    int[] forwardInDegree = new int[n];
    for (int i = 0; i < n; i++) {
      fOut.add(new ArrayList<>());
      fIn.add(new ArrayList<>());
    }
    for (int u = 0; u < n; u++) {
      for (int v : out.get(u)) {
        if (backEdges.contains(edgeKey(u, v))) {
          continue;
        }
        fOut.get(u).add(v);
        fIn.get(v).add(u);
        forwardInDegree[v]++;
      }
    }

    // Rank assignment via longest path (topological order on the DAG of forward edges).
    int[] rank = new int[n];
    int[] degree = forwardInDegree.clone();
    List<Integer> queue = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      if (degree[i] == 0) {
        queue.add(i);
      }
    }
    int qh = 0;
    int processed = 0;
    while (qh < queue.size()) {
      int u = queue.get(qh++);
      processed++;
      for (int v : fOut.get(u)) {
        if (rank[v] < rank[u] + 1) {
          rank[v] = rank[u] + 1;
        }
        if (--degree[v] == 0) {
          queue.add(v);
        }
      }
    }
    // Safety net: if cycle removal missed something, force any remaining nodes forward.
    if (processed < n) {
      for (int u = 0; u < n; u++) {
        for (int v : fOut.get(u)) {
          if (rank[v] <= rank[u]) {
            rank[v] = rank[u] + 1;
          }
        }
      }
    }

    // Group nodes by rank.
    int maxRank = 0;
    for (int i = 0; i < n; i++) {
      maxRank = Math.max(maxRank, rank[i]);
    }
    List<List<Integer>> layers = new ArrayList<>();
    for (int r = 0; r <= maxRank; r++) {
      layers.add(new ArrayList<>());
    }
    for (int i = 0; i < n; i++) {
      layers.get(rank[i]).add(i);
    }

    // Determine weakly-connected components (using all edges, both directions).
    int[] component = new int[n];
    java.util.Arrays.fill(component, -1);
    int nrComponents = 0;
    for (int s = 0; s < n; s++) {
      if (component[s] != -1) {
        continue;
      }
      List<Integer> stack = new ArrayList<>();
      stack.add(s);
      component[s] = nrComponents;
      while (!stack.isEmpty()) {
        int u = stack.remove(stack.size() - 1);
        for (int v : out.get(u)) {
          if (component[v] == -1) {
            component[v] = nrComponents;
            stack.add(v);
          }
        }
        for (int v : in.get(u)) {
          if (component[v] == -1) {
            component[v] = nrComponents;
            stack.add(v);
          }
        }
      }
      nrComponents++;
    }

    // Ordering within each layer: barycenter sweeps to reduce crossings.
    Map<Integer, Integer> orderInLayer = new HashMap<>();
    for (List<Integer> layer : layers) {
      layer.sort(
          (a, b) -> {
            if (component[a] != component[b]) {
              return Integer.compare(component[a], component[b]);
            }
            return Integer.compare(a, b);
          });
      for (int i = 0; i < layer.size(); i++) {
        orderInLayer.put(layer.get(i), i);
      }
    }

    int iterations = Math.max(0, options.getCrossingIterations());
    for (int iter = 0; iter < iterations; iter++) {
      boolean down = (iter % 2) == 0;
      if (down) {
        for (int r = 1; r <= maxRank; r++) {
          sweep(layers.get(r), fIn, component, orderInLayer);
        }
      } else {
        for (int r = maxRank - 1; r >= 0; r--) {
          sweep(layers.get(r), fOut, component, orderInLayer);
        }
      }
    }

    // Spacing along the flow direction (between layers) and perpendicular (between nodes).
    int flowSpace = snap(options.getLayerSpacing());
    int crossSpace = snap(options.getNodeSpacing());
    Direction direction = options.getDirection();
    boolean horizontal = direction == Direction.LEFT_RIGHT || direction == Direction.RIGHT_LEFT;
    boolean reversed = direction == Direction.RIGHT_LEFT || direction == Direction.BOTTOM_TOP;

    // Assign a cross-axis band per component so components never overlap.
    int[] componentBaseRow = new int[nrComponents];
    int[] componentRows = new int[nrComponents];
    for (List<Integer> layer : layers) {
      int[] perComp = new int[nrComponents];
      for (int node : layer) {
        perComp[component[node]]++;
      }
      for (int c = 0; c < nrComponents; c++) {
        componentRows[c] = Math.max(componentRows[c], perComp[c]);
      }
    }
    int acc = 0;
    for (int c = 0; c < nrComponents; c++) {
      componentBaseRow[c] = acc;
      acc += componentRows[c] + 1; // 1-row gap between component bands
    }

    // Place nodes. The rank gives the position along the flow axis, the order within a
    // (layer, component) gives the position on the cross axis.
    for (List<Integer> layer : layers) {
      Map<Integer, Integer> rowInComp = new HashMap<>();
      for (int node : layer) {
        int c = component[node];
        int row = rowInComp.getOrDefault(c, 0);
        rowInComp.put(c, row + 1);

        int flowIndex = reversed ? (maxRank - rank[node]) : rank[node];
        int crossIndex = componentBaseRow[c] + row;
        int along = flowIndex * flowSpace;
        int cross = crossIndex * crossSpace;

        int x = horizontal ? MARGIN_X + along : MARGIN_X + cross;
        int y = horizontal ? MARGIN_Y + cross : MARGIN_Y + along;
        sink.setPosition(node, snap(x), snap(y));
      }
    }
  }

  private static void sweep(
      List<Integer> layer,
      List<List<Integer>> neighbors,
      int[] component,
      Map<Integer, Integer> orderInLayer) {
    Map<Integer, Double> bary = new HashMap<>();
    for (int node : layer) {
      List<Integer> nb = neighbors.get(node);
      if (nb.isEmpty()) {
        bary.put(node, (double) orderInLayer.getOrDefault(node, 0));
      } else {
        double sum = 0;
        for (int m : nb) {
          sum += orderInLayer.getOrDefault(m, 0);
        }
        bary.put(node, sum / nb.size());
      }
    }
    layer.sort(
        (a, b) -> {
          if (component[a] != component[b]) {
            return Integer.compare(component[a], component[b]);
          }
          int cmp = Double.compare(bary.get(a), bary.get(b));
          if (cmp != 0) {
            return cmp;
          }
          return Integer.compare(a, b);
        });
    for (int i = 0; i < layer.size(); i++) {
      orderInLayer.put(layer.get(i), i);
    }
  }

  /** DFS to identify back-edges (edges to a node currently on the recursion stack). */
  private static Set<Long> findBackEdges(int n, List<Set<Integer>> out) {
    Set<Long> backEdges = new HashSet<>();
    int[] state = new int[n]; // 0=unvisited, 1=on-stack, 2=done
    for (int start = 0; start < n; start++) {
      if (state[start] != 0) {
        continue;
      }
      List<int[]> stack = new ArrayList<>(); // {node, neighborCursor}
      Map<Integer, List<Integer>> neighborLists = new HashMap<>();
      stack.add(new int[] {start, 0});
      state[start] = 1;
      neighborLists.put(start, new ArrayList<>(out.get(start)));
      while (!stack.isEmpty()) {
        int[] top = stack.get(stack.size() - 1);
        int u = top[0];
        List<Integer> nb = neighborLists.get(u);
        if (top[1] >= nb.size()) {
          state[u] = 2;
          stack.remove(stack.size() - 1);
          continue;
        }
        int v = nb.get(top[1]);
        top[1]++;
        if (state[v] == 1) {
          backEdges.add(edgeKey(u, v));
        } else if (state[v] == 0) {
          state[v] = 1;
          neighborLists.put(v, new ArrayList<>(out.get(v)));
          stack.add(new int[] {v, 0});
        }
      }
    }
    return backEdges;
  }

  private static long edgeKey(int u, int v) {
    return (((long) u) << 32) | (v & 0xffffffffL);
  }

  private static int snap(int value) {
    return Math.round((float) value / GRID) * GRID;
  }
}
