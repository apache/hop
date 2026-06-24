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

package org.apache.hop.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.layout.LayeredGraphLayout;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * Layered (Sugiyama-style) DAG auto-layout for a {@link WorkflowMeta}.
 *
 * <p>It rewrites the x/y coordinates of every action so the graph reads cleanly left-to-right with
 * no overlaps. This is a thin adapter over {@link LayeredGraphLayout}; see that class for the
 * algorithm details. Robust against empty workflows, a single action, cycles (workflows commonly
 * loop back) and disconnected nodes.
 */
public final class WorkflowMetaLayout {

  public static final int DEFAULT_X_SPACING = LayeredGraphLayout.DEFAULT_X_SPACING;
  public static final int DEFAULT_Y_SPACING = LayeredGraphLayout.DEFAULT_Y_SPACING;
  public static final int MARGIN_X = LayeredGraphLayout.MARGIN_X;
  public static final int MARGIN_Y = LayeredGraphLayout.MARGIN_Y;

  private WorkflowMetaLayout() {}

  /** Layout the whole workflow with default options. */
  public static void layout(WorkflowMeta workflowMeta) {
    layout(workflowMeta, new LayeredGraphLayout.Options());
  }

  /**
   * Layout the whole workflow with explicit options.
   *
   * @param workflowMeta the workflow to lay out
   * @param options the layout tuning options
   */
  public static void layout(WorkflowMeta workflowMeta, LayeredGraphLayout.Options options) {
    layout(workflowMeta, options, null);
  }

  /**
   * Layout the given workflow. When {@code subset} is non-empty, only those actions are arranged
   * (using the hops between them) and the result is anchored to the top-left of the area the subset
   * originally occupied, so the rest of the graph stays put. When {@code subset} is null or empty,
   * the whole workflow is laid out.
   *
   * @param workflowMeta the workflow to lay out
   * @param options the layout tuning options
   * @param subset the actions to arrange, or null/empty for the whole workflow
   */
  public static void layout(
      WorkflowMeta workflowMeta, LayeredGraphLayout.Options options, List<ActionMeta> subset) {
    if (workflowMeta == null) {
      return;
    }
    if (options == null) {
      options = new LayeredGraphLayout.Options();
    }

    final List<ActionMeta> actions = new ArrayList<>();
    Map<ActionMeta, Integer> index = new HashMap<>();
    if (subset == null || subset.isEmpty()) {
      for (int i = 0; i < workflowMeta.nrActions(); i++) {
        ActionMeta a = workflowMeta.getAction(i);
        index.put(a, actions.size());
        actions.add(a);
      }
    } else {
      for (ActionMeta a : subset) {
        if (a != null && !index.containsKey(a)) {
          index.put(a, actions.size());
          actions.add(a);
        }
      }
    }
    int n = actions.size();
    if (n == 0) {
      return;
    }

    List<int[]> edges = new ArrayList<>();
    for (int h = 0; h < workflowMeta.nrWorkflowHops(); h++) {
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop(h);
      ActionMeta from = hop.getFromAction();
      ActionMeta to = hop.getToAction();
      if (from == null || to == null) {
        continue;
      }
      Integer fi = index.get(from);
      Integer ti = index.get(to);
      if (fi == null || ti == null) {
        continue;
      }
      edges.add(new int[] {fi, ti});
    }

    boolean anchor = subset != null && !subset.isEmpty();
    Point origin = anchor ? topLeftOfActions(actions) : null;
    final Point[] computed = new Point[n];
    LayeredGraphLayout.layout(n, edges, options, (node, x, y) -> computed[node] = new Point(x, y));

    // Translate so the arranged block lands where the selection used to be.
    int dx = 0;
    int dy = 0;
    if (anchor) {
      Point computedMin = topLeft(computed);
      dx = origin.x - computedMin.x;
      dy = origin.y - computedMin.y;
    }

    // Capture the node positions before/after so notes can follow the node they're closest to.
    int[] nodeBeforeX = new int[n];
    int[] nodeBeforeY = new int[n];
    int[] nodeAfterX = new int[n];
    int[] nodeAfterY = new int[n];
    for (int i = 0; i < n; i++) {
      Point before = actions.get(i).getLocation();
      nodeBeforeX[i] = before.x;
      nodeBeforeY[i] = before.y;
      if (computed[i] != null) {
        nodeAfterX[i] = computed[i].x + dx;
        nodeAfterY[i] = computed[i].y + dy;
        actions.get(i).setLocation(nodeAfterX[i], nodeAfterY[i]);
      } else {
        nodeAfterX[i] = before.x;
        nodeAfterY[i] = before.y;
      }
    }

    if (options.isMoveNotes()) {
      double threshold = Math.max(options.getLayerSpacing(), options.getNodeSpacing());
      for (int i = 0; i < workflowMeta.nrNotes(); i++) {
        NotePadMeta note = workflowMeta.getNote(i);
        Point p = note.getLocation();
        if (p == null) {
          continue;
        }
        int nearest = LayeredGraphLayout.nearestNode(p.x, p.y, nodeBeforeX, nodeBeforeY, threshold);
        if (nearest >= 0) {
          note.setLocation(
              p.x + (nodeAfterX[nearest] - nodeBeforeX[nearest]),
              p.y + (nodeAfterY[nearest] - nodeBeforeY[nearest]));
        }
      }
    }
  }

  /** The minimum x and minimum y across the locations of the given actions (as one Point). */
  private static Point topLeftOfActions(List<ActionMeta> actions) {
    int minX = Integer.MAX_VALUE;
    int minY = Integer.MAX_VALUE;
    for (ActionMeta a : actions) {
      Point p = a.getLocation();
      minX = Math.min(minX, p.x);
      minY = Math.min(minY, p.y);
    }
    return new Point(minX, minY);
  }

  /** The minimum x and minimum y across the given points (as one Point). */
  private static Point topLeft(Point[] points) {
    int minX = Integer.MAX_VALUE;
    int minY = Integer.MAX_VALUE;
    for (Point p : points) {
      if (p != null) {
        minX = Math.min(minX, p.x);
        minY = Math.min(minY, p.y);
      }
    }
    return new Point(minX, minY);
  }
}
