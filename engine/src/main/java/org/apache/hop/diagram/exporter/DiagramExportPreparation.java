/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.diagram.exporter;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramLayoutAlgorithm;
import org.apache.hop.core.diagram.DiagramLayoutPolicy;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SvgExportCanvas;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.layout.DiagramGraphLayout;
import org.apache.hop.core.layout.LayoutGraph;
import org.apache.hop.core.layout.LayoutPosition;
import org.apache.hop.core.layout.NodeBounds;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineSvgPainter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Detached drawing models shared by SVG and PNG export. Never changes the source diagram. */
public final class DiagramExportPreparation {
  private DiagramExportPreparation() {}

  /** Copies drawing state, remaps hops, and optionally lays out the entire pipeline. */
  public static PipelineMeta prepare(final PipelineMeta source, final DiagramExportOptions options)
      throws HopException {
    final var copy = new PipelineMeta();
    copy.setName(source.getName());
    final Map<TransformMeta, TransformMeta> nodes = new IdentityHashMap<>();
    for (final var original : source.getTransforms()) {
      final var node = (TransformMeta) original.clone();
      if (original.getLocation() == null) {
        node.setLocation((Point) null);
      }
      copy.addTransform(node);
      nodes.put(original, node);
    }
    for (final var original : source.getTransforms()) {
      final var node = nodes.get(original);
      if (node.getTransformErrorMeta() != null) {
        final var error = node.getTransformErrorMeta();
        error.setSourceTransform(node);
        error.setTargetTransform(nodes.get(original.getTransformErrorMeta().getTargetTransform()));
      }
    }
    for (final var original : source.getPipelineHops()) {
      final var hop = original.clone();
      hop.setFromTransform(nodes.get(original.getFromTransform()));
      hop.setToTransform(nodes.get(original.getToTransform()));
      copy.addPipelineHop(hop);
    }
    if (options == null || options.isIncludeNotes()) {
      for (final var note : source.getNotes()) {
        copy.getNotes().add(note.clone());
      }
    }
    if (shouldLayout(copy.getTransforms(), TransformMeta::isLocationExplicit, options)) {
      final var graphics = HopSvgGraphics2D.newDocument();
      try {
        final var gc = new SvgGc(graphics, new Point(1, 1), SvgExportCanvas.ICON_SIZE, 0, 0);
        layout(
            copy.getTransforms(),
            copy.getPipelineHops(),
            hop -> hop.getFromTransform(),
            hop -> hop.getToTransform(),
            node -> {
              final var bounds = new SvgExportCanvas.Bounds();
              PipelineSvgPainter.includeTransformBounds(bounds, gc, node);
              return relativeBounds(bounds, node.getLocation());
            },
            (node, position) -> node.setLocation(position.getX(), position.getY()),
            options);
      } finally {
        graphics.dispose();
      }
    }
    return copy;
  }

  /** Copies drawing state, remaps hops, and optionally lays out the entire workflow. */
  public static WorkflowMeta prepare(final WorkflowMeta source, final DiagramExportOptions options)
      throws HopException {
    final var copy = new WorkflowMeta();
    copy.setName(source.getName());
    final Map<ActionMeta, ActionMeta> nodes = new IdentityHashMap<>();
    for (final var original : source.getActions()) {
      final var node = (ActionMeta) original.cloneDeep();
      if (original.getLocation() == null) {
        node.setLocation((Point) null);
      }
      copy.addAction(node);
      nodes.put(original, node);
    }
    for (final var original : source.getWorkflowHops()) {
      final var hop = original.clone();
      hop.setFromAction(nodes.get(original.getFromAction()));
      hop.setToAction(nodes.get(original.getToAction()));
      copy.addWorkflowHop(hop);
    }
    if (options == null || options.isIncludeNotes()) {
      for (final var note : source.getNotes()) {
        copy.getNotes().add(note.clone());
      }
    }
    if (shouldLayout(copy.getActions(), ActionMeta::isLocationExplicit, options)) {
      final var graphics = HopSvgGraphics2D.newDocument();
      try {
        final var gc = new SvgGc(graphics, new Point(1, 1), SvgExportCanvas.ICON_SIZE, 0, 0);
        layout(
            copy.getActions(),
            copy.getWorkflowHops(),
            hop -> hop.getFromAction(),
            hop -> hop.getToAction(),
            node -> {
              final var bounds = new SvgExportCanvas.Bounds();
              SvgExportCanvas.includeNode(
                  bounds,
                  gc,
                  node.getLocation(),
                  node.getName(),
                  node.getAction() != null && !Utils.isEmpty(node.getDescription()),
                  SvgExportCanvas.ICON_SIZE);
              return relativeBounds(bounds, node.getLocation());
            },
            (node, position) -> node.setLocation(position.getX(), position.getY()),
            options);
      } finally {
        graphics.dispose();
      }
    }
    return copy;
  }

  private static NodeBounds relativeBounds(
      final SvgExportCanvas.Bounds bounds, final Point location) {
    final var anchor = location == null ? new Point(50, 50) : location;
    return new NodeBounds(
        bounds.getMinX() - anchor.x,
        bounds.getMinY() - anchor.y,
        bounds.getMaxX() - bounds.getMinX(),
        bounds.getMaxY() - bounds.getMinY());
  }

  private static <N> boolean shouldLayout(
      final List<N> nodes, final Predicate<N> explicit, final DiagramExportOptions options) {
    if (options == null || options.getLayoutPolicy() == null) {
      return false;
    }
    return options.getLayoutPolicy() == DiagramLayoutPolicy.ALWAYS
        || (options.getLayoutPolicy() == DiagramLayoutPolicy.ON_MISSING
            && nodes.stream().anyMatch(node -> !explicit.test(node)));
  }

  private static <N, E> void layout(
      final List<N> nodes,
      final List<E> edges,
      final Function<E, N> source,
      final Function<E, N> target,
      final Function<N, NodeBounds> bounds,
      final BiConsumer<N, LayoutPosition> apply,
      final DiagramExportOptions options) {
    final LayoutGraph<N, E> graph =
        new LayoutGraph<>() {
          @Override
          public List<N> nodes() {
            return nodes;
          }

          @Override
          public List<E> edges() {
            return edges;
          }

          @Override
          public N source(final E edge) {
            return source.apply(edge);
          }

          @Override
          public N target(final E edge) {
            return target.apply(edge);
          }
        };
    final var positions =
        DiagramGraphLayout.layout(
            graph,
            bounds,
            options.getLayoutAlgorithm() == null
                ? DiagramLayoutAlgorithm.LAYERED
                : options.getLayoutAlgorithm());
    positions.forEach(apply);
  }
}
