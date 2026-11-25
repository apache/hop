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

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hop.core.NotePadMeta;
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
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineSvgPainter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowSvgPainter;
import org.apache.hop.workflow.action.ActionMeta;

/** Prepares drawing models for SVG export without changing the source diagram. */
public final class DiagramExportPreparation {
  private DiagramExportPreparation() {}

  /** Returns the source when no changes are needed, otherwise prepares a detached drawing model. */
  public static PipelineMeta prepare(final PipelineMeta source, final DiagramExportOptions options)
      throws HopException {
    return prepare(source, options, null);
  }

  /** Prepares a pipeline using the same variables as the SVG painter for note measurement. */
  public static PipelineMeta prepare(
      final PipelineMeta source, final DiagramExportOptions options, final IVariables variables)
      throws HopException {
    final boolean arrange =
        shouldLayout(source.getTransforms(), TransformMeta::isLocationExplicit, options);
    if (!arrange && (options == null || options.isIncludeNotes())) {
      return source;
    }
    final var copy = new PipelineMeta();
    copy.setName(source.getName());
    copy.setFilename(source.getFilename());
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
    for (final var node : copy.getTransforms()) {
      node.getTransform().searchInfoAndTargetTransforms(copy.getTransforms());
    }
    if (options == null || options.isIncludeNotes()) {
      for (final var note : source.getNotes()) {
        copy.getNotes().add(note.clone());
      }
    }
    if (arrange) {
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
            copy.getNotes(),
            PipelineSvgPainter.getNoteDrawnSizes(copy, gc, variables),
            options);
      } finally {
        graphics.dispose();
      }
    }
    return copy;
  }

  /** Returns the source when no changes are needed, otherwise prepares a detached drawing model. */
  public static WorkflowMeta prepare(final WorkflowMeta source, final DiagramExportOptions options)
      throws HopException {
    return prepare(source, options, null);
  }

  /** Prepares a workflow using the same variables as the SVG painter for note measurement. */
  public static WorkflowMeta prepare(
      final WorkflowMeta source, final DiagramExportOptions options, final IVariables variables)
      throws HopException {
    final boolean arrange =
        shouldLayout(source.getActions(), ActionMeta::isLocationExplicit, options);
    if (!arrange && (options == null || options.isIncludeNotes())) {
      return source;
    }
    final var copy = new WorkflowMeta();
    copy.setName(source.getName());
    copy.setFilename(source.getFilename());
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
    if (arrange) {
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
            copy.getNotes(),
            WorkflowSvgPainter.getNoteDrawnSizes(copy, gc, variables),
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
      final List<NotePadMeta> notes,
      final List<Point> noteSizes,
      final DiagramExportOptions options) {
    // Notes are drawing items, not executable graph vertices. GRID packs all items together;
    // LAYERED keeps the execution graph separate and packs annotations in a band above it.
    final var algorithm =
        options.getLayoutAlgorithm() == null
            ? DiagramLayoutAlgorithm.LAYERED
            : options.getLayoutAlgorithm();
    final var footprints = new IdentityHashMap<Object, NodeBounds>();
    final var items = new ArrayList<Object>(nodes);
    nodes.forEach(node -> footprints.put(node, bounds.apply(node)));
    for (int i = 0; i < notes.size(); i++) {
      final var size = noteSizes.get(i);
      footprints.put(
          notes.get(i), new NodeBounds(-1, -1, Math.addExact(size.x, 2), Math.addExact(size.y, 2)));
    }
    if (algorithm == DiagramLayoutAlgorithm.GRID) {
      items.addAll(notes);
    }
    final var positions = positions(items, edges, source, target, footprints::get, algorithm);
    int graphOffsetY = 0;
    if (algorithm == DiagramLayoutAlgorithm.LAYERED && !notes.isEmpty()) {
      final var notePositions =
          positions(
              notes,
              List.<NotePadMeta>of(),
              note -> note,
              note -> note,
              footprints::get,
              DiagramLayoutAlgorithm.GRID);
      long bottom = DiagramGraphLayout.DEFAULT_MARGIN;
      for (final var note : notes) {
        final var position = notePositions.get(note);
        final var footprint = footprints.get(note);
        bottom =
            Math.max(bottom, (long) position.getY() + footprint.getY() + footprint.getHeight());
        note.setLocation(new Point(position.getX(), position.getY()));
      }
      if (!nodes.isEmpty()) {
        long top = Long.MAX_VALUE;
        for (final var node : nodes) {
          top = Math.min(top, (long) positions.get(node).getY() + footprints.get(node).getY());
        }
        graphOffsetY = Math.toIntExact(bottom + DiagramGraphLayout.DEFAULT_GAP - top);
      }
    }
    for (final var node : nodes) {
      final var position = positions.get(node);
      apply.accept(
          node, new LayoutPosition(position.getX(), Math.addExact(position.getY(), graphOffsetY)));
    }
    if (algorithm == DiagramLayoutAlgorithm.GRID) {
      for (final var note : notes) {
        final var position = positions.get(note);
        note.setLocation(new Point(position.getX(), position.getY()));
      }
    }
  }

  private static <N, E> Map<N, LayoutPosition> positions(
      final List<N> nodes,
      final List<E> edges,
      final Function<E, ? extends N> source,
      final Function<E, ? extends N> target,
      final Function<N, NodeBounds> bounds,
      final DiagramLayoutAlgorithm algorithm) {
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
    return DiagramGraphLayout.layout(graph, bounds, algorithm);
  }
}
