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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramLayoutAlgorithm;
import org.apache.hop.core.diagram.DiagramLayoutPolicy;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SvgExportCanvas;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.layout.DiagramGraphLayout;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineSvgPainter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowSvgPainter;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DiagramExportNotesTest {
  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void largeNotesAndCompleteNodeFootprintsDoNotOverlap() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      for (final var changed : List.of(false, true)) {
        for (final var source : diagrams(true)) {
          source.setChanged(changed);
          source.getNotes().getFirst().setChanged(changed);
          final var before = snapshot(source);
          final var prepared = prepare(source, options(algorithm));
          assertNotSame(source, prepared);
          assertDetachedNotes(source, prepared);
          final var bounds = footprints(prepared);
          assertNoOverlap(bounds);
          if (algorithm == DiagramLayoutAlgorithm.GRID) {
            // Executable nodes precede notes, all using the same grid cell dimensions.
            assertGrid(bounds);
          } else {
            final int nodeCount = locations(prepared).size();
            final int graphTop =
                bounds.subList(0, nodeCount).stream()
                    .mapToInt(SvgExportCanvas.Bounds::getMinY)
                    .min()
                    .orElseThrow();
            final var noteBounds = bounds.subList(nodeCount, bounds.size());
            for (final var note : noteBounds) {
              assertTrue(note.getMaxY() + DiagramGraphLayout.DEFAULT_GAP <= graphTop);
            }
            assertGrid(noteBounds);
            final var withoutNotes = options(algorithm);
            withoutNotes.setIncludeNotes(false);
            final var graphOnly = locations(prepare(source, withoutNotes));
            final var graphWithNotes = locations(prepared);
            final int offset = graphWithNotes.getFirst().y - graphOnly.getFirst().y;
            assertTrue(offset > 0);
            for (int i = 0; i < graphOnly.size(); i++) {
              assertEquals(graphOnly.get(i).x, graphWithNotes.get(i).x);
              assertEquals(graphOnly.get(i).y + offset, graphWithNotes.get(i).y);
            }
          }
          assertEquals(before, snapshot(source));
          assertEquals(changed, source.hasChanged());
          assertEquals(changed, source.getNotes().getFirst().hasChanged());
        }
      }
    }
  }

  @Test
  void layoutIsDeterministicAndIndependentOfOriginalNoteLocations() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      for (final var source : diagrams(true)) {
        final var options = options(algorithm);
        final var before = snapshot(source);
        final var first = prepare(source, options);
        final var second = prepare(source, options);
        assertCoordinatesEqual(first, second);
        assertEquals(before, snapshot(source));
        for (int i = 0; i < source.getNotes().size(); i++) {
          source.getNotes().get(i).setLocation(900_000 - i * 800_000, -700_000 + i * 900_000);
        }
        source.clearChanged();
        final var relocated = snapshot(source);
        assertCoordinatesEqual(first, prepare(source, options));
        assertEquals(relocated, snapshot(source));
        assertFalse(source.hasChanged());
      }
    }
  }

  @Test
  void excludedNotesDoNotInfluenceExecutableCoordinates() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      for (final var source : diagrams(true)) {
        final var before = snapshot(source);
        final var options = options(algorithm);
        options.setIncludeNotes(false);
        final var excluded = prepare(source, options);
        assertTrue(excluded.getNotes().isEmpty());
        assertEquals(before, snapshot(source));
        // A genuinely note-free model must produce exactly the same executable coordinates.
        final var noteFree = prepare(source, options);
        assertNotSame(source.getNotes(), noteFree.getNotes());
        options.setIncludeNotes(true);
        final var baseline = prepare(noteFree, options);
        assertEquals(locations(baseline), locations(excluded));
        source.getNotes().getFirst().setWidth(20_000);
        source.getNotes().getFirst().setHeight(30_000);
        source.clearChanged();
        final var enlarged = snapshot(source);
        options.setIncludeNotes(false);
        assertEquals(locations(baseline), locations(prepare(source, options)));
        assertEquals(enlarged, snapshot(source));
        assertEquals(3, source.getNotes().size());
      }
    }
  }

  @Test
  void noteOnlyAndEmptyDiagramsAreSupported() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      for (final var source : diagrams(false)) {
        final var before = snapshot(source);
        final var options = options(algorithm);
        final var prepared = prepare(source, options);
        assertTrue(locations(prepared).isEmpty());
        assertDetachedNotes(source, prepared);
        final var bounds = footprints(prepared);
        assertEquals(3, bounds.size());
        assertNoOverlap(bounds);
        assertGrid(bounds);
        assertCoordinatesEqual(prepared, prepare(source, options));
        assertEquals(before, snapshot(source));
        options.setIncludeNotes(false);
        final var empty = prepare(source, options);
        assertTrue(empty.getNotes().isEmpty());
        assertTrue(locations(empty).isEmpty());
        options.setIncludeNotes(true);
        final var emptyBefore = snapshot(empty);
        final var emptyPrepared = prepare(empty, options);
        assertTrue(footprints(emptyPrepared).isEmpty());
        assertEquals(emptyBefore, snapshot(empty));
        assertEquals(before, snapshot(source));
      }
    }
  }

  @Test
  void onMissingIgnoresNoteCoordinatesWhenExecutableLocationsAreExplicit() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      for (final boolean withNodes : List.of(false, true)) {
        for (final var source : diagrams(withNodes)) {
          source.getNotes().getFirst().setLocation(-1, -1);
          source.getNotes().get(1).setLocation(0, 0);
          source.clearChanged();
          final var before = snapshot(source);
          final var options = options(algorithm);
          options.setLayoutPolicy(DiagramLayoutPolicy.ON_MISSING);
          assertSame(source, prepare(source, options));
          options.setIncludeNotes(false);
          final var excluded = prepare(source, options);
          assertTrue(excluded.getNotes().isEmpty());
          assertEquals(locations(source), locations(excluded));
          assertEquals(before, snapshot(source));
          assertFalse(source.hasChanged());
        }
      }
    }
  }

  private static List<AbstractMeta> diagrams(final boolean withNodes) {
    final var pipeline = new PipelineMeta();
    pipeline.setName("pipeline notes");
    final var workflow = new WorkflowMeta();
    workflow.setName("workflow notes");
    if (withNodes) {
      for (int i = 0; i < 2; i++) {
        final var name = "Executable " + i + " with a very long label extending beyond its icon";
        final var transform = new TransformMeta("Dummy", name, new DummyMeta());
        transform.setDescription("Description badge outside the icon");
        transform.setCopiesString("12");
        transform.setLocation(200 + i * 400, 300 + i * 500);
        pipeline.addTransform(transform);
        final var action = new ActionMeta(new ActionDummy());
        action.setName(name);
        action.setDescription("Description badge outside the icon");
        action.setLocation(200 + i * 400, 300 + i * 500);
        workflow.addAction(action);
      }
      pipeline.addPipelineHop(
          new PipelineHopMeta(pipeline.getTransform(0), pipeline.getTransform(1)));
      workflow.addWorkflowHop(new WorkflowHopMeta(workflow.getAction(0), workflow.getAction(1)));
    }
    final List<AbstractMeta> diagrams = List.of(pipeline, workflow);
    for (final var diagram : diagrams) {
      final var plain =
          new NotePadMeta(
              "Plain text deliberately much wider than the tiny stored note dimensions. ".repeat(5)
                  + "\nSecond line\nThird line\nFourth line",
              -800_000,
              -600_000,
              12,
              8);
      plain.setMarkdown(false);
      plain.setFontSize(18);
      final var markdown =
          new NotePadMeta(
              "# Markdown annotation\n\n**Large note** with wrapped explanatory text.\n\n"
                  + "- First item\n- Second item\n\n"
                  + "A paragraph that should wrap within the note. ".repeat(20),
              700_000,
              900_000,
              420,
              20);
      markdown.setMarkdown(true);
      final var large = new NotePadMeta("Another large note", -500_000, 800_000, 950, 650);
      large.setMarkdown(false);
      diagram.addNote(plain);
      diagram.addNote(markdown);
      diagram.addNote(large);
      diagram.clearChanged();
    }
    return diagrams;
  }

  private static DiagramExportOptions options(final DiagramLayoutAlgorithm algorithm) {
    final var options = new DiagramExportOptions();
    options.setLayoutPolicy(DiagramLayoutPolicy.ALWAYS);
    options.setLayoutAlgorithm(algorithm);
    options.setIncludeNotes(true);
    return options;
  }

  private static AbstractMeta prepare(final AbstractMeta source, final DiagramExportOptions options)
      throws Exception {
    if (source instanceof PipelineMeta pipeline) {
      return DiagramExportPreparation.prepare(pipeline, options, new Variables());
    }
    return DiagramExportPreparation.prepare((WorkflowMeta) source, options, new Variables());
  }

  private static List<Point> locations(final AbstractMeta source) {
    if (source instanceof PipelineMeta pipeline) {
      return pipeline.getTransforms().stream().map(TransformMeta::getLocation).toList();
    }
    return ((WorkflowMeta) source).getActions().stream().map(ActionMeta::getLocation).toList();
  }

  private static void assertCoordinatesEqual(final AbstractMeta first, final AbstractMeta second) {
    assertEquals(locations(first), locations(second));
    assertEquals(
        first.getNotes().stream().map(NotePadMeta::getLocation).toList(),
        second.getNotes().stream().map(NotePadMeta::getLocation).toList());
  }

  private static void assertDetachedNotes(final AbstractMeta source, final AbstractMeta prepared) {
    assertEquals(source.getNotes().size(), prepared.getNotes().size());
    for (int i = 0; i < source.getNotes().size(); i++) {
      final var original = source.getNotes().get(i);
      final var copy = prepared.getNotes().get(i);
      assertNotSame(original, copy);
      assertNotSame(original.getLocation(), copy.getLocation());
      assertEquals(original.getNote(), copy.getNote());
      assertEquals(original.isMarkdown(), copy.isMarkdown());
      assertEquals(original.getWidth(), copy.getWidth());
      assertEquals(original.getHeight(), copy.getHeight());
    }
  }

  private static List<SvgExportCanvas.Bounds> footprints(final AbstractMeta diagram)
      throws Exception {
    final var graphics = HopSvgGraphics2D.newDocument();
    try {
      final var gc = new SvgGc(graphics, new Point(1, 1), SvgExportCanvas.ICON_SIZE, 0, 0);
      final var result = new ArrayList<SvgExportCanvas.Bounds>();
      final List<Point> sizes;
      if (diagram instanceof PipelineMeta pipeline) {
        for (final var transform : pipeline.getTransforms()) {
          final var bounds = new SvgExportCanvas.Bounds();
          PipelineSvgPainter.includeTransformBounds(bounds, gc, transform);
          result.add(bounds);
        }
        sizes = PipelineSvgPainter.getNoteDrawnSizes(pipeline, gc, new Variables());
      } else {
        final var workflow = (WorkflowMeta) diagram;
        for (final var action : workflow.getActions()) {
          final var bounds = new SvgExportCanvas.Bounds();
          SvgExportCanvas.includeNode(
              bounds, gc, action.getLocation(), action.getName(), true, SvgExportCanvas.ICON_SIZE);
          result.add(bounds);
        }
        sizes = WorkflowSvgPainter.getNoteDrawnSizes(workflow, gc, new Variables());
      }
      assertEquals(diagram.getNotes().size(), sizes.size());
      if (!sizes.isEmpty()) {
        // Guard the fixture: using stored dimensions instead of painter sizes must be detectable.
        assertTrue(sizes.getFirst().x > diagram.getNotes().getFirst().getWidth());
        assertTrue(sizes.getFirst().y > diagram.getNotes().getFirst().getHeight());
        assertTrue(sizes.get(1).y > diagram.getNotes().get(1).getHeight());
      }
      for (int i = 0; i < sizes.size(); i++) {
        final var bounds = new SvgExportCanvas.Bounds();
        final var location = diagram.getNotes().get(i).getLocation();
        final var size = sizes.get(i);
        SvgExportCanvas.includeNote(bounds, location, size);
        assertEquals(location.x - 1, bounds.getMinX());
        assertEquals(location.y - 1, bounds.getMinY());
        assertEquals(location.x + size.x + 1, bounds.getMaxX());
        assertEquals(location.y + size.y + 1, bounds.getMaxY());
        result.add(bounds);
      }
      return result;
    } finally {
      graphics.dispose();
    }
  }

  private static void assertNoOverlap(final List<SvgExportCanvas.Bounds> bounds) {
    for (int i = 0; i < bounds.size(); i++) {
      final var a = bounds.get(i);
      for (int j = i + 1; j < bounds.size(); j++) {
        final var b = bounds.get(j);
        assertTrue(
            a.getMaxX() <= b.getMinX()
                || b.getMaxX() <= a.getMinX()
                || a.getMaxY() <= b.getMinY()
                || b.getMaxY() <= a.getMinY(),
            "Painted footprints overlap: " + i + " and " + j);
      }
    }
  }

  private static void assertGrid(final List<SvgExportCanvas.Bounds> bounds) {
    final int columns = (int) Math.ceil(Math.sqrt(bounds.size()));
    final int width = bounds.stream().mapToInt(b -> b.getMaxX() - b.getMinX()).max().orElseThrow();
    final int height = bounds.stream().mapToInt(b -> b.getMaxY() - b.getMinY()).max().orElseThrow();
    for (int i = 0; i < bounds.size(); i++) {
      assertEquals(
          DiagramGraphLayout.DEFAULT_MARGIN
              + (i % columns) * (width + DiagramGraphLayout.DEFAULT_GAP),
          bounds.get(i).getMinX());
      assertEquals(
          DiagramGraphLayout.DEFAULT_MARGIN
              + (i / columns) * (height + DiagramGraphLayout.DEFAULT_GAP),
          bounds.get(i).getMinY());
    }
  }

  private static Snapshot snapshot(final AbstractMeta source) throws Exception {
    final String xml;
    final List<Boolean> explicit;
    final List<Boolean> nodeChanged;
    if (source instanceof PipelineMeta pipeline) {
      xml = pipeline.getXml(new Variables());
      explicit = pipeline.getTransforms().stream().map(TransformMeta::isLocationExplicit).toList();
      nodeChanged = pipeline.getTransforms().stream().map(TransformMeta::hasChanged).toList();
    } else {
      final var workflow = (WorkflowMeta) source;
      xml = workflow.getXml(new Variables());
      explicit = workflow.getActions().stream().map(ActionMeta::isLocationExplicit).toList();
      nodeChanged = workflow.getActions().stream().map(ActionMeta::hasChanged).toList();
    }
    return new Snapshot(
        xml,
        source.hasChanged(),
        source.getNotes().stream().map(NotePadMeta::hasChanged).toList(),
        explicit,
        nodeChanged);
  }

  private record Snapshot(
      String xml,
      boolean changed,
      List<Boolean> noteChanged,
      List<Boolean> explicit,
      List<Boolean> nodeChanged) {}
}
