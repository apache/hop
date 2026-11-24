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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramLayoutAlgorithm;
import org.apache.hop.core.diagram.DiagramLayoutPolicy;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DiagramExportPreparationTest {
  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void noLayoutPreservesNullLocationsForPainterFallback() throws Exception {
    final var pipeline = pipeline();
    pipeline.getTransform(0).setLocation((Point) null);
    final var workflow = new WorkflowMeta();
    final var action = new ActionMeta(new ActionDummy());
    action.setName("action");
    action.setLocation((Point) null);
    workflow.addAction(action);
    final var options = new DiagramExportOptions();
    assertNull(DiagramExportPreparation.prepare(pipeline, options).getTransform(0).getLocation());
    assertNull(DiagramExportPreparation.prepare(workflow, options).getAction(0).getLocation());
    assertNull(pipeline.getTransform(0).getLocation());
    assertNull(action.getLocation());
  }

  @Test
  void pipelineCopiesAreDetachedAndDeterministicForBothAlgorithms() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      final var pipeline = pipeline();
      pipeline.clearChanged();
      final var originalXml = pipeline.getXml(new Variables());
      final var options = options(algorithm);
      final var first = DiagramExportPreparation.prepare(pipeline, options);
      final var second = DiagramExportPreparation.prepare(pipeline, options);
      for (int i = 0; i < pipeline.nrTransforms(); i++) {
        assertNotSame(pipeline.getTransform(i), first.getTransform(i));
        assertNotSame(pipeline.getTransform(i).getLocation(), first.getTransform(i).getLocation());
        assertEquals(first.getTransform(i).getLocation(), second.getTransform(i).getLocation());
        assertSame(first, first.getTransform(i).getParentPipelineMeta());
      }
      assertNotEquals(first.getTransform(0).getLocation(), first.getTransform(1).getLocation());
      assertSame(first.getTransform(0), first.getPipelineHop(0).getFromTransform());
      assertSame(first.getTransform(1), first.getPipelineHop(0).getToTransform());
      assertFalse(first.getPipelineHop(0).isEnabled());
      assertSame(
          first.getTransform(1),
          first.getTransform(0).getTransformErrorMeta().getTargetTransform());
      assertNotSame(pipeline.getNotes().getFirst(), first.getNotes().getFirst());
      assertNotSame(
          pipeline.getNotes().getFirst().getLocation(), first.getNotes().getFirst().getLocation());
      assertEquals(originalXml, pipeline.getXml(new Variables()));
      assertFalse(pipeline.hasChanged());
      assertTrue(pipeline.getTransform(0).isLocationExplicit());
      assertFalse(pipeline.getTransform(1).isLocationExplicit());
      assertSame(pipeline.getTransform(0), pipeline.getPipelineHop(0).getFromTransform());
      assertSame(pipeline.getTransform(1), pipeline.getPipelineHop(0).getToTransform());
      assertSame(
          pipeline.getTransform(0),
          pipeline.getTransform(0).getTransformErrorMeta().getSourceTransform());
      assertSame(
          pipeline.getTransform(1),
          pipeline.getTransform(0).getTransformErrorMeta().getTargetTransform());
    }
  }

  @Test
  void onMissingPreservesExplicitZeroButLayoutsWholeGraphForMissingPosition() throws Exception {
    final var pipeline = pipeline();
    final var options = options(DiagramLayoutAlgorithm.LAYERED);
    options.setLayoutPolicy(DiagramLayoutPolicy.ON_MISSING);
    pipeline.getTransform(0).setLocation(0, 0);
    pipeline.getTransform(1).setLocation(0, 0);
    final var positioned = DiagramExportPreparation.prepare(pipeline, options);
    assertEquals(new Point(0, 0), positioned.getTransform(0).getLocation());
    assertEquals(new Point(0, 0), positioned.getTransform(1).getLocation());
    pipeline.getTransform(1).setLocation((Point) null);
    final var arranged = DiagramExportPreparation.prepare(pipeline, options);
    assertNotEquals(new Point(0, 0), arranged.getTransform(0).getLocation());
    assertNotEquals(arranged.getTransform(0).getLocation(), arranged.getTransform(1).getLocation());
    options.setLayoutPolicy(DiagramLayoutPolicy.NONE);
    assertEquals(
        new Point(0, 0),
        DiagramExportPreparation.prepare(pipeline, options).getTransform(0).getLocation());
  }

  @Test
  void notesCanBeExcludedWithoutRemovingSourceNotes() throws Exception {
    final var pipeline = pipeline();
    final var options = new DiagramExportOptions();
    options.setIncludeNotes(false);
    assertTrue(DiagramExportPreparation.prepare(pipeline, options).getNotes().isEmpty());
    assertEquals(1, pipeline.getNotes().size());
  }

  @Test
  void workflowCopiesRemapConditionalHopsAndRetainSourceState() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      final var workflow = new WorkflowMeta();
      final var a = new ActionMeta(new ActionDummy());
      a.setName("a");
      final var b = new ActionMeta(new ActionDummy());
      b.setName("b with a long label");
      a.setLocation(0, 0);
      workflow.addAction(a);
      workflow.addAction(b);
      final var hop = new WorkflowHopMeta(a, b);
      hop.setUnconditional(false);
      hop.setEvaluation(false);
      workflow.addWorkflowHop(hop);
      workflow.addNote(new NotePadMeta("note", 100, 100, 80, 40));
      workflow.clearChanged();
      final var xml = workflow.getXml(new Variables());
      final var copy = DiagramExportPreparation.prepare(workflow, options(algorithm));
      assertNotSame(a, copy.getAction(0));
      assertNotSame(a.getAction(), copy.getAction(0).getAction());
      assertNotSame(a.getLocation(), copy.getAction(0).getLocation());
      assertNotSame(workflow.getNotes().getFirst(), copy.getNotes().getFirst());
      assertNotSame(
          workflow.getNotes().getFirst().getLocation(), copy.getNotes().getFirst().getLocation());
      assertSame(copy.getAction(0), copy.getWorkflowHop(0).getFromAction());
      assertSame(copy.getAction(1), copy.getWorkflowHop(0).getToAction());
      assertFalse(copy.getWorkflowHop(0).getEvaluation());
      assertNotEquals(copy.getAction(0).getLocation(), copy.getAction(1).getLocation());
      assertEquals(xml, workflow.getXml(new Variables()));
      assertFalse(workflow.hasChanged());
      assertTrue(a.isLocationExplicit());
      assertFalse(b.isLocationExplicit());
      assertSame(a, workflow.getWorkflowHop(0).getFromAction());
      assertSame(b, workflow.getWorkflowHop(0).getToAction());
      final var exportOptions = options(algorithm);
      exportOptions.setMagnification(1.5f);
      final var svg =
          new WorkflowSvgDiagramExporter().export(workflow, exportOptions, null).getContent();
      assertEquals(
          svg, new WorkflowSvgDiagramExporter().export(workflow, exportOptions, null).getContent());
      assertSvgDimensions(svg);
      assertEquals(xml, workflow.getXml(new Variables()));
      assertFalse(workflow.hasChanged());
      assertTrue(a.isLocationExplicit());
      assertFalse(b.isLocationExplicit());
    }
  }

  @Test
  void svgIsDeterministicAndLeavesSourceUntouched() throws Exception {
    final var pipeline = pipeline();
    pipeline.clearChanged();
    final var xml = pipeline.getXml(new Variables());
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      final var options = options(algorithm);
      options.setMagnification(1.5f);
      final var svg = new PipelineSvgDiagramExporter().export(pipeline, options, null).getContent();
      assertEquals(
          svg, new PipelineSvgDiagramExporter().export(pipeline, options, null).getContent());
      assertSvgDimensions(svg);
      assertEquals(xml, pipeline.getXml(new Variables()));
      assertFalse(pipeline.hasChanged());
      assertTrue(pipeline.getTransform(0).isLocationExplicit());
      assertFalse(pipeline.getTransform(1).isLocationExplicit());
    }
  }

  private static PipelineMeta pipeline() {
    final var pipeline = new PipelineMeta();
    pipeline.setName("layout");
    final var a = new TransformMeta("Dummy", "Source", new DummyMeta());
    final var b =
        new TransformMeta("Dummy", "Sink with a very long label to measure", new DummyMeta());
    a.setLocation(0, 0);
    pipeline.addTransform(a);
    pipeline.addTransform(b);
    final var hop = new PipelineHopMeta(a, b);
    hop.setEnabled(false);
    pipeline.addPipelineHop(hop);
    final var error = new TransformErrorMeta(a, b);
    error.setEnabled(true);
    a.setTransformErrorMeta(error);
    pipeline.addNote(new NotePadMeta("note", 400, 400, 80, 40));
    return pipeline;
  }

  private static DiagramExportOptions options(final DiagramLayoutAlgorithm algorithm) {
    final var options = new DiagramExportOptions();
    options.setLayoutPolicy(DiagramLayoutPolicy.ALWAYS);
    options.setLayoutAlgorithm(algorithm);
    return options;
  }

  private static void assertSvgDimensions(final String svg) {
    assertNotNull(svg);
    // Match root attributes only; child elements can also have width and height attributes.
    final var width = Pattern.compile("<svg\\b[^>]*\\swidth=\"(\\d+)\"").matcher(svg);
    final var height = Pattern.compile("<svg\\b[^>]*\\sheight=\"(\\d+)\"").matcher(svg);
    assertTrue(width.find());
    assertTrue(height.find());
    assertTrue(Integer.parseInt(width.group(1)) > 0);
    assertTrue(Integer.parseInt(height.group(1)) > 0);
  }
}
