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
package org.apache.hop.pipeline.transforms.filterrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramLayoutAlgorithm;
import org.apache.hop.core.diagram.DiagramLayoutPolicy;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.diagram.exporter.DiagramExportPreparation;
import org.apache.hop.diagram.exporter.PipelineSvgDiagramExporter;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineSvgPainter;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class FilterRowsDiagramExportTest {
  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void defaultSvgMatchesDirectPainterForConditionalAndErrorHops() throws Exception {
    final var source = pipeline();
    final var xml = source.getXml(new Variables());
    final var expected = PipelineSvgPainter.generatePipelineSvg(source, 1.0f, null);
    final var exporter = new PipelineSvgDiagramExporter();

    assertEquals(expected, exporter.export(source, null, null).getContent());
    final var options = new DiagramExportOptions();
    assertSame(source, DiagramExportPreparation.prepare(source, options));
    assertEquals(expected, exporter.export(source, options, null).getContent());

    options.setLayoutPolicy(DiagramLayoutPolicy.ON_MISSING);
    assertSame(source, DiagramExportPreparation.prepare(source, options));
    assertEquals(expected, exporter.export(source, options, null).getContent());
    assertSourceUnchanged(source, xml);
  }

  @Test
  void alwaysLayoutResolvesStreamsOnDetachedCopiesForBothAlgorithms() throws Exception {
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      final var source = pipeline();
      final var xml = source.getXml(new Variables());
      final var options = new DiagramExportOptions();
      options.setLayoutPolicy(DiagramLayoutPolicy.ALWAYS);
      options.setLayoutAlgorithm(algorithm);
      final var prepared = DiagramExportPreparation.prepare(source, options);

      assertDetachedStreams(source, prepared);
      assertNotEquals(source.getTransform(0).getLocation(), prepared.getTransform(0).getLocation());
      assertNotSame(source.getNotes().getFirst(), prepared.getNotes().getFirst());
      assertPreparedSvgMatchesExporter(source, prepared, options);
      assertSourceUnchanged(source, xml);
    }
  }

  @Test
  void onMissingLayoutsWholeGraphWithoutChangingSourceStreams() throws Exception {
    final var source = pipeline();
    source.getTransform(2).setLocation((Point) null);
    source.clearChanged();
    final var xml = source.getXml(new Variables());
    final var options = new DiagramExportOptions();
    options.setLayoutPolicy(DiagramLayoutPolicy.ON_MISSING);
    final var prepared = DiagramExportPreparation.prepare(source, options);

    assertDetachedStreams(source, prepared);
    assertTrue(prepared.getTransforms().stream().allMatch(TransformMeta::isLocationExplicit));
    assertNotEquals(source.getTransform(0).getLocation(), prepared.getTransform(0).getLocation());
    assertPreparedSvgMatchesExporter(source, prepared, options);
    assertSourceUnchanged(source, xml);
    assertFalse(source.getTransform(2).isLocationExplicit());
  }

  @Test
  void excludingNotesWithoutLayoutStillResolvesDetachedStreams() throws Exception {
    final var source = pipeline();
    final var xml = source.getXml(new Variables());
    final var options = new DiagramExportOptions();
    options.setLayoutPolicy(DiagramLayoutPolicy.NONE);
    options.setIncludeNotes(false);
    final var prepared = DiagramExportPreparation.prepare(source, options);

    assertDetachedStreams(source, prepared);
    assertTrue(prepared.getNotes().isEmpty());
    assertEquals(1, source.getNotes().size());
    for (int i = 0; i < source.nrTransforms(); i++) {
      assertEquals(source.getTransform(i).getLocation(), prepared.getTransform(i).getLocation());
    }
    assertPreparedSvgMatchesExporter(source, prepared, options);
    assertSourceUnchanged(source, xml);
  }

  private static PipelineMeta pipeline() {
    final var source = new PipelineMeta();
    source.setName("Filter rows stream export");
    final var filter = new FilterRowsMeta();
    filter.setDefault();
    filter.setTrueTransformName("Accepted");
    filter.setFalseTransformName("Rejected");
    final var filterTransform = new TransformMeta("FilterRows", "Filter", filter);
    final var accepted = new TransformMeta("Dummy", "Accepted", new ErrorHandlingDummyMeta());
    final var rejected = new TransformMeta("Dummy", "Rejected", new DummyMeta());
    final var errors = new TransformMeta("Dummy", "Errors", new DummyMeta());
    filterTransform.setLocation(200, 200);
    accepted.setLocation(400, 100);
    rejected.setLocation(400, 300);
    errors.setLocation(600, 100);
    source.addTransform(filterTransform);
    source.addTransform(accepted);
    source.addTransform(rejected);
    source.addTransform(errors);
    filter.searchInfoAndTargetTransforms(source.getTransforms());
    source.addPipelineHop(new PipelineHopMeta(filterTransform, accepted));
    source.addPipelineHop(new PipelineHopMeta(filterTransform, rejected));
    source.addPipelineHop(new PipelineHopMeta(accepted, errors));
    final var error = new TransformErrorMeta(accepted, errors);
    error.setEnabled(true);
    accepted.setTransformErrorMeta(error);
    source.addNote(new NotePadMeta("Keep this note", 100, 400, 160, 40));
    source.clearChanged();
    return source;
  }

  private static void assertDetachedStreams(
      final PipelineMeta source, final PipelineMeta prepared) {
    assertNotSame(source, prepared);
    for (int i = 0; i < source.nrTransforms(); i++) {
      assertNotSame(source.getTransform(i), prepared.getTransform(i));
      assertNotSame(source.getTransform(i).getTransform(), prepared.getTransform(i).getTransform());
      assertSame(prepared, prepared.getTransform(i).getParentPipelineMeta());
    }
    final var streams =
        prepared.getTransform(0).getTransform().getTransformIOMeta().getTargetStreams();
    final var originalStreams =
        source.getTransform(0).getTransform().getTransformIOMeta().getTargetStreams();
    assertEquals(2, streams.size());
    assertEquals(StreamIcon.TRUE, streams.get(0).getStreamIcon());
    assertEquals(StreamIcon.FALSE, streams.get(1).getStreamIcon());
    for (int i = 0; i < streams.size(); i++) {
      assertNotSame(originalStreams.get(i), streams.get(i));
      assertSame(prepared.getTransform(i + 1), streams.get(i).getTransformMeta());
      assertSame(source.getTransform(i + 1), originalStreams.get(i).getTransformMeta());
    }
    for (int i = 0; i < source.nrPipelineHops(); i++) {
      final var original = source.getPipelineHop(i);
      final var hop = prepared.getPipelineHop(i);
      assertNotSame(original, hop);
      assertSame(
          prepared.findTransform(original.getFromTransform().getName()), hop.getFromTransform());
      assertSame(prepared.findTransform(original.getToTransform().getName()), hop.getToTransform());
    }
    final var error = prepared.getTransform(1).getTransformErrorMeta();
    assertNotSame(source.getTransform(1).getTransformErrorMeta(), error);
    assertSame(prepared.getTransform(1), error.getSourceTransform());
    assertSame(prepared.getTransform(3), error.getTargetTransform());
    assertTrue(prepared.getTransform(1).isSendingErrorRowsToTransform(prepared.getTransform(3)));
  }

  private static void assertSourceUnchanged(final PipelineMeta source, final String xml)
      throws Exception {
    assertEquals(xml, source.getXml(new Variables()));
    assertFalse(source.hasChanged());
    final var streams =
        source.getTransform(0).getTransform().getTransformIOMeta().getTargetStreams();
    assertSame(source.getTransform(1), streams.get(0).getTransformMeta());
    assertSame(source.getTransform(2), streams.get(1).getTransformMeta());
    assertSame(
        source.getTransform(1),
        source.getTransform(1).getTransformErrorMeta().getSourceTransform());
    assertSame(
        source.getTransform(3),
        source.getTransform(1).getTransformErrorMeta().getTargetTransform());
    assertTrue(source.getTransform(1).isSendingErrorRowsToTransform(source.getTransform(3)));
  }

  private static void assertPreparedSvgMatchesExporter(
      final PipelineMeta source, final PipelineMeta prepared, final DiagramExportOptions options)
      throws Exception {
    assertEquals(
        PipelineSvgPainter.generatePipelineSvg(prepared, options.getMagnification(), null),
        new PipelineSvgDiagramExporter().export(source, options, null).getContent());
  }

  private static class ErrorHandlingDummyMeta extends DummyMeta {
    @Override
    public boolean supportsErrorHandling() {
      return true;
    }
  }
}
