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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.SvgExportCanvas;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowSvgPainter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for {@link PipelineSvgPainter} {@link WorkflowSvgPainter} svg. */
class SvgDiagramExportTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void pipelineCanvasStartsAtTheDrawing() throws Exception {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("grey-border");
    TransformMeta transform = new TransformMeta("Dummy", "Far away", new DummyMeta());
    transform.setLocation(800, 400);
    pipeline.addTransform(transform);

    Variables variables = new Variables();
    String svg = PipelineSvgPainter.generatePipelineSvg(pipeline, 1f, variables);
    String doubled = PipelineSvgPainter.generatePipelineSvg(pipeline, 2f, variables);

    int width = svgLength(svg, "width");
    int height = svgLength(svg, "height");
    int margin = SvgExportCanvas.MARGIN;

    assertTrue(width < 200, "width " + width);
    assertTrue(height < 160, "height " + height);
    assertTrue(textX(svg, "Far away") < 40, svg);

    int wide = svgLength(doubled, "width");
    int high = svgLength(doubled, "height");
    assertEquals(2 * width - 2 * margin, wide, "width " + width);
    assertEquals(2 * height - 2 * margin, high, "height " + height);
  }

  @Test
  void workflowNoteIsNotClipped() throws Exception {
    WorkflowMeta workflow = new WorkflowMeta();
    workflow.setName("note-clipped");
    NotePadMeta note =
        new NotePadMeta(
            "This note is stored only 160 pixels wide. The painter measures the whole line and"
                + " draws past the canvas. The last word must stay visible: END_MARKER",
            48,
            16,
            160,
            48);
    note.setMarkdown(false);
    note.setFontName("Arial");
    note.setFontSize(11);
    workflow.addNote(note);

    String svg = WorkflowSvgPainter.generateWorkflowSvg(workflow, 1f, new Variables());
    int width = svgLength(svg, "width");
    int x = textX(svg, "END_MARKER");
    int textLength = textLength(svg);

    assertTrue(width > 500, "width " + width);
    assertTrue(x < 40, "x " + x);
    assertTrue(x + textLength <= width - SvgExportCanvas.MARGIN + 2, "end " + (x + textLength));
  }

  private static int svgLength(String svg, String attribute) {
    Matcher matcher = Pattern.compile("\\b" + attribute + "=\"(\\d+)\"").matcher(svg);
    assertTrue(matcher.find(), attribute);
    return Integer.parseInt(matcher.group(1));
  }

  private static int textX(String svg, String text) {
    int at = svg.indexOf(text);
    assertTrue(at > 0, text);
    int xAt = svg.lastIndexOf("x=\"", at);
    assertTrue(xAt >= 0);
    int end = svg.indexOf('"', xAt + 3);
    return Math.round(Float.parseFloat(svg.substring(xAt + 3, end)));
  }

  private static int textLength(String svg) {
    int at = svg.indexOf("END_MARKER");
    assertTrue(at > 0, "END_MARKER");
    int lengthAt = svg.lastIndexOf("textLength=\"", at);
    assertTrue(lengthAt >= 0);
    int end = svg.indexOf('"', lengthAt + "textLength=\"".length());
    return Math.round(Float.parseFloat(svg.substring(lengthAt + "textLength=\"".length(), end)));
  }
}
