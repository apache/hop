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

package org.apache.hop.ui.hopgui.dialog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.diagram.DiagramExportFormat;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportService;
import org.apache.hop.core.diagram.IDiagramExporter;
import org.apache.hop.diagram.exporter.PipelineMermaidDiagramExporter;
import org.apache.hop.diagram.exporter.PipelineSvgDiagramExporter;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DiagramExportDialogModelTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
    DiagramExportService.getInstance().registerStaticExporter(new PipelineSvgDiagramExporter());
    DiagramExportService.getInstance().registerStaticExporter(new PipelineMermaidDiagramExporter());
  }

  @Test
  void testModelFormatsAndExtensionSync() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("test-pipeline");

    DiagramExportDialogModel model =
        new DiagramExportDialogModel(pipelineMeta, "/tmp/test-pipeline.svg");

    List<String> formatNames = model.getFormatNames(null, null);
    assertFalse(formatNames.isEmpty());
    assertTrue(formatNames.contains(DiagramExportFormat.SVG.getName()));
    assertTrue(formatNames.contains(DiagramExportFormat.MERMAID.getName()));

    model.setFormat("MERMAID");
    model.updateExtensionForSelectedFormat();
    assertEquals("/tmp/test-pipeline.mmd", model.getFilename());

    model.setFormat(DiagramExportFormat.MERMAID.getName());
    model.updateExtensionForSelectedFormat();
    assertEquals("/tmp/test-pipeline.mmd", model.getFilename());

    model.setMagnification("2.0");
    model.setIncludeNotes(false);

    DiagramExportOptions options = model.toOptions();
    assertEquals("/tmp/test-pipeline.mmd", options.getTargetFilename());
    assertEquals(DiagramExportFormat.MERMAID.getId(), options.getFormat());
    assertEquals(2.0f, options.getMagnification());
    assertFalse(options.isIncludeNotes());
  }

  @Test
  void testExtensionIgnoresDotsInFolderName() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("test-pipeline");

    DiagramExportDialogModel model =
        new DiagramExportDialogModel(pipelineMeta, "/tmp/my.project/test-pipeline");

    model.setFormat(DiagramExportFormat.MERMAID.getId());
    model.updateExtensionForSelectedFormat();
    assertEquals("/tmp/my.project/test-pipeline.mmd", model.getFilename());
  }

  @Test
  void testSelectedExporterMatchesIdNameAndExtension() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    DiagramExportDialogModel model =
        new DiagramExportDialogModel(pipelineMeta, "/tmp/test-pipeline.svg");

    model.setFormat("pipeline-mermaid");
    IDiagramExporter<?> byId = model.getSelectedExporter();
    assertNotNull(byId);
    assertEquals("pipeline-mermaid", byId.getId());

    model.setFormat("mmd");
    IDiagramExporter<?> byExt = model.getSelectedExporter();
    assertNotNull(byExt);
    assertEquals("pipeline-mermaid", byExt.getId());

    model.setFormat(DiagramExportFormat.SVG.getName());
    IDiagramExporter<?> byName = model.getSelectedExporter();
    assertNotNull(byName);
    assertEquals("pipeline-svg", byName.getId());
  }

  @Test
  void testDefaultConstructorDoesNotThrow() {
    DiagramExportDialogModel model = new DiagramExportDialogModel();
    assertNotNull(model.getFormatNames(null, null));
    assertNotNull(model.toOptions());
  }
}
