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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportResult;
import org.apache.hop.core.diagram.ExportContext;
import org.apache.hop.core.diagram.IExportContext;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WorkflowSvgDiagramExporterTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testExportWorkflowToSvg() throws Exception {
    WorkflowSvgDiagramExporter exporter = new WorkflowSvgDiagramExporter();
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("test-workflow");

    assertTrue(exporter.supportsSubject(workflowMeta));
    assertEquals("SVG", exporter.getFormat().getId());
    assertEquals("svg", exporter.getFileExtension());

    DiagramExportOptions options = new DiagramExportOptions(null, "SVG");
    IExportContext context =
        new ExportContext(new Variables(), null, null, IExportContext.ExportEnvironment.TEST);

    DiagramExportResult result = exporter.export(workflowMeta, options, context);
    assertTrue(result.isSuccess());
    assertNotNull(result.getContent());
    assertTrue(result.getContent().contains("<svg"));
    assertEquals("image/svg+xml", result.getMimeType());
  }
}
