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

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportResult;
import org.apache.hop.core.diagram.ExportContext;
import org.apache.hop.core.diagram.IExportContext;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.Test;

class PipelineMermaidDiagramExporterTest {

  @Test
  void testExportPipelineToMermaid() throws Exception {
    PipelineMermaidDiagramExporter exporter = new PipelineMermaidDiagramExporter();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("order-pipeline");

    TransformMeta t1 = new TransformMeta("Source", new DummyMeta());
    TransformMeta t2 = new TransformMeta("Process Orders", new DummyMeta());
    TransformMeta t3 = new TransformMeta("Output", new DummyMeta());

    pipelineMeta.addTransform(t1);
    pipelineMeta.addTransform(t2);
    pipelineMeta.addTransform(t3);

    PipelineHopMeta hop1 = new PipelineHopMeta(t1, t2);
    PipelineHopMeta hop2 = new PipelineHopMeta(t2, t3);
    pipelineMeta.addPipelineHop(hop1);
    pipelineMeta.addPipelineHop(hop2);

    NotePadMeta note = new NotePadMeta("Important pipeline note", 100, 100, 200, 50);
    pipelineMeta.addNote(note);

    assertTrue(exporter.supportsSubject(pipelineMeta));
    assertEquals("MERMAID", exporter.getFormat().getId());
    assertEquals("mmd", exporter.getFileExtension());

    DiagramExportOptions options = new DiagramExportOptions(null, "MERMAID");
    IExportContext context =
        new ExportContext(new Variables(), null, null, IExportContext.ExportEnvironment.TEST);

    DiagramExportResult result = exporter.export(pipelineMeta, options, context);
    assertTrue(result.isSuccess());
    assertNotNull(result.getContent());

    String mmd = result.getContent();
    assertTrue(mmd.startsWith("flowchart LR"));
    assertTrue(mmd.contains("[\"Source\"]"));
    assertTrue(mmd.contains("[\"Process Orders\"]"));
    assertTrue(mmd.contains("[\"Output\"]"));
    assertTrue(mmd.contains(" --> "));
    assertTrue(mmd.contains("Important pipeline note"));
    assertEquals("text/vnd.mermaid", result.getMimeType());
  }
}
