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
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.junit.jupiter.api.Test;

class WorkflowMermaidDiagramExporterTest {

  @Test
  void testExportWorkflowToMermaid() throws Exception {
    WorkflowMermaidDiagramExporter exporter = new WorkflowMermaidDiagramExporter();
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("batch-workflow");

    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    ActionMeta action1 = new ActionMeta(new ActionDummy());
    action1.setName("Process Files");
    ActionMeta action2 = new ActionMeta(new ActionDummy());
    action2.setName("Send Notification");

    workflowMeta.addAction(start);
    workflowMeta.addAction(action1);
    workflowMeta.addAction(action2);

    WorkflowHopMeta hop1 = new WorkflowHopMeta(start, action1);
    hop1.setUnconditional(true);
    workflowMeta.addWorkflowHop(hop1);

    WorkflowHopMeta hop2 = new WorkflowHopMeta(action1, action2);
    hop2.setUnconditional(false);
    hop2.setEvaluation(true);
    workflowMeta.addWorkflowHop(hop2);

    NotePadMeta note = new NotePadMeta("Workflow reminder note", 50, 50, 150, 40);
    workflowMeta.addNote(note);

    assertTrue(exporter.supportsSubject(workflowMeta));
    assertEquals("MERMAID", exporter.getFormat().getId());
    assertEquals("mmd", exporter.getFileExtension());

    DiagramExportOptions options = new DiagramExportOptions(null, "MERMAID");
    IExportContext context =
        new ExportContext(new Variables(), null, null, IExportContext.ExportEnvironment.TEST);

    DiagramExportResult result = exporter.export(workflowMeta, options, context);
    assertTrue(result.isSuccess());
    assertNotNull(result.getContent());

    String mmd = result.getContent();
    assertTrue(mmd.startsWith("flowchart TD"));
    assertTrue(mmd.contains("([\"Start\"])"));
    assertTrue(mmd.contains("[\"Process Files\"]"));
    assertTrue(mmd.contains("[\"Send Notification\"]"));
    assertTrue(mmd.contains(" --> "));
    assertTrue(mmd.contains(" -->|success| "));
    assertTrue(mmd.contains("Workflow reminder note"));
    assertEquals("text/vnd.mermaid", result.getMimeType());
  }
}
