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

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.diagram.BaseDiagramExporter;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportResult;
import org.apache.hop.core.diagram.DiagramExporter;
import org.apache.hop.core.diagram.IExportContext;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Diagram exporter that generates a Mermaid flowchart (flowchart TD/LR) from a Workflow. */
@DiagramExporter(
    id = "workflow-mermaid",
    name = "Workflow Mermaid Exporter",
    description = "Exports a workflow diagram to Mermaid flowchart (.mmd)",
    format = "MERMAID",
    fileExtension = "mmd",
    fileFilterNames = {"Mermaid Diagrams (*.mmd)"},
    supportedSubjectTypes = {WorkflowMeta.class})
public class WorkflowMermaidDiagramExporter extends BaseDiagramExporter<WorkflowMeta> {

  public WorkflowMermaidDiagramExporter() {
    super();
  }

  @Override
  public DiagramExportResult export(
      WorkflowMeta workflowMeta, DiagramExportOptions options, IExportContext context)
      throws HopException {
    if (workflowMeta == null) {
      throw new HopException("WorkflowMeta is null");
    }

    String direction =
        options != null && options.getDirection() != null ? options.getDirection() : "TD";

    StringBuilder sb = new StringBuilder();
    sb.append("flowchart ").append(direction).append("\n");

    Map<String, String> actionIds = new HashMap<>();
    int aIndex = 0;
    for (ActionMeta action : workflowMeta.getActions()) {
      String id = "a" + aIndex++;
      actionIds.put(action.getName(), id);
      String label = PipelineMermaidDiagramExporter.escapeMermaidLabel(action.getName());
      if (action.isStart()) {
        sb.append("  ").append(id).append("([\"").append(label).append("\"])\n");
      } else {
        sb.append("  ").append(id).append("[\"").append(label).append("\"]\n");
      }
    }

    for (WorkflowHopMeta hop : workflowMeta.getWorkflowHops()) {
      if (hop.getFrom() == null || hop.getTo() == null) {
        continue;
      }
      String fromId = actionIds.get(hop.getFrom().getName());
      String toId = actionIds.get(hop.getTo().getName());
      if (fromId == null || toId == null) {
        continue;
      }

      if (!hop.isEnabled()) {
        sb.append("  ").append(fromId).append(" -.->|disabled| ").append(toId).append("\n");
      } else if (hop.isUnconditional()) {
        sb.append("  ").append(fromId).append(" --> ").append(toId).append("\n");
      } else if (hop.getEvaluation()) {
        sb.append("  ").append(fromId).append(" -->|success| ").append(toId).append("\n");
      } else {
        sb.append("  ").append(fromId).append(" -.->|failure| ").append(toId).append("\n");
      }
    }

    if (options == null || options.isIncludeNotes()) {
      int nIndex = 0;
      for (NotePadMeta note : workflowMeta.getNotes()) {
        if (note.getNote() != null && !note.getNote().trim().isEmpty()) {
          String noteId = "note" + nIndex++;
          String noteText =
              PipelineMermaidDiagramExporter.escapeMermaidLabel(note.getNote().trim());
          sb.append("  ").append(noteId).append("[\"").append(noteText).append("\"]\n");
        }
      }
    }

    String content = sb.toString();

    if (options != null && options.getTargetFilename() != null) {
      writeToTarget(options.getTargetFilename(), content, context);
    }

    return DiagramExportResult.success(
        options != null ? options.getTargetFilename() : null, content, "text/vnd.mermaid");
  }
}
