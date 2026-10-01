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
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Diagram exporter that generates a Mermaid flowchart (flowchart LR/TD) from a Pipeline. */
@DiagramExporter(
    id = "pipeline-mermaid",
    name = "Pipeline Mermaid Exporter",
    description = "Exports a pipeline diagram to Mermaid flowchart (.mmd)",
    format = "MERMAID",
    fileExtension = "mmd",
    fileFilterNames = {"Mermaid Diagrams (*.mmd)"},
    supportedSubjectTypes = {PipelineMeta.class})
public class PipelineMermaidDiagramExporter extends BaseDiagramExporter<PipelineMeta> {

  public PipelineMermaidDiagramExporter() {
    super();
  }

  @Override
  public DiagramExportResult export(
      PipelineMeta pipelineMeta, DiagramExportOptions options, IExportContext context)
      throws HopException {
    if (pipelineMeta == null) {
      throw new HopException("PipelineMeta is null");
    }

    String direction =
        options != null && options.getDirection() != null ? options.getDirection() : "LR";

    StringBuilder sb = new StringBuilder();
    sb.append("flowchart ").append(direction).append("\n");

    Map<String, String> transformIds = new HashMap<>();
    int tIndex = 0;
    for (TransformMeta transform : pipelineMeta.getTransforms()) {
      String id = "t" + tIndex++;
      transformIds.put(transform.getName(), id);
      String label = escapeMermaidLabel(transform.getName());
      sb.append("  ").append(id).append("[\"").append(label).append("\"]\n");
    }

    for (PipelineHopMeta hop : pipelineMeta.getPipelineHops()) {
      if (hop.getFrom() == null || hop.getTo() == null) {
        continue;
      }
      String fromId = transformIds.get(hop.getFrom().getName());
      String toId = transformIds.get(hop.getTo().getName());
      if (fromId == null || toId == null) {
        continue;
      }

      boolean isErrorHop = false;
      if (hop.getFrom().getTransformErrorMeta() != null
          && hop.getFrom().getTransformErrorMeta().getTargetTransform() != null
          && hop.getTo().equals(hop.getFrom().getTransformErrorMeta().getTargetTransform())) {
        isErrorHop = true;
      }

      if (!hop.isEnabled()) {
        sb.append("  ").append(fromId).append(" -.->|disabled| ").append(toId).append("\n");
      } else if (isErrorHop) {
        sb.append("  ").append(fromId).append(" -.->|error| ").append(toId).append("\n");
      } else {
        sb.append("  ").append(fromId).append(" --> ").append(toId).append("\n");
      }
    }

    if (options == null || options.isIncludeNotes()) {
      int nIndex = 0;
      for (NotePadMeta note : pipelineMeta.getNotes()) {
        if (note.getNote() != null && !note.getNote().trim().isEmpty()) {
          String noteId = "note" + nIndex++;
          String noteText = escapeMermaidLabel(note.getNote().trim());
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

  public static String escapeMermaidLabel(String text) {
    if (text == null) {
      return "";
    }
    return text.replace("\"", "#34;")
        .replace("[", "#91;")
        .replace("]", "#93;")
        .replace("(", "#40;")
        .replace(")", "#41;")
        .replace("\r\n", "<br/>")
        .replace("\n", "<br/>")
        .replace("\r", "<br/>");
  }
}
