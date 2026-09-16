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

import org.apache.hop.core.diagram.BaseDiagramExporter;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportResult;
import org.apache.hop.core.diagram.DiagramExporter;
import org.apache.hop.core.diagram.IExportContext;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowSvgPainter;

/** Diagram exporter for Workflow to SVG. */
@DiagramExporter(
    id = "workflow-svg",
    name = "Workflow SVG Exporter",
    description = "Exports a workflow diagram to Scalable Vector Graphics (SVG)",
    format = "SVG",
    fileExtension = "svg",
    fileFilterNames = {"SVG Files (*.svg)"},
    supportedSubjectTypes = {WorkflowMeta.class})
public class WorkflowSvgDiagramExporter extends BaseDiagramExporter<WorkflowMeta> {

  public WorkflowSvgDiagramExporter() {
    super();
  }

  @Override
  public DiagramExportResult export(
      WorkflowMeta workflowMeta, DiagramExportOptions options, IExportContext context)
      throws HopException {
    if (workflowMeta == null) {
      throw new HopException("WorkflowMeta is null");
    }
    float magnification = options != null ? options.getMagnification() : 1.0f;
    String svgXml =
        WorkflowSvgPainter.generateWorkflowSvg(
            workflowMeta, magnification, context != null ? context.getVariables() : null);

    if (options != null && options.getTargetFilename() != null) {
      writeToTarget(options.getTargetFilename(), svgXml, context);
    }

    return DiagramExportResult.success(
        options != null ? options.getTargetFilename() : null, svgXml, "image/svg+xml");
  }
}
