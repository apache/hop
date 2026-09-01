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
package org.apache.hop.lint;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.apache.hop.workflow.WorkflowMeta;

/** Opens the lint results when the lint totals canvas overlay is clicked on a workflow. */
@ExtensionPoint(
    id = "WorkflowLintTotalsClickExtension",
    extensionPointId = "WorkflowGraphMouseDown",
    description = "Shows lint results when the workflow lint totals overlay is clicked")
public class WorkflowLintTotalsClickExtension
    implements IExtensionPoint<HopGuiWorkflowGraphExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiWorkflowGraphExtension ext) throws HopException {
    if (ext == null || ext.getEvent() == null) {
      return;
    }
    try {
      HopGuiWorkflowGraph graph = ext.getWorkflowGraph();
      if (graph == null || graph.getWorkflowMeta() == null) {
        return;
      }
      WorkflowMeta meta = graph.getWorkflowMeta();
      String filePath = LintPathUtils.normalizePath(meta.getFilename());
      if (LintCanvasOverlayHelper.totalsRectContains(
          filePath, ext.getEvent().x, ext.getEvent().y)) {
        ext.setPreventingDefault(true);
        // Straight to the Problems tab, never the results window: the overlay is drawn on the
        // canvas of an open file, and an open file always has a tab to show its findings in. The
        // window is for what has no editor — a folder, a project, a file that is not open.
        //
        // Nothing to show is not an error either: revealForFile does nothing when the file is
        // clean and no tab has been opened for it, so clicking a 0/0/0 overlay is a no-op rather
        // than an empty window.
        WorkflowProblemsTabSync.revealForFile(filePath);
      }
    } catch (Exception e) {
      log.logError("Error handling lint totals overlay click: " + e.getMessage(), e);
    }
  }
}
