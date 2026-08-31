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

import java.util.List;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowCheckDelegate;
import org.apache.hop.workflow.WorkflowMeta;

/** Runs workflow verify with native action checks plus YAML/custom lint rules. */
public final class WorkflowVerifyLintService {

  private static final ILogChannel log = LogChannel.GENERAL;

  private WorkflowVerifyLintService() {}

  public static void runVerify(HopGuiWorkflowGraph graph) {
    if (graph == null || graph.isDisposed()) {
      return;
    }

    try {
      graph.addAllTabs();

      HopGuiWorkflowCheckDelegate delegate = graph.workflowCheckDelegate;
      if (delegate == null) {
        return;
      }
      delegate.addWorkflowCheck();
      graph.extraViewTabFolder.setSelection(delegate.getWorkflowCheckTab());

      HopGui hopGui = HopGui.getInstance();
      WorkflowMeta workflowMeta = graph.getWorkflowMeta();
      String fileName = LintPathUtils.normalizePath(workflowMeta.getFilename());

      List<LintResult> results =
          WorkflowLintResultsBuilder.lintWorkflowLikeVerify(
              workflowMeta,
              fileName,
              hopGui != null ? hopGui.getMetadataProvider() : null,
              graph.getVariables());

      LintResultsManager.getInstance().updateResultsForFile(fileName, results);
      LintProblemsBarManager.getInstance().updateProblemsBar(fileName);
      WorkflowProblemsTabSync.refreshGraph(graph, results);

      log.logDetailed(
          "Workflow verify with lint completed for "
              + LintEditorGraphHelper.displayName(fileName)
              + ": "
              + results.size()
              + " issue(s)");
    } catch (Exception e) {
      log.logError("Error running workflow verify with lint: " + e.getMessage(), e);
    }
  }
}
