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
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowCheckDelegate;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Display;

/** Keeps the native workflow editor Problems tab in sync with lint results. */
public final class WorkflowProblemsTabSync {

  private static final ILogChannel log = LogChannel.GENERAL;

  private WorkflowProblemsTabSync() {}

  /**
   * Push the findings for a file into its editor's Problems tab.
   *
   * @param filePath the file whose findings to show
   * @return true when an editor for the file was found, false when there is nothing to sync yet
   */
  public static boolean refreshForFile(String filePath) {
    if (filePath == null) {
      return false;
    }
    List<LintResult> results = LintResultsManager.getInstance().getResultsForFile(filePath);
    HopGuiAbstractGraph graph = LintEditorGraphHelper.findOpenGraphForFilename(filePath);
    if (graph instanceof HopGuiWorkflowGraph workflowGraph) {
      // Passive: fill the tab if it exists, never create or select it.
      refreshGraph(workflowGraph, results, false);
      return true;
    }
    return false;
  }

  /**
   * Put the findings for a file in the Problems tab and bring that tab to the front.
   *
   * @param filePath the file whose findings to show
   * @return true when an editor was found and its Problems tab revealed
   */
  public static boolean revealForFile(String filePath) {
    HopGuiAbstractGraph graph = LintEditorGraphHelper.findOpenGraphForFilename(filePath);
    if (!(graph instanceof HopGuiWorkflowGraph workflowGraph)) {
      return false;
    }
    List<LintResult> results = LintResultsManager.getInstance().getResultsForFile(filePath);
    HopGuiWorkflowCheckDelegate delegate = workflowGraph.workflowCheckDelegate;
    boolean tabExists =
        delegate != null
            && delegate.getWorkflowCheckTab() != null
            && !delegate.getWorkflowCheckTab().isDisposed();
    if (results.isEmpty() && !tabExists) {
      // Nothing to show and no tab to show it in. Say so the other way, rather than swallowing the
      // click.
      return false;
    }
    // The explicit path: create the tab if it is not there, then bring it to the front.
    refreshGraph(workflowGraph, results, true);
    workflowGraph
        .getDisplay()
        .asyncExec(
            () -> {
              try {
                if (workflowGraph.isDisposed()) {
                  return;
                }
                CTabItem tab =
                    workflowGraph.workflowCheckDelegate == null
                        ? null
                        : workflowGraph.workflowCheckDelegate.getWorkflowCheckTab();
                if (tab != null
                    && !tab.isDisposed()
                    && workflowGraph.extraViewTabFolder != null
                    && !workflowGraph.extraViewTabFolder.isDisposed()) {
                  workflowGraph.extraViewTabFolder.setSelection(tab);
                }
              } catch (Exception e) {
                log.logDetailed("Could not reveal the Problems tab: " + e.getMessage());
              }
            });
    return true;
  }

  public static void refreshGraph(HopGuiWorkflowGraph graph, List<LintResult> results) {
    refreshGraph(graph, results, false);
  }

  /**
   * Put the findings into the editor's Problems tab.
   *
   * <p>A background sync — opening a file, saving it, linting on edit — only fills a tab which is
   * already there. Creating one would put it in the tab folder ahead of Log and Metrics, which
   * changes which tab {@code addAllTabs()} selects when the pipeline runs, and would open a panel
   * the user never asked for. Only an explicit lint creates the tab, and only that selects it.
   *
   * @param graph the editor to update
   * @param results the findings for its file
   * @param createTab whether the Problems tab may be created when it does not exist yet
   */
  public static void refreshGraph(
      HopGuiWorkflowGraph graph, List<LintResult> results, boolean createTab) {
    if (graph == null || graph.isDisposed()) {
      return;
    }

    Runnable sync =
        () -> {
          if (graph.isDisposed()) {
            return;
          }
          try {
            HopGuiWorkflowCheckDelegate delegate = graph.workflowCheckDelegate;
            if (delegate == null) {
              return;
            }

            // Only open the Problems view when there is something to put in it. A clean file
            // should not have a panel forced open at the bottom of its editor; an existing tab is
            // still refreshed, so findings that have been fixed do get cleared.
            boolean tabExists =
                delegate.getWorkflowCheckTab() != null
                    && !delegate.getWorkflowCheckTab().isDisposed();
            if (!tabExists && (results.isEmpty() || !createTab)) {
              return;
            }
            delegate.addWorkflowCheck();

            WorkflowMeta workflowMeta = graph.getWorkflowMeta();
            List<ICheckResult> remarks =
                WorkflowCheckResultAdapter.toCheckResults(results, workflowMeta);
            delegate.refresh(remarks);
          } catch (Exception e) {
            log.logError("Error refreshing workflow Problems tab: " + e.getMessage(), e);
          }
        };

    Display display = graph.getDisplay();
    if (display != null && !display.isDisposed()) {
      display.asyncExec(sync);
    } else {
      sync.run();
    }
  }
}
