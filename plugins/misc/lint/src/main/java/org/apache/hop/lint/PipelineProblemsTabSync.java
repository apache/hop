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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineCheckDelegate;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Display;

/** Keeps the native pipeline editor Problems tab in sync with lint results. */
public final class PipelineProblemsTabSync {

  private static final ILogChannel log = LogChannel.GENERAL;

  private PipelineProblemsTabSync() {}

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
    if (graph instanceof HopGuiPipelineGraph pipelineGraph) {
      refreshGraph(pipelineGraph, results);
      return true;
    }
    return false;
  }

  /**
   * Put the findings for a file in the Problems tab and bring that tab to the front.
   *
   * <p>This is where lint results belong: next to Hop's own checks, in the editor for the file they
   * are about, rather than in a window of their own.
   *
   * @param filePath the file whose findings to show
   * @return true when an editor was found and its Problems tab revealed
   */
  public static boolean revealForFile(String filePath) {
    HopGuiAbstractGraph graph = LintEditorGraphHelper.findOpenGraphForFilename(filePath);
    if (!(graph instanceof HopGuiPipelineGraph pipelineGraph)) {
      return false;
    }
    List<LintResult> results = LintResultsManager.getInstance().getResultsForFile(filePath);
    HopGuiPipelineCheckDelegate delegate = pipelineGraph.pipelineCheckDelegate;
    boolean tabExists =
        delegate != null
            && delegate.getPipelineCheckTab() != null
            && !delegate.getPipelineCheckTab().isDisposed();
    if (results.isEmpty() && !tabExists) {
      // Nothing to show and no tab to show it in. Say so the other way, rather than swallowing the
      // click.
      return false;
    }
    refreshGraph(pipelineGraph, results);
    pipelineGraph
        .getDisplay()
        .asyncExec(
            () -> {
              try {
                if (pipelineGraph.isDisposed()) {
                  return;
                }
                CTabItem tab =
                    pipelineGraph.pipelineCheckDelegate == null
                        ? null
                        : pipelineGraph.pipelineCheckDelegate.getPipelineCheckTab();
                if (tab != null
                    && !tab.isDisposed()
                    && pipelineGraph.extraViewTabFolder != null
                    && !pipelineGraph.extraViewTabFolder.isDisposed()) {
                  pipelineGraph.extraViewTabFolder.setSelection(tab);
                }
              } catch (Exception e) {
                log.logDetailed("Could not reveal the Problems tab: " + e.getMessage());
              }
            });
    return true;
  }

  public static void refreshGraph(HopGuiPipelineGraph graph, List<LintResult> results) {
    if (graph == null || graph.isDisposed()) {
      return;
    }

    Runnable sync =
        () -> {
          if (graph.isDisposed()) {
            return;
          }
          try {
            HopGuiPipelineCheckDelegate delegate = graph.pipelineCheckDelegate;
            if (delegate == null) {
              return;
            }

            // Only open the Problems view when there is something to put in it. A clean file
            // should not have a panel forced open at the bottom of its editor; an existing tab is
            // still refreshed, so findings that have been fixed do get cleared.
            boolean tabExists =
                delegate.getPipelineCheckTab() != null
                    && !delegate.getPipelineCheckTab().isDisposed();
            if (results.isEmpty() && !tabExists) {
              return;
            }
            delegate.addPipelineCheck();

            // Creating the tab does not select it, and a CTabFolder with no selection draws an
            // empty client area: the findings were going into a tab nobody could see. Select it
            // only when nothing else is, so we never pull the user off the execution results.
            if (graph.extraViewTabFolder != null
                && !graph.extraViewTabFolder.isDisposed()
                && graph.extraViewTabFolder.getSelectionIndex() == -1
                && delegate.getPipelineCheckTab() != null
                && !delegate.getPipelineCheckTab().isDisposed()) {
              graph.extraViewTabFolder.setSelection(delegate.getPipelineCheckTab());
            }

            PipelineMeta pipelineMeta = graph.getPipelineMeta();
            List<ICheckResult> remarks =
                LintCheckResultAdapter.toCheckResults(results, pipelineMeta);
            graph.setRemarks(remarks);
            delegate.refresh(remarks);
          } catch (Exception e) {
            log.logError("Error refreshing pipeline Problems tab: " + e.getMessage(), e);
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
