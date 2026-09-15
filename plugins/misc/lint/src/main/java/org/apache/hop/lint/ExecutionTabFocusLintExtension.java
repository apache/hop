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

import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Display;

/**
 * Hand the execution panel back to the logging tab when a run starts.
 *
 * <p>Hop only picks a tab when nothing is selected ({@code addAllTabs} selects index 0), so a panel
 * already open on the Problems tab stays there while the pipeline runs. That is fine in stock Hop,
 * where Problems is rarely the selected tab; with the linter it often is, and watching lint
 * findings while a run produces log output is not what anyone wants.
 *
 * <p>Only ever moves away from the lint Problems tab. Any other tab the user chose — metrics, a
 * preview, a plugin tab — is left exactly where it is.
 */
final class ExecutionTabFocusLintExtension {

  private ExecutionTabFocusLintExtension() {}

  /**
   * Move off the Problems tab, once the execution tabs exist.
   *
   * <p>Posted rather than run inline: the run path adds the logging and metrics tabs after this
   * fires, and selecting before they exist would select the wrong thing.
   */
  static void restoreLogTab(HopGuiPipelineGraph graph) {
    if (graph == null || graph.isDisposed()) {
      return;
    }
    Display display = graph.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (graph.isDisposed() || graph.pipelineCheckDelegate == null) {
            return;
          }
          selectAway(
              graph.extraViewTabFolder,
              graph.pipelineCheckDelegate.getPipelineCheckTab(),
              graph.pipelineLogDelegate == null
                  ? null
                  : graph.pipelineLogDelegate.getPipelineLogTab());
        });
  }

  static void restoreLogTab(HopGuiWorkflowGraph graph) {
    if (graph == null || graph.isDisposed()) {
      return;
    }
    Display display = graph.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (graph.isDisposed() || graph.workflowCheckDelegate == null) {
            return;
          }
          selectAway(
              graph.extraViewTabFolder,
              graph.workflowCheckDelegate.getWorkflowCheckTab(),
              graph.workflowLogDelegate == null
                  ? null
                  : graph.workflowLogDelegate.getWorkflowLogTab());
        });
  }

  private static void selectAway(CTabFolder folder, CTabItem checkTab, CTabItem logTab) {
    if (folder == null
        || folder.isDisposed()
        || checkTab == null
        || checkTab.isDisposed()
        || logTab == null
        || logTab.isDisposed()) {
      return;
    }
    if (folder.getSelection() == checkTab) {
      folder.setSelection(logTab);
    }
  }
}
