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

import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;

/** Shared logic for editor-related lint extension points. */
final class EditorLintSupport {

  private EditorLintSupport() {}

  static void onNewGraph(HopGuiAbstractGraph graph) {
    if (graph == null || graph.isDisposed()) {
      return;
    }
    LintProblemsBarManager.getInstance().attachToGraph(graph);
    BackgroundLintService.getInstance().scheduleGraphLint(graph, true);
  }

  static void onGraphUpdate(HopGuiAbstractGraph graph) {
    if (graph == null || graph.isDisposed()) {
      return;
    }
    if (!LinterConfigPlugin.getInstance().isLintOnEditEnabled()) {
      return;
    }
    String filename = LintEditorGraphHelper.getFilename(graph);
    if (!LintEditorGraphHelper.isLintableFilename(filename)) {
      return;
    }
    if (!LintProblemsBarManager.getInstance().hasBarForGraph(graph.getId())) {
      LintProblemsBarManager.getInstance().attachToGraph(graph);
    }
    BackgroundLintService.getInstance().getTracker().invalidate(filename);
    BackgroundLintService.getInstance().scheduleGraphLint(graph, true);
  }

  static void onFileSaved(String filename) {
    if (!LintEditorGraphHelper.isLintableFilename(filename)) {
      return;
    }
    BackgroundLintService.getInstance().getTracker().invalidate(filename);
    BackgroundLintService.getInstance().scheduleFileLint(filename, true);
  }

  static void onGraphClosed(String filename) {
    if (!Utils.isEmpty(filename)) {
      LintProblemsBarManager.getInstance().detachByFilename(filename);
    }
  }

  static void handleGraphObject(Object object) {
    if (object instanceof HopGuiPipelineGraph) {
      onNewGraph((HopGuiPipelineGraph) object);
    } else if (object instanceof HopGuiWorkflowGraph) {
      onNewGraph((HopGuiWorkflowGraph) object);
    }
  }

  static void handleGraphUpdateObject(Object object) {
    if (object instanceof HopGuiPipelineGraph) {
      onGraphUpdate((HopGuiPipelineGraph) object);
    } else if (object instanceof HopGuiWorkflowGraph) {
      onGraphUpdate((HopGuiWorkflowGraph) object);
    }
  }
}
