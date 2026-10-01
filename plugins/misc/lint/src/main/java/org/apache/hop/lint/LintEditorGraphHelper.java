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

import java.io.File;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.widgets.Composite;

/** Utility methods for locating pipeline/workflow editor widgets from the lint plugin. */
public final class LintEditorGraphHelper {

  private LintEditorGraphHelper() {}

  public static String getFilename(HopGuiAbstractGraph graph) {
    if (graph == null) {
      return null;
    }
    if (graph instanceof HopGuiPipelineGraph) {
      return ((HopGuiPipelineGraph) graph).getFilename();
    }
    if (graph instanceof HopGuiWorkflowGraph) {
      return ((HopGuiWorkflowGraph) graph).getFilename();
    }
    return null;
  }

  public static HopGuiAbstractGraph findOpenGraphForFilename(String filename) {
    if (Utils.isEmpty(filename)) {
      return null;
    }
    // peekInstance so that asking "is this file open?" never brings a GUI into existence; on a
    // headless machine constructing one throws rather than returning null.
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui == null) {
      return null;
    }

    HopGuiPipelineGraph pipelineGraph = HopGui.getActivePipelineGraph();
    if (pipelineGraph != null && LintPathUtils.pathsMatch(pipelineGraph.getFilename(), filename)) {
      return pipelineGraph;
    }

    HopGuiWorkflowGraph workflowGraph = HopGui.getActiveWorkflowGraph();
    if (workflowGraph != null && LintPathUtils.pathsMatch(workflowGraph.getFilename(), filename)) {
      return workflowGraph;
    }

    if (hopGui.getActiveFileTypeHandler() instanceof HopGuiAbstractGraph) {
      HopGuiAbstractGraph activeGraph = (HopGuiAbstractGraph) hopGui.getActiveFileTypeHandler();
      if (LintPathUtils.pathsMatch(getFilename(activeGraph), filename)) {
        return activeGraph;
      }
    }

    // Finally every open editor, not only the focused one. Linting happens in the background for
    // whatever the user opened or saved, which is often not the tab they are looking at, and
    // stopping at the active editor left those files with a Problems tab that never filled.
    return findAmongOpenEditors(filename);
  }

  private static HopGuiAbstractGraph findAmongOpenEditors(String filename) {
    try {
      // Ask for the explorer perspective by type rather than taking the active one: a background
      // lint finishes whenever it finishes, and the user may well be looking at a different
      // perspective by then. Keying off the focused perspective made the sync miss those.
      ExplorerPerspective perspective =
          HopGui.peekInstance() == null ? null : HopGui.getExplorerPerspective();
      if (perspective == null) {
        return null;
      }
      for (TabItemHandler item : perspective.getItems()) {
        if (item.getTypeHandler() instanceof HopGuiAbstractGraph graph
            && !graph.isDisposed()
            && LintPathUtils.pathsMatch(getFilename(graph), filename)) {
          return graph;
        }
      }
    } catch (Exception e) {
      // A perspective which cannot be walked is not a reason to fail a background lint.
      LogChannel.GENERAL.logDetailed(
          "Could not search open editors for " + filename + ": " + e.getMessage());
    }
    return null;
  }

  public static void scheduleAttachForFilename(String filename) {
    if (!isLintableFilename(filename)) {
      return;
    }
    HopGui gui = HopGui.peekInstance();
    Composite displayRoot = gui == null ? null : gui.getShell();
    if (displayRoot == null || displayRoot.isDisposed()) {
      return;
    }
    displayRoot.getDisplay().timerExec(250, () -> tryAttachForFilename(filename, 0));
  }

  private static void tryAttachForFilename(String filename, int attempt) {
    HopGuiAbstractGraph graph = findOpenGraphForFilename(filename);
    if (graph != null) {
      EditorLintSupport.onNewGraph(graph);
      return;
    }
    if (attempt < 8) {
      HopGui gui = HopGui.peekInstance();
      Composite displayRoot = gui == null ? null : gui.getShell();
      if (displayRoot != null && !displayRoot.isDisposed()) {
        displayRoot.getDisplay().timerExec(250, () -> tryAttachForFilename(filename, attempt + 1));
      }
    }
  }

  public static String getActiveEditorFilename() {
    IHopFileTypeHandler handler = getActiveEditorHandler();
    if (handler == null) {
      return null;
    }
    String filename = handler.getFilename();
    if (Utils.isEmpty(filename)) {
      return null;
    }
    return LintPathUtils.normalizePath(filename);
  }

  public static IHopFileTypeHandler getActiveEditorHandler() {
    // No UI, no active editor. Asking through getInstance() would build a HopGui to answer, and
    // building one needs a display.
    if (HopGui.peekInstance() == null) {
      return null;
    }
    ExplorerPerspective explorer = HopGui.getExplorerPerspective();
    if (explorer == null) {
      return null;
    }
    IHopFileTypeHandler handler = explorer.getActiveFileTypeHandler();
    if (handler == null || handler instanceof EmptyHopFileTypeHandler) {
      return null;
    }
    return handler;
  }

  public static IHopFileTypeHandler findOpenHandlerForPath(String path) {
    if (Utils.isEmpty(path)) {
      return null;
    }
    if (HopGui.peekInstance() == null) {
      return null;
    }
    ExplorerPerspective explorer = HopGui.getExplorerPerspective();
    if (explorer == null) {
      return null;
    }

    IHopFileTypeHandler exact = explorer.findFileTypeHandlerByFilename(path);
    if (exact != null) {
      return exact;
    }

    String normalized = LintPathUtils.normalizePath(path);
    if (!normalized.equals(path)) {
      exact = explorer.findFileTypeHandlerByFilename(normalized);
      if (exact != null) {
        return exact;
      }
    }

    for (TabItemHandler tabItem : explorer.getTabItemHandlersInPaneOrder()) {
      IHopFileTypeHandler handler = tabItem.getTypeHandler();
      if (handler != null && LintPathUtils.pathsMatch(handler.getFilename(), path)) {
        return handler;
      }
    }
    return null;
  }

  public static boolean isLintableFilename(String filename) {
    if (Utils.isEmpty(filename)) {
      return false;
    }
    String lower = filename.toLowerCase();
    if (lower.endsWith(".hpl") || lower.endsWith(".hwf")) {
      return true;
    }
    return HopMetadataFileLoader.isMetadataJsonFile(filename);
  }

  public static String displayName(String filePath) {
    if (Utils.isEmpty(filePath)) {
      return filePath;
    }
    return new File(LintPathUtils.normalizePath(filePath)).getName();
  }
}
