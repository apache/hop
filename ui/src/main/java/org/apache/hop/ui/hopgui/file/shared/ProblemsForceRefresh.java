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

package org.apache.hop.ui.hopgui.file.shared;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;

/**
 * Clears cached database metadata and asks for a fresh check of the open editors.
 *
 * <p>The Problems tab of a pipeline and of a workflow both call this. The database cache is
 * process-wide, so the click also drops the field lists of every open pipeline. The editor whose
 * button was pressed is re-checked by the caller. Every other open editor is handed to {@link
 * HopExtensionPoint#ProblemsForceRefresh} so the linter can refresh it without a second modal
 * dialog.
 */
public final class ProblemsForceRefresh {

  private ProblemsForceRefresh() {}

  /**
   * Clear the database cache and the open pipelines' field caches, then re-check.
   *
   * @param active the editor whose Problems tab was clicked
   * @param recheckActive runs the existing check of that editor, after the caches are empty
   */
  public static void refresh(HopGuiAbstractGraph active, Runnable recheckActive) {
    refresh(openPipelineMetas(), () -> notifyOthers(active), recheckActive);
  }

  /**
   * The cache half of {@link #refresh(HopGuiAbstractGraph, Runnable)}, with the open pipelines and
   * the "other editors" step supplied by the caller. Tests use this so they do not need a display.
   */
  static void refresh(
      List<PipelineMeta> openPipelines, Runnable refreshOthers, Runnable recheckActive) {
    DbCache.getInstance().clear(null);
    if (openPipelines != null) {
      for (PipelineMeta pipelineMeta : openPipelines) {
        if (pipelineMeta != null) {
          pipelineMeta.clearCaches();
        }
      }
    }
    if (refreshOthers != null) {
      refreshOthers.run();
    }
    if (recheckActive != null) {
      recheckActive.run();
    }
  }

  private static void notifyOthers(HopGuiAbstractGraph active) {
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL,
          Variables.getADefaultVariableSpace(),
          HopExtensionPoint.ProblemsForceRefresh.id,
          active);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Could not refresh the other open editors after clearing the database cache", e);
    }
  }

  private static List<PipelineMeta> openPipelineMetas() {
    List<PipelineMeta> pipelines = new ArrayList<>();
    // peekInstance: asking for the perspective must not build a HopGui. On a headless run that
    // throws instead of returning null.
    if (HopGui.peekInstance() == null) {
      return pipelines;
    }
    try {
      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      if (perspective == null) {
        return pipelines;
      }
      for (TabItemHandler item : perspective.getItems()) {
        if (item.getTypeHandler() instanceof HopGuiPipelineGraph graph
            && !graph.isDisposed()
            && graph.getPipelineMeta() != null) {
          pipelines.add(graph.getPipelineMeta());
        }
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed("Could not list open pipelines: " + e.getMessage());
    }
    return pipelines;
  }
}
