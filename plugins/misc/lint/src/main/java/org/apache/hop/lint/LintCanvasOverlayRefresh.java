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

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.widgets.Display;

/**
 * Redraws open pipeline/workflow editors when lint results change so canvas overlays stay in sync.
 */
public final class LintCanvasOverlayRefresh {

  private static volatile boolean registered;

  private LintCanvasOverlayRefresh() {}

  public static void ensureRegistered() {
    if (registered) {
      return;
    }
    synchronized (LintCanvasOverlayRefresh.class) {
      if (registered) {
        return;
      }
      LintResultsManager.getInstance().addListener(LintCanvasOverlayRefresh::onResultsUpdated);
      registered = true;
    }
  }

  private static void onResultsUpdated() {
    redrawOpenGraphs();
  }

  public static void redrawOpenGraphs() {
    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null) {
      return;
    }

    Display display = null;
    if (hopGui.getShell() != null && !hopGui.getShell().isDisposed()) {
      display = hopGui.getShell().getDisplay();
    }
    if (display == null) {
      display = Display.getCurrent();
    }
    if (display == null || display.isDisposed()) {
      return;
    }

    display.asyncExec(
        () -> {
          ExplorerPerspective explorer = hopGui.getExplorerPerspective();
          if (explorer == null) {
            return;
          }
          for (TabItemHandler tabItem : explorer.getTabItemHandlersInPaneOrder()) {
            IHopFileTypeHandler handler = tabItem.getTypeHandler();
            if (handler instanceof HopGuiAbstractGraph graph && !graph.isDisposed()) {
              graph.redraw();
            }
          }
        });
  }
}
