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

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.SessionDisplay;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.widgets.Display;

/**
 * Redraws open pipeline/workflow editors when lint results change so canvas overlays stay in sync.
 */
public final class LintCanvasOverlayRefresh {

  /**
   * The results this listens to, one entry per set of findings we have subscribed to.
   *
   * <p>A single flag registered with the first session's results and left every later session
   * without canvas overlays: Hop Web gives each of them findings of their own. Weakly held so a
   * session that goes away takes its entry with it.
   */
  private static final Set<LintResultsManager> registered =
      Collections.newSetFromMap(Collections.synchronizedMap(new WeakHashMap<>()));

  private LintCanvasOverlayRefresh() {}

  public static void ensureRegistered() {
    LintResultsManager results = LintResultsManager.getInstance();
    synchronized (LintCanvasOverlayRefresh.class) {
      if (!registered.add(results)) {
        return;
      }
      results.addListener(LintCanvasOverlayRefresh::onResultsUpdated);
    }
  }

  private static void onResultsUpdated() {
    redrawOpenGraphs();
  }

  public static void redrawOpenGraphs() {
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui == null) {
      return;
    }

    Display display = null;
    if (hopGui.getShell() != null && !hopGui.getShell().isDisposed()) {
      display = hopGui.getShell().getDisplay();
    }
    if (display == null) {
      display = SessionDisplay.current();
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
