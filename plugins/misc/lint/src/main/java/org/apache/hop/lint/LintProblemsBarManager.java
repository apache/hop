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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.SessionDisplay;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.eclipse.swt.widgets.Display;

/** Manages lint problem indicators on open pipeline/workflow editor toolbars. */
public class LintProblemsBarManager {

  private static final ILogChannel log = LogChannel.GENERAL;

  /** How long to keep looking for the editor of a file that was just linted, in 250ms steps. */
  private static final int EDITOR_WAIT_ATTEMPTS = 12;

  private static final int EDITOR_WAIT_INTERVAL_MS = 250;

  /** Used when there is no GUI to own the editors: the command line, and unit tests. */
  private static LintProblemsBarManager fallback;

  private final Map<String, HopGuiAbstractGraph> graphsById = new ConcurrentHashMap<>();
  private final Map<String, String> filePathByGraphId = new ConcurrentHashMap<>();

  private LintProblemsBarManager() {}

  /**
   * The editors of the GUI that asks for them.
   *
   * <p>This holds widgets, and in Hop Web the widgets of one session may not be touched from
   * another: a shared map had {@link #refreshAllOpenEditors()} reaching into every session that
   * happened to be logged in. The desktop has one HopGui and therefore one of these.
   */
  public static LintProblemsBarManager getInstance() {
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui != null) {
      return hopGui.getSessionSingleton(LintProblemsBarManager.class, LintProblemsBarManager::new);
    }
    synchronized (LintProblemsBarManager.class) {
      if (fallback == null) {
        fallback = new LintProblemsBarManager();
      }
      return fallback;
    }
  }

  public void attachToGraph(HopGuiAbstractGraph graph) {
    attachToGraph(graph, 0);
  }

  private void attachToGraph(HopGuiAbstractGraph graph, int attempt) {
    if (graph == null || graph.isDisposed()) {
      return;
    }

    String graphId = graph.getId();
    graphsById.put(graphId, graph);

    Runnable attach =
        () -> {
          if (graph.isDisposed()) {
            return;
          }

          String filename = LintEditorGraphHelper.getFilename(graph);
          if (filename != null) {
            filePathByGraphId.put(graphId, filename);
          }

          List<LintResult> results =
              filename != null
                  ? LintResultsManager.getInstance().getResultsForFile(filename)
                  : List.of();

          // Keep the Problems tab in sync; the canvas overlay repaints itself on result change.
          if (filename != null) {
            updateProblemsBar(filename);
          }

          if (results.isEmpty() && attempt < 8 && filename != null) {
            retryAttachLater(graph, attempt);
            return;
          }

          log.logDetailed(
              "Attached lint problems sync for "
                  + graphId
                  + " ("
                  + LintEditorGraphHelper.displayName(filename)
                  + ")");
        };

    Display display = graph.getDisplay();
    if (display != null) {
      display.asyncExec(attach);
    } else {
      attach.run();
    }
  }

  private void retryAttachLater(HopGuiAbstractGraph graph, int attempt) {
    if (attempt >= 8 || graph.isDisposed()) {
      return;
    }
    Display display = graph.getDisplay();
    if (display != null) {
      display.timerExec(250, () -> attachToGraph(graph, attempt + 1));
    }
  }

  public void detachGraph(String graphId) {
    graphsById.remove(graphId);
    filePathByGraphId.remove(graphId);
  }

  public void updateProblemsBar(String filePath) {
    if (filePath == null) {
      return;
    }
    // This touches SWT widgets, so make sure it runs on the UI thread regardless of which
    // thread the caller is on (background lint threads call this too).
    if (SessionDisplay.current() == null) {
      Display display = sessionDisplay();
      if (display == null || display.isDisposed()) {
        log.logDetailed(
            "Not syncing the Problems tab for "
                + filePath
                + ": this thread has no display to do it on");
        return;
      }
      display.asyncExec(() -> updateProblemsBar(filePath));
      return;
    }
    updateProblemsBar(filePath, 0);
  }

  /**
   * Sync the editor Problems tab, retrying while the editor is not there yet.
   *
   * <p>Linting a file that has just been opened finishes before the editor tab is registered, and a
   * single attempt simply lost that race: the findings sat in the results manager with nothing
   * showing them until the user clicked the canvas totals. Lint totals themselves are drawn as a
   * canvas overlay (see Pipeline/WorkflowLintTotalsPainterExtension); here we only keep the tab in
   * sync.
   */
  private void updateProblemsBar(String filePath, int attempt) {
    boolean synced =
        PipelineProblemsTabSync.refreshForFile(filePath)
            | WorkflowProblemsTabSync.refreshForFile(filePath);
    if (synced) {
      return;
    }
    if (attempt >= EDITOR_WAIT_ATTEMPTS) {
      log.logDetailed(
          "Gave up syncing the Problems tab for " + filePath + ": no editor found in time");
      return;
    }
    Display display = SessionDisplay.current();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.timerExec(EDITOR_WAIT_INTERVAL_MS, () -> updateProblemsBar(filePath, attempt + 1));
  }

  /**
   * The display to get onto the UI thread with.
   *
   * <p>An editor we already know about answers first: the default display only knows the session
   * bound to the calling thread, which is nothing at all on a thread that was started without one -
   * and in Hop Web the wrong session's display would be worse than none.
   */
  private Display sessionDisplay() {
    for (HopGuiAbstractGraph graph : graphsById.values()) {
      if (!graph.isDisposed()) {
        Display display = graph.getDisplay();
        if (display != null && !display.isDisposed()) {
          return display;
        }
      }
    }
    return SessionDisplay.currentOrDefault();
  }

  public void refreshAllOpenEditors() {
    for (Map.Entry<String, String> entry : filePathByGraphId.entrySet()) {
      updateProblemsBar(entry.getValue());
    }
  }

  public boolean hasBarForGraph(String graphId) {
    return graphsById.containsKey(graphId);
  }

  public void detachByFilename(String filePath) {
    filePathByGraphId
        .entrySet()
        .removeIf(
            entry -> {
              if (LintPathUtils.pathsMatch(entry.getValue(), filePath)) {
                graphsById.remove(entry.getKey());
                return true;
              }
              return false;
            });
  }
}
