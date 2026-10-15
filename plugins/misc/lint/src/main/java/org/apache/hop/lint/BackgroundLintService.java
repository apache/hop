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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.widgets.Display;

/** Runs lint checks in the background with debouncing and file modification tracking. */
public class BackgroundLintService {

  private static final ILogChannel log = LogChannel.GENERAL;
  private static final int DEFERRED_CHECK_DELAY_MS = 500;

  /**
   * Shared by every session: this is CPU bound file parsing, and a pool per Hop Web session would
   * multiply the threads by the number of people logged in.
   */
  private static final ExecutorService executor =
      Executors.newFixedThreadPool(
          Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
          r -> {
            Thread t = new Thread(r, "HopLinter-Background");
            t.setDaemon(true);
            return t;
          });

  /** Used when there is no GUI to own this: the command line, and unit tests. */
  private static BackgroundLintService fallback;

  private final LintCheckTracker tracker;
  private final Map<String, AtomicInteger> deferredGenerations = new ConcurrentHashMap<>();

  /**
   * Bumped whenever the findings already published stop counting: another project was enabled, or
   * the linter was switched off. A pass remembers the value it started with and does not publish
   * once it has changed, nor mark its file as checked.
   */
  private final AtomicInteger generation = new AtomicInteger(0);

  private String currentProjectPath;

  private BackgroundLintService() {
    this.tracker = new LintCheckTracker();
  }

  /**
   * The service belonging to the GUI that asks for it.
   *
   * <p>Hop Web serves many people from one JVM, each with their own editors and their own findings,
   * so what has already been linted and which editors are waiting for an answer is per session. The
   * desktop has one HopGui and therefore one of these.
   */
  public static BackgroundLintService getInstance() {
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui != null) {
      return hopGui.getSessionSingleton(BackgroundLintService.class, BackgroundLintService::new);
    }
    synchronized (BackgroundLintService.class) {
      if (fallback == null) {
        fallback = new BackgroundLintService();
      }
      return fallback;
    }
  }

  public LintCheckTracker getTracker() {
    return tracker;
  }

  public boolean isEnabled() {
    try {
      return LinterConfigPlugin.getInstance().isLinterEnabled();
    } catch (Exception e) {
      return true;
    }
  }

  public void scheduleFileLint(String filePath, boolean force) {
    scheduleFileLint(filePath, force, null);
  }

  public void scheduleFileLint(String filePath, boolean force, String graphId) {
    if (!isEnabled() || !LintEditorGraphHelper.isLintableFilename(filePath)) {
      return;
    }
    if (!force && !tracker.needsLinting(filePath)) {
      LintProblemsBarManager.getInstance().updateProblemsBar(filePath);
      return;
    }
    int passGeneration = currentGeneration();
    submit(() -> lintFileInternal(filePath, graphId, passGeneration));
  }

  public void scheduleGraphLint(HopGuiAbstractGraph graph, boolean force) {
    // Opening or editing a file schedules this even when the linter is switched off. Without the
    // check, that one open file is still linted and stays marked in the Explorer.
    if (!isEnabled() || graph == null) {
      return;
    }
    String graphId = graph.getId();
    AtomicInteger generation =
        deferredGenerations.computeIfAbsent(graphId, k -> new AtomicInteger(0));
    int runGeneration = generation.incrementAndGet();
    Display display = graph.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.timerExec(
        DEFERRED_CHECK_DELAY_MS,
        () -> {
          if (graph.isDisposed() || runGeneration != generation.get()) {
            return;
          }
          lintGraphInternal(graph, force);
        });
  }

  /**
   * Called when a project is enabled. When it is another project than before, the findings and the
   * record of what was checked belong to the project that was left: drop both, and stop the passes
   * still running for it from publishing.
   *
   * <p>The tracker has to go as well. It skips files that have not changed since they were last
   * checked, so switching back to a project would otherwise leave it without findings.
   */
  public void switchProject(String projectPath) {
    String normalized = Utils.isEmpty(projectPath) ? "" : LintPathUtils.normalizePath(projectPath);
    LintResultsManager manager = LintResultsManager.getInstance();
    synchronized (manager) {
      if (normalized.equals(currentProjectPath)) {
        return;
      }
      // The first project of the session: nothing to drop, and the editors it just restored may
      // already have published their findings.
      boolean firstProject = currentProjectPath == null;
      currentProjectPath = normalized;
      generation.incrementAndGet();
      if (!firstProject) {
        tracker.clear();
        manager.clearResults();
      }
    }
  }

  /**
   * Drop lint tracking for every open editor and re-lint all of them except the one the Problems
   * tab is about to check itself.
   *
   * <p>Called after the database cache has been cleared. The active editor's check fills its own
   * Problems tab. Scheduling it here as well would check it twice.
   *
   * @param active the editor whose force-refresh button was pressed, or null to re-lint every open
   *     editor
   */
  public void refreshOpenEditorsExcept(HopGuiAbstractGraph active) {
    List<OpenEditor> open = new ArrayList<>();
    for (HopGuiAbstractGraph graph : LintEditorGraphHelper.openGraphs()) {
      open.add(new OpenEditor(graph, LintEditorGraphHelper.getFilename(graph)));
    }
    refreshOpenEditorsExcept(
        active, open, graph -> scheduleGraphLint((HopGuiAbstractGraph) graph, true));
  }

  /**
   * Invalidate tracking for each open editor and schedule a forced lint of every one that is not
   * the active editor.
   */
  void refreshOpenEditorsExcept(Object active, List<OpenEditor> open, Consumer<Object> schedule) {
    if (open == null) {
      return;
    }
    for (OpenEditor editor : open) {
      if (editor == null || editor.graph() == null) {
        continue;
      }
      tracker.invalidate(editor.filename());
      if (editor.graph() != active && schedule != null) {
        schedule.accept(editor.graph());
      }
    }
  }

  /** An open editor, reduced to what a force refresh needs so a test does not build a display. */
  record OpenEditor(Object graph, String filename) {}

  /**
   * Called after the linter was switched off. Drops the findings and the record of what was
   * checked, and stops the passes still running from publishing or marking a file as checked.
   *
   * <p>The tracker has to go: switching the linter back on would otherwise skip every file that has
   * not changed since it was last checked, and the project would come back without findings.
   */
  public void discardResults() {
    LintResultsManager manager = LintResultsManager.getInstance();
    synchronized (manager) {
      generation.incrementAndGet();
      tracker.clear();
      manager.clearResults();
    }
  }

  /**
   * The value a pass takes when it starts, to hand to {@link #publish} when it is done. Read it
   * where the pass is started, on the UI thread: that is where a project switch happens.
   */
  int currentGeneration() {
    return generation.get();
  }

  /**
   * Publish the findings of a pass, unless the project was switched or the linter switched off
   * since it started.
   *
   * <p>Checked and published under the lock {@link #switchProject} and {@link #discardResults}
   * clear the results with, so a pass cannot slip its findings in right after either.
   *
   * @return false when the findings were dropped
   */
  boolean publish(String filePath, List<LintResult> results, int passGeneration) {
    return publish(filePath, results, passGeneration, false, DbCache.getInstance().getGeneration());
  }

  /**
   * {@link #publish}, and record the file as checked, unless the database cache was cleared after
   * {@code dbCacheGeneration} was read.
   *
   * @return false when the findings were dropped
   */
  boolean publishIfCurrent(
      String filePath, List<LintResult> results, int passGeneration, int dbCacheGeneration) {
    return publish(filePath, results, passGeneration, true, dbCacheGeneration);
  }

  /** {@link #publish}, and record the file as checked so an unchanged file is not linted again. */
  private boolean publishChecked(
      String filePath, List<LintResult> results, int passGeneration, int dbCacheGeneration) {
    return publish(filePath, results, passGeneration, true, dbCacheGeneration);
  }

  private boolean publish(
      String filePath,
      List<LintResult> results,
      int passGeneration,
      boolean markChecked,
      int dbCacheGeneration) {
    LintResultsManager manager = LintResultsManager.getInstance();
    synchronized (manager) {
      if (!isCurrent(passGeneration) || !sameDbCacheGeneration(dbCacheGeneration)) {
        return false;
      }
      if (markChecked) {
        tracker.markChecked(filePath);
      }
      manager.updateResultsForFile(filePath, results);
      return true;
    }
  }

  /** {@link #publish} for a pass over the whole project, which replaces every finding. */
  boolean publishAll(List<LintResult> results, int passGeneration) {
    LintResultsManager manager = LintResultsManager.getInstance();
    synchronized (manager) {
      if (!isCurrent(passGeneration)) {
        return false;
      }
      manager.updateResults(results);
      return true;
    }
  }

  private boolean isCurrent(int passGeneration) {
    return passGeneration == generation.get() && isEnabled();
  }

  /** False when a Problems-tab force refresh cleared the database cache during this pass. */
  private boolean sameDbCacheGeneration(int dbCacheGeneration) {
    if (DbCache.getInstance().getGeneration() == dbCacheGeneration) {
      return true;
    }
    log.logDetailed(
        "Not publishing lint results because the database cache was cleared during the check");
    return false;
  }

  public void lintProjectAsync(
      String projectPath, IHopMetadataProvider metadataProvider, IVariables variables) {
    if (!isEnabled() || Utils.isEmpty(projectPath)) {
      return;
    }
    int passGeneration = currentGeneration();
    submit(
        () -> {
          int dbCacheGenerationAtStart = DbCache.getInstance().getGeneration();
          try {
            HopLinter linter = new HopLinter();
            List<String> allFiles =
                linter.findLintableFiles(projectPath, includeMetadataInGuiLint());
            List<String> filesToCheck = tracker.filterFilesNeedingCheck(allFiles);
            log.logBasic(
                "Background linting: "
                    + filesToCheck.size()
                    + " files need checking out of "
                    + allFiles.size());

            List<LintResult> accumulated = new ArrayList<>();
            for (String filePath : filesToCheck) {
              if (passGeneration != generation.get()) {
                log.logBasic(
                    "Background linting of " + projectPath + " stopped: results discarded");
                return;
              }
              try {
                // Taken before the check. A force refresh clears the database cache while this
                // pass is running, and the remarks from before that clear must not be published.
                int dbCacheGeneration = DbCache.getInstance().getGeneration();
                List<LintResult> results =
                    lintFileForGui(filePath, linter, metadataProvider, variables);
                if (publishChecked(filePath, results, passGeneration, dbCacheGeneration)) {
                  accumulated.addAll(results);
                }
              } catch (Exception e) {
                log.logError("Error linting " + filePath, e);
              }
            }
            log.logBasic(
                "Background linting completed: "
                    + accumulated.size()
                    + " issues in "
                    + filesToCheck.size()
                    + " files");
            // A force refresh cleared the cache while this pass ran and is already rechecking the
            // open editors. Repainting them from this pass would put the old remarks back.
            if (DbCache.getInstance().getGeneration() != dbCacheGenerationAtStart) {
              return;
            }
            LintProblemsBarManager.getInstance().refreshAllOpenEditors();
          } catch (Exception e) {
            log.logError("Error in background project linting", e);
          }
        });
  }

  /**
   * Lint the editor's current content on a worker thread.
   *
   * <p>Called on the UI thread, which is where the snapshot has to be taken: the user keeps editing
   * the same {@code PipelineMeta} while this runs, and it is not thread-safe. Reading it from the
   * worker meant iterating a model that could be mutated mid-pass — a
   * ConcurrentModificationException waiting for a large project and a fast typist.
   */
  private void lintGraphInternal(HopGuiAbstractGraph graph, boolean force) {
    String graphId = graph.getId();
    if (!isEnabled()) {
      deferredGenerations.remove(graphId);
      return;
    }
    String filename = LintEditorGraphHelper.getFilename(graph);
    if (!LintEditorGraphHelper.isLintableFilename(filename)) {
      deferredGenerations.remove(graphId);
      return;
    }
    if (!force && !tracker.needsLinting(filename)) {
      LintProblemsBarManager.getInstance().updateProblemsBar(filename);
      deferredGenerations.remove(graphId);
      return;
    }

    int passGeneration = currentGeneration();
    HopGui hopGuiForSnapshot = HopGui.peekInstance();
    EditorSnapshot snapshot =
        snapshotOf(graph, hopGuiForSnapshot != null ? hopGuiForSnapshot.getVariables() : null);

    submit(
        () -> {
          // Taken before the check. A force refresh clears the database cache while this pass is
          // running, and the remarks from before that clear must not be published.
          int dbCacheGeneration = DbCache.getInstance().getGeneration();
          try {
            // Switched off while this pass was waiting or running: do not publish what it found.
            if (!isEnabled()) {
              return;
            }
            HopGui hopGui = HopGui.peekInstance();
            IHopMetadataProvider metadataProvider =
                hopGui != null ? hopGui.getMetadataProvider() : null;
            IVariables variables = hopGui != null ? hopGui.getVariables() : null;
            String normalizedPath = LintPathUtils.normalizePath(filename);

            List<LintResult> results =
                lintSnapshot(snapshot, normalizedPath, metadataProvider, variables);
            if (!publishChecked(filename, results, passGeneration, dbCacheGeneration)) {
              return;
            }
            Display display = graph.getDisplay();
            if (display != null && !display.isDisposed()) {
              display.asyncExec(
                  () -> LintProblemsBarManager.getInstance().updateProblemsBar(filename));
            }
            log.logDetailed("Linted open editor " + filename + ": " + results.size() + " issues");
          } catch (Exception e) {
            log.logError("Error linting open editor graph: " + e.getMessage(), e);
          } finally {
            deferredGenerations.remove(graphId);
          }
        });
  }

  /** The editor's unsaved content, captured as XML so a worker thread can parse its own copy. */
  private record EditorSnapshot(String xml, boolean pipeline, String name, String filename) {}

  /**
   * Capture the editor's current content. Must be called on the UI thread.
   *
   * <p>Hop refuses to clone a {@code PipelineMeta} — "a pipeline can't be cloned without building
   * new external references" — and points at serialising to XML instead. That suits this case: the
   * cheap half, writing the XML, happens on the UI thread where the model is stable, and the
   * expensive half, parsing it back, happens on the worker.
   *
   * <p>Returns null when nothing can be captured, and the caller falls back to the saved file.
   */
  private EditorSnapshot snapshotOf(HopGuiAbstractGraph graph, IVariables variables) {
    try {
      if (graph instanceof HopGuiPipelineGraph pipelineGraph
          && pipelineGraph.getPipelineMeta() != null) {
        PipelineMeta meta = pipelineGraph.getPipelineMeta();
        return new EditorSnapshot(meta.getXml(variables), true, meta.getName(), meta.getFilename());
      }
      if (graph instanceof HopGuiWorkflowGraph workflowGraph
          && workflowGraph.getWorkflowMeta() != null) {
        WorkflowMeta meta = workflowGraph.getWorkflowMeta();
        return new EditorSnapshot(
            meta.getXml(variables), false, meta.getName(), meta.getFilename());
      }
    } catch (Exception e) {
      // Serialising a half-built model mid-edit can fail. Linting the saved file is a worse
      // but still useful answer, and the next keystroke schedules another pass.
      log.logDetailed("Could not snapshot the open editor, linting the saved file: " + e);
    }
    return null;
  }

  private List<LintResult> lintSnapshot(
      EditorSnapshot snapshot,
      String normalizedPath,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    if (snapshot != null) {
      try {
        byte[] xml = snapshot.xml().getBytes(StandardCharsets.UTF_8);
        if (snapshot.pipeline()) {
          PipelineMeta pipelineMeta =
              new PipelineMeta(new ByteArrayInputStream(xml), metadataProvider, variables);
          restoreIdentity(pipelineMeta, snapshot);
          return PipelineLintResultsBuilder.build(
              pipelineMeta, normalizedPath, metadataProvider, variables);
        }
        WorkflowMeta workflowMeta =
            new WorkflowMeta(new ByteArrayInputStream(xml), metadataProvider, variables);
        restoreIdentity(workflowMeta, snapshot);
        return WorkflowLintResultsBuilder.lintWorkflowLikeVerify(
            workflowMeta, normalizedPath, metadataProvider, variables);
      } catch (Exception e) {
        log.logDetailed(
            "Could not parse the editor snapshot, linting the saved file instead: " + e);
      }
    }

    HopLinter linter = new HopLinter();
    return lintFileForGui(normalizedPath, linter, metadataProvider, variables);
  }

  /**
   * The XML carries the content but not always the name and filename, and rules read both — a
   * pipeline that lost its name would be reported as unnamed on every keystroke.
   */
  private void restoreIdentity(AbstractMeta meta, EditorSnapshot snapshot) {
    if (Utils.isEmpty(meta.getName()) && !Utils.isEmpty(snapshot.name())) {
      meta.setName(snapshot.name());
    }
    if (Utils.isEmpty(meta.getFilename()) && !Utils.isEmpty(snapshot.filename())) {
      meta.setFilename(snapshot.filename());
    }
  }

  /** Runs on a worker thread. */
  private void lintFileInternal(String filePath, String graphId, int passGeneration) {
    try {
      if (lintAndPublishFile(filePath, passGeneration)) {
        LintProblemsBarManager.getInstance().updateProblemsBar(filePath);
      }
    } catch (Exception e) {
      log.logError("Error linting file " + filePath + ": " + e.getMessage(), e);
    } finally {
      if (graphId != null) {
        deferredGenerations.remove(graphId);
      }
    }
  }

  /**
   * Lint a saved file and publish what it finds, unless the results were discarded since the pass
   * was scheduled. Touches no widget, so it can run without a display.
   *
   * @return false when the findings were dropped
   */
  boolean lintAndPublishFile(String filePath, int passGeneration) throws HopException {
    // Taken before the check. A force refresh clears the database cache while this pass is
    // running, and the remarks from before that clear must not be published.
    int dbCacheGeneration = DbCache.getInstance().getGeneration();
    HopGui hopGui = HopGui.peekInstance();
    IHopMetadataProvider metadataProvider = hopGui != null ? hopGui.getMetadataProvider() : null;
    IVariables variables = hopGui != null ? hopGui.getVariables() : null;

    HopLinter linter = new HopLinter();
    List<LintResult> results = lintFileForGui(filePath, linter, metadataProvider, variables);
    if (!publishChecked(filePath, results, passGeneration, dbCacheGeneration)) {
      return false;
    }
    log.logDetailed("Linted " + filePath + ": " + results.size() + " issues");
    return true;
  }

  private List<LintResult> lintFileForGui(
      String filePath,
      HopLinter linter,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    String normalizedPath = LintPathUtils.normalizePath(filePath);
    if (normalizedPath.toLowerCase().endsWith(".hpl")) {
      PipelineMeta pipelineMeta = new PipelineMeta(filePath, metadataProvider, variables);
      return PipelineLintResultsBuilder.build(
          pipelineMeta, normalizedPath, metadataProvider, variables);
    }
    return linter.lintFile(filePath, metadataProvider, variables);
  }

  /**
   * Hand work to the pool without losing the session it belongs to.
   *
   * <p>Everything the work then reaches for - the HopGui of the person who asked, its metadata
   * provider, the editors to report into - belongs to a RAP {@code UISession} in Hop Web, and a
   * pooled thread has none of its own: the lookup fails with "Invalid thread access" and the lint
   * dies before it starts. {@link BackgroundThreadFacade} carries the session of the thread that
   * schedules the work over to the thread that runs it, and is a no-op on the desktop.
   *
   * <p>Call this from the UI thread: that is where the session is read.
   */
  private static void submit(Runnable work) {
    executor.submit(BackgroundThreadFacade.bind(work));
  }

  private boolean includeMetadataInGuiLint() {
    try {
      return LinterConfigPlugin.getInstance().isPreCommitIncludeMetadata();
    } catch (Exception e) {
      return true;
    }
  }
}
