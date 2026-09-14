/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lint;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;

/**
 * Accepting a lint finding from the canvas, on the transform or action it points at.
 *
 * <p>The decision is written to the project's hop-lint.yml, next to the exclusions and the rules,
 * and never into the pipeline or workflow: those are opened by people who do not have this plugin
 * installed, and bookkeeping for a plugin they do not run has no business in their files.
 *
 * <p>Two actions rather than one: a transform that is checked offers to stop checking it, and one
 * already marked offers to check it again. The state is visible in the menu, so nobody has to open
 * a dialog to find out which of the two they are looking at.
 */
@GuiPlugin(
    id = "HopLintSuppressGuiPlugin",
    description = "Ignore lint findings on a transform or action")
public class LintSuppressGuiPlugin {

  private static final Class<?> PKG = LintSuppressGuiPlugin.class; // for i18n purposes

  private static final String ACTION_IGNORE_TRANSFORM =
      "pipeline-graph-transform-10900-lint-ignore";
  private static final String ACTION_CHECK_TRANSFORM = "pipeline-graph-transform-10901-lint-check";
  private static final String ACTION_IGNORE_ACTION = "workflow-graph-action-10900-lint-ignore";
  private static final String ACTION_CHECK_ACTION = "workflow-graph-action-10901-lint-check";

  // ==================== PIPELINE TRANSFORMS ====================

  @GuiContextAction(
      id = ACTION_IGNORE_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::LintSuppressGuiPlugin.IgnoreTransform.Name",
      tooltip = "i18n::LintSuppressGuiPlugin.IgnoreTransform.Tooltip",
      image = "lint-check.svg",
      category = "Lint",
      categoryOrder = "8")
  public void ignoreTransform(HopGuiPipelineTransformContext context) {
    HopGuiPipelineGraph graph = context.getPipelineGraph();
    ignore(
        graph.getShell(),
        context.getTransformMeta().getName(),
        context.getPipelineMeta().getFilename(),
        LintSourceRef.Kind.TRANSFORM,
        graph);
  }

  @GuiContextAction(
      id = ACTION_CHECK_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::LintSuppressGuiPlugin.CheckTransform.Name",
      tooltip = "i18n::LintSuppressGuiPlugin.CheckTransform.Tooltip",
      image = "lint-check.svg",
      category = "Lint",
      categoryOrder = "8")
  public void checkTransformAgain(HopGuiPipelineTransformContext context) {
    HopGuiPipelineGraph graph = context.getPipelineGraph();
    checkAgain(
        graph.getShell(),
        context.getTransformMeta().getName(),
        context.getPipelineMeta().getFilename(),
        graph);
  }

  @GuiContextActionFilter(parentId = HopGuiPipelineTransformContext.CONTEXT_ID)
  public boolean filterTransformActions(
      String contextActionId, HopGuiPipelineTransformContext context) {
    return filter(
        contextActionId,
        context.getPipelineMeta().getFilename(),
        context.getTransformMeta().getName(),
        LintSourceRef.Kind.TRANSFORM,
        ACTION_IGNORE_TRANSFORM,
        ACTION_CHECK_TRANSFORM);
  }

  // ==================== WORKFLOW ACTIONS ====================

  @GuiContextAction(
      id = ACTION_IGNORE_ACTION,
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::LintSuppressGuiPlugin.IgnoreAction.Name",
      tooltip = "i18n::LintSuppressGuiPlugin.IgnoreAction.Tooltip",
      image = "lint-check.svg",
      category = "Lint",
      categoryOrder = "8")
  public void ignoreAction(HopGuiWorkflowActionContext context) {
    HopGuiWorkflowGraph graph = context.getWorkflowGraph();
    ignore(
        graph.getShell(),
        context.getActionMeta().getName(),
        context.getWorkflowMeta().getFilename(),
        LintSourceRef.Kind.ACTION,
        graph);
  }

  @GuiContextAction(
      id = ACTION_CHECK_ACTION,
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::LintSuppressGuiPlugin.CheckAction.Name",
      tooltip = "i18n::LintSuppressGuiPlugin.CheckAction.Tooltip",
      image = "lint-check.svg",
      category = "Lint",
      categoryOrder = "8")
  public void checkActionAgain(HopGuiWorkflowActionContext context) {
    HopGuiWorkflowGraph graph = context.getWorkflowGraph();
    checkAgain(
        graph.getShell(),
        context.getActionMeta().getName(),
        context.getWorkflowMeta().getFilename(),
        graph);
  }

  @GuiContextActionFilter(parentId = HopGuiWorkflowActionContext.CONTEXT_ID)
  public boolean filterWorkflowActions(
      String contextActionId, HopGuiWorkflowActionContext context) {
    return filter(
        contextActionId,
        context.getWorkflowMeta().getFilename(),
        context.getActionMeta().getName(),
        LintSourceRef.Kind.ACTION,
        ACTION_IGNORE_ACTION,
        ACTION_CHECK_ACTION);
  }

  // ==================== SHARED ====================

  /**
   * Offer the action that matches the state the element is in: accepting findings when there are
   * some to accept, taking that back when there is something to take back.
   *
   * <p>The state comes from the project configuration, read on the right-click. Asking the last
   * lint run instead made the menu depend on whether this session had happened to lint the file,
   * which is how "check this again" could appear once and then never come back.
   */
  private boolean filter(
      String contextActionId,
      String fileName,
      String elementName,
      LintSourceRef.Kind kind,
      String ignoreActionId,
      String checkActionId) {
    if (!ignoreActionId.equals(contextActionId) && !checkActionId.equals(contextActionId)) {
      return true;
    }

    String filePath = LintPathUtils.normalizePath(fileName);
    if (Utils.isEmpty(filePath) || Utils.isEmpty(elementName)) {
      return false;
    }

    HopLinter linter = new HopLinter();
    linter.loadConfigurationForContext(new File(filePath));
    if (linter.isExcluded(filePath)) {
      // The whole file is out of linting, so there is nothing to accept or to take back here.
      // Putting it back is the Explorer's job, on the file, where it was excluded.
      return false;
    }

    boolean marked = linter.isMarkedElement(filePath, elementName);
    if (ignoreActionId.equals(contextActionId)) {
      return !marked && !findingsFor(filePath, kind, elementName).isEmpty();
    }
    return marked;
  }

  private void ignore(
      Shell shell,
      String elementName,
      String fileName,
      LintSourceRef.Kind kind,
      HopGuiAbstractGraph graph) {
    String filePath = LintPathUtils.normalizePath(fileName);
    ProjectConfig config = projectConfigFor(shell, filePath);
    if (config == null) {
      return;
    }

    LintSuppressDialog.Suppression suppression =
        new LintSuppressDialog(shell, elementName, findingsFor(filePath, kind, elementName)).open();
    if (suppression == null) {
      return;
    }

    try {
      for (String ruleId : suppression.ruleIds()) {
        LintPolicyYamlWriter.addSuppression(
            config.yamlFile().toPath(),
            ruleId,
            config.relativePath(),
            elementName,
            suppression.reason());
      }
    } catch (IOException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LintSuppressGuiPlugin.WriteFailed.Title"),
          BaseMessages.getString(
              PKG, "LintSuppressGuiPlugin.WriteFailed.Message", config.yamlFile().getPath()),
          e);
      return;
    }
    relint(graph, filePath);
  }

  private void checkAgain(
      Shell shell, String elementName, String fileName, HopGuiAbstractGraph graph) {
    String filePath = LintPathUtils.normalizePath(fileName);
    ProjectConfig config = projectConfigFor(shell, filePath);
    if (config == null) {
      return;
    }
    try {
      int removed =
          LintPolicyYamlWriter.removeSuppressionsFor(
              config.yamlFile().toPath(), config.relativePath(), elementName);
      if (removed == 0) {
        // Accepted by an entry that names a pattern rather than this file, or one written by
        // hand: which entry to take out is a judgement call, so say where to look.
        showWarning(
            shell,
            BaseMessages.getString(PKG, "LintSuppressGuiPlugin.NothingRemoved.Title"),
            BaseMessages.getString(
                PKG,
                "LintSuppressGuiPlugin.NothingRemoved.Message",
                elementName,
                config.yamlFile().getPath()));
        return;
      }
    } catch (IOException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LintSuppressGuiPlugin.WriteFailed.Title"),
          BaseMessages.getString(
              PKG, "LintSuppressGuiPlugin.WriteFailed.Message", config.yamlFile().getPath()),
          e);
      return;
    }
    relint(graph, filePath);
  }

  /** The project configuration to write to, and the path of the file as it will be written. */
  private record ProjectConfig(File yamlFile, String relativePath) {}

  private ProjectConfig projectConfigFor(Shell shell, String filePath) {
    File yamlFile = ExplorerLintGuiPlugin.resolveProjectYaml(filePath);
    if (yamlFile == null || yamlFile.getParentFile() == null) {
      showWarning(
          shell,
          BaseMessages.getString(PKG, "LintSuppressGuiPlugin.NoProject.Title"),
          BaseMessages.getString(PKG, "LintSuppressGuiPlugin.NoProject.Message"));
      return null;
    }
    Path projectRoot = yamlFile.getParentFile().toPath().toAbsolutePath();
    String relativePath = LintPolicy.relativise(filePath, projectRoot);
    if (Utils.isEmpty(relativePath) || relativePath.equals(filePath)) {
      // Outside the project: an absolute path in a portable configuration would only work here.
      showWarning(
          shell,
          BaseMessages.getString(PKG, "LintSuppressGuiPlugin.OutsideProject.Title"),
          BaseMessages.getString(
              PKG, "LintSuppressGuiPlugin.OutsideProject.Message", projectRoot.toString()));
      return null;
    }
    return new ProjectConfig(yamlFile, relativePath);
  }

  /** The configuration changed, so what is on screen is stale until the file is linted again. */
  private void relint(HopGuiAbstractGraph graph, String filePath) {
    BackgroundLintService.getInstance().getTracker().invalidate(filePath);
    if (graph != null) {
      BackgroundLintService.getInstance().scheduleGraphLint(graph, true);
    }
    LintCanvasOverlayRefresh.redrawOpenGraphs();
  }

  private void showWarning(Shell shell, String title, String message) {
    MessageBox box = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
    box.setText(title);
    box.setMessage(message);
    box.open();
  }

  private List<LintResult> findingsFor(
      String filePath, LintSourceRef.Kind kind, String elementName) {
    if (Utils.isEmpty(filePath) || Utils.isEmpty(elementName)) {
      return List.of();
    }
    Map<String, List<LintResult>> byName =
        LintResultsManager.getInstance().getOverlayIndex(filePath, kind);
    List<LintResult> findings = byName.get(elementName);
    return findings == null ? List.of() : findings;
  }
}
