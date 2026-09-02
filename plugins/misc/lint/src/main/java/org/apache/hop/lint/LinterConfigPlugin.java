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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lint.registry.ProjectLintYamlExporter;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

/**
 * Configuration plugin for the Hop Linter. The options here are surfaced in the Configuration
 * perspective; the lint rules themselves are managed via Tools &rarr; Lint &rarr; Manage Custom
 * Rules.
 */
@ConfigPlugin(id = "linter-config", description = "Configure linter rules and settings")
@GuiPlugin(description = "Linter Configuration GUI")
public class LinterConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final ILogChannel log = LogChannel.GENERAL;

  // Option keys in hop-config.json. Prefixed so they cannot collide with another plugin's.
  private static final String KEY_ENABLED = "LinterEnabled";
  private static final String KEY_LINT_ON_EDIT = "LinterLintOnEdit";
  private static final String KEY_SHOW_PROBLEMS_BAR = "LinterShowProblemsBar";
  private static final String KEY_CONFIG_FILE = "LinterConfigFile";
  private static final String KEY_PRE_COMMIT = "LinterPreCommitEnabled";
  private static final String KEY_PRE_COMMIT_WARNINGS = "LinterPreCommitBlockWarnings";
  private static final String KEY_PRE_COMMIT_METADATA = "LinterPreCommitIncludeMetadata";
  private static final String KEY_PIPELINE_VERIFY = "LinterIncludeInPipelineVerify";
  private static final String KEY_WORKFLOW_VERIFY = "LinterIncludeInWorkflowVerify";
  private static final String KEY_NATIVE_CHECKS = "LinterIncludeNativeChecks";

  /**
   * Read the current settings.
   *
   * <p>Each call builds an instance from {@code hop-config.json} rather than handing out a cached
   * object. The previous singleton was never the instance the configuration UI edited — Hop
   * constructs its own through the plugin registry, and nothing ever assigned {@code instance =
   * this} — so every runtime caller read a fresh object holding nothing but defaults. That made all
   * of these options inert, and they did not survive a restart either.
   */
  public static LinterConfigPlugin getInstance() {
    return new LinterConfigPlugin();
  }

  /** Populate this instance from the persisted Hop configuration. */
  private void loadFromHopConfig() {
    try {
      linterEnabled = HopConfig.readOptionBoolean(KEY_ENABLED, true);
      lintOnEditEnabled = HopConfig.readOptionBoolean(KEY_LINT_ON_EDIT, true);
      showProblemsBarEnabled = HopConfig.readOptionBoolean(KEY_SHOW_PROBLEMS_BAR, true);
      configFilePath = HopConfig.readOptionString(KEY_CONFIG_FILE, "");
      preCommitLintEnabled = HopConfig.readOptionBoolean(KEY_PRE_COMMIT, false);
      preCommitBlockWarnings = HopConfig.readOptionBoolean(KEY_PRE_COMMIT_WARNINGS, false);
      preCommitIncludeMetadata = HopConfig.readOptionBoolean(KEY_PRE_COMMIT_METADATA, true);
      includeLintInPipelineVerify = HopConfig.readOptionBoolean(KEY_PIPELINE_VERIFY, true);
      includeLintInWorkflowVerify = HopConfig.readOptionBoolean(KEY_WORKFLOW_VERIFY, true);
      includeNativeChecks = HopConfig.readOptionBoolean(KEY_NATIVE_CHECKS, true);
    } catch (Exception e) {
      // No readable configuration (a fresh install, or a CLI run outside a Hop home) simply
      // means the field defaults stand.
      log.logDetailed("Using default linter settings: " + e.getMessage());
    }
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to do: the widgets are filled from this instance's fields, which the constructor
    // has already loaded from hop-config.json.
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to do.
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
  }

  /**
   * Write what the user chose back into hop-config.json.
   *
   * <p>Without this the configuration perspective changed nothing: it fills the widgets from this
   * instance and then relies on this listener to save them. Every linter option was inert, and
   * reverted the moment the dialog closed.
   *
   * @param compositeWidgets the widgets holding the user's choices
   */
  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    for (String widgetId : compositeWidgets.getWidgetsMap().keySet()) {
      Control control = compositeWidgets.getWidgetsMap().get(widgetId);
      switch (widgetId) {
        case "linter-enabled" -> linterEnabled = ((Button) control).getSelection();
        case "linter-lint-on-edit" -> lintOnEditEnabled = ((Button) control).getSelection();
        case "linter-show-problems-bar" ->
            showProblemsBarEnabled = ((Button) control).getSelection();
        case "linter-config-file" -> configFilePath = ((TextVar) control).getText();
        case "linter-pre-commit-enabled" ->
            preCommitLintEnabled = ((Button) control).getSelection();
        case "linter-pre-commit-block-warnings" ->
            preCommitBlockWarnings = ((Button) control).getSelection();
        case "linter-pre-commit-include-metadata" ->
            preCommitIncludeMetadata = ((Button) control).getSelection();
        case "linter-include-in-pipeline-verify" ->
            includeLintInPipelineVerify = ((Button) control).getSelection();
        case "linter-include-in-workflow-verify" ->
            includeLintInWorkflowVerify = ((Button) control).getSelection();
        case "linter-include-native-checks" ->
            includeNativeChecks = ((Button) control).getSelection();
        default -> {
          // A widget this plugin does not own.
        }
      }
    }
    saveToHopConfig();
  }

  /**
   * Persist the current settings so they apply to later runs and survive a restart.
   *
   * <p>Only settings which actually have a value are written. The instance {@code hop conf} builds
   * is a fresh one holding just the options the user typed, so writing the unset ones would blank
   * every setting the user had chosen in the GUI.
   *
   * @return the settings that were written
   */
  public Map<String, Object> saveToHopConfig() {
    Map<String, Object> options = new HashMap<>();
    putIfSet(options, KEY_ENABLED, linterEnabled);
    putIfSet(options, KEY_LINT_ON_EDIT, lintOnEditEnabled);
    putIfSet(options, KEY_SHOW_PROBLEMS_BAR, showProblemsBarEnabled);
    putIfSet(options, KEY_CONFIG_FILE, Utils.isEmpty(configFilePath) ? null : configFilePath);
    putIfSet(options, KEY_PRE_COMMIT, preCommitLintEnabled);
    putIfSet(options, KEY_PRE_COMMIT_WARNINGS, preCommitBlockWarnings);
    putIfSet(options, KEY_PRE_COMMIT_METADATA, preCommitIncludeMetadata);
    putIfSet(options, KEY_PIPELINE_VERIFY, includeLintInPipelineVerify);
    putIfSet(options, KEY_WORKFLOW_VERIFY, includeLintInWorkflowVerify);
    putIfSet(options, KEY_NATIVE_CHECKS, includeNativeChecks);
    if (!options.isEmpty()) {
      HopConfig.saveOptions(options);
    }
    return options;
  }

  private static void putIfSet(Map<String, Object> options, String key, Object value) {
    if (value != null) {
      options.put(key, value);
    }
  }

  // Global linter settings
  @GuiWidgetElement(
      id = "linter-enabled",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.Enabled.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.Enabled.ToolTip")
  @CommandLine.Option(
      names = {"--lint-enabled"},
      description = "Enable or disable the linter (default: true)")
  private Boolean linterEnabled;

  @GuiWidgetElement(
      id = "linter-lint-on-edit",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.LintOnEdit.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.LintOnEdit.ToolTip")
  @CommandLine.Option(
      names = {"--lint-on-edit"},
      description = "Lint files while they are being edited (default: true)")
  private Boolean lintOnEditEnabled;

  @GuiWidgetElement(
      id = "linter-show-problems-bar",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.ShowIndicators.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.ShowIndicators.ToolTip")
  @CommandLine.Option(
      names = {"--lint-problems-bar"},
      description = "Show the lint problems bar (default: true)")
  private Boolean showProblemsBarEnabled;

  @GuiWidgetElement(
      id = "linter-config-file",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      variables = true,
      label = "i18n::LinterConfigPlugin.Option.ConfigFile.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.ConfigFile.ToolTip")
  @CommandLine.Option(
      names = {"--lint-config-file"},
      description = "Path to the hop-lint.yml the GUI should use")
  private String configFilePath = "";

  @GuiWidgetElement(
      id = "linter-pre-commit-enabled",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.PreCommit.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.PreCommit.ToolTip")
  @CommandLine.Option(
      names = {"--lint-block-commits"},
      description = "Block git commits from Hop Gui on lint failures (default: false)")
  private Boolean preCommitLintEnabled;

  @GuiWidgetElement(
      id = "linter-pre-commit-block-warnings",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.PreCommitWarnings.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.PreCommitWarnings.ToolTip")
  @CommandLine.Option(
      names = {"--lint-block-on-warnings"},
      description = "Block commits on warnings, not only errors (default: false)")
  private Boolean preCommitBlockWarnings;

  @GuiWidgetElement(
      id = "linter-pre-commit-include-metadata",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.PreCommitMetadata.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.PreCommitMetadata.ToolTip")
  @CommandLine.Option(
      names = {"--lint-commit-metadata"},
      description = "Lint metadata files when checking a commit (default: true)")
  private Boolean preCommitIncludeMetadata;

  @GuiWidgetElement(
      id = "linter-include-in-pipeline-verify",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.PipelineVerify.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.PipelineVerify.ToolTip")
  @CommandLine.Option(
      names = {"--lint-in-pipeline-verify"},
      description = "Add lint findings to pipeline Verify (default: true)")
  private Boolean includeLintInPipelineVerify;

  @GuiWidgetElement(
      id = "linter-include-in-workflow-verify",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.WorkflowVerify.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.WorkflowVerify.ToolTip")
  @CommandLine.Option(
      names = {"--lint-in-workflow-verify"},
      description = "Add lint findings to workflow Verify (default: true)")
  private Boolean includeLintInWorkflowVerify;

  @GuiWidgetElement(
      id = "linter-include-native-checks",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::LinterConfigPlugin.Option.NativeChecks.Label",
      toolTip = "i18n::LinterConfigPlugin.Option.NativeChecks.ToolTip")
  @CommandLine.Option(
      names = {"--lint-native-checks"},
      description = "Include Hop's own checks alongside lint findings (default: true)")
  private Boolean includeNativeChecks;

  public LinterConfigPlugin() {
    // Load here rather than only in getInstance(): the configuration perspective builds its own
    // instance through the plugin registry and fills the widgets straight from these fields, so an
    // unloaded instance showed every option at its default instead of what the user had chosen.
    // Lint rules themselves are resolved on demand from the RuleRegistry.
    loadFromHopConfig();
  }

  /**
   * Called by the configuration perspective when the user applies changes, and by {@code hop conf}
   * for every registered config plugin whether or not it was given any options.
   *
   * <p>Because {@code hop conf} calls this unconditionally, doing anything when nothing was set
   * would rewrite the linter settings on every unrelated {@code hop conf} command.
   */
  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    Map<String, Object> written = saveToHopConfig();
    if (written.isEmpty()) {
      return false;
    }
    if (!Utils.isEmpty(configFilePath)) {
      saveConfiguration();
    }
    log.logBasic("Linter configuration updated");
    return true;
  }

  // Getters and setters for configuration persistence

  public boolean isLinterEnabled() {
    return linterEnabled == null ? true : linterEnabled;
  }

  public void setLinterEnabled(boolean linterEnabled) {
    this.linterEnabled = linterEnabled;
  }

  public boolean isLintOnEditEnabled() {
    return lintOnEditEnabled == null ? true : lintOnEditEnabled;
  }

  public void setLintOnEditEnabled(boolean lintOnEditEnabled) {
    this.lintOnEditEnabled = lintOnEditEnabled;
  }

  public boolean isShowProblemsBarEnabled() {
    return showProblemsBarEnabled == null ? true : showProblemsBarEnabled;
  }

  public void setShowProblemsBarEnabled(boolean showProblemsBarEnabled) {
    this.showProblemsBarEnabled = showProblemsBarEnabled;
  }

  public boolean isPreCommitLintEnabled() {
    return preCommitLintEnabled == null ? false : preCommitLintEnabled;
  }

  public void setPreCommitLintEnabled(boolean preCommitLintEnabled) {
    this.preCommitLintEnabled = preCommitLintEnabled;
  }

  public boolean isPreCommitBlockWarnings() {
    return preCommitBlockWarnings == null ? false : preCommitBlockWarnings;
  }

  public void setPreCommitBlockWarnings(boolean preCommitBlockWarnings) {
    this.preCommitBlockWarnings = preCommitBlockWarnings;
  }

  public boolean isPreCommitIncludeMetadata() {
    return preCommitIncludeMetadata == null ? true : preCommitIncludeMetadata;
  }

  public void setPreCommitIncludeMetadata(boolean preCommitIncludeMetadata) {
    this.preCommitIncludeMetadata = preCommitIncludeMetadata;
  }

  public boolean isIncludeLintInPipelineVerify() {
    return includeLintInPipelineVerify == null ? true : includeLintInPipelineVerify;
  }

  public void setIncludeLintInPipelineVerify(boolean includeLintInPipelineVerify) {
    this.includeLintInPipelineVerify = includeLintInPipelineVerify;
  }

  public boolean isIncludeLintInWorkflowVerify() {
    return includeLintInWorkflowVerify == null ? true : includeLintInWorkflowVerify;
  }

  public void setIncludeLintInWorkflowVerify(boolean includeLintInWorkflowVerify) {
    this.includeLintInWorkflowVerify = includeLintInWorkflowVerify;
  }

  public boolean isIncludeNativeChecks() {
    return includeNativeChecks == null ? true : includeNativeChecks;
  }

  public void setIncludeNativeChecks(boolean includeNativeChecks) {
    this.includeNativeChecks = includeNativeChecks;
  }

  /** Minimum severity that blocks a pre-commit when {@link #isPreCommitLintEnabled()} is true. */
  public String getPreCommitFailOnSeverity() {
    return isPreCommitBlockWarnings() ? "WARNING" : "ERROR";
  }

  /** Returns true if a lint result at the given severity should block a commit. */
  public boolean shouldBlockCommitForSeverity(String severity) {
    if (Utils.isEmpty(severity)) {
      return false;
    }
    if ("ERROR".equalsIgnoreCase(severity)) {
      return true;
    }
    return isPreCommitBlockWarnings() && "WARNING".equalsIgnoreCase(severity);
  }

  public String getConfigFilePath() {
    return configFilePath;
  }

  public void setConfigFilePath(String configFilePath) {
    this.configFilePath = configFilePath;
  }

  public String getProjectPath() {
    if (configFilePath != null && !configFilePath.isEmpty()) {
      return new File(configFilePath).getParent();
    }

    // Try to get project path from Hop variables
    try {
      HopGui hopGui = HopGui.peekInstance();
      if (hopGui != null && hopGui.getVariables() != null) {
        String projectPath = hopGui.getVariables().getVariable("PROJECT_HOME");
        if (Utils.isEmpty(projectPath)) {
          projectPath = hopGui.getVariables().getVariable("HOP_PROJECT_FOLDER");
        }
        if (Utils.isEmpty(projectPath)) {
          projectPath = hopGui.getVariables().getVariable("PROJECT_FOLDER");
        }
        return projectPath;
      }
    } catch (Exception | LinkageError e) {
      // Outside the GUI there is no HopGui implementation on the classpath, and touching the
      // singleton raises ExceptionInInitializerError rather than an Exception. The CLI is a
      // first-class caller of this method, so an absent GUI is a normal case, not a failure.
      log.logDetailed("No Hop GUI available to resolve the project path: " + e);
    }

    return System.getProperty("user.dir");
  }

  /** The effective set of lint rules for the current project (YAML packs + project overrides). */
  public List<CustomLintRule> getCustomRules() {
    return new ArrayList<>(RuleRegistry.getInstance().resolveForCurrentProject().getRules());
  }

  /** Export project hop-lint.yml with pack overrides and project-local rules only. */
  public String exportToYaml(List<CustomLintRule> desiredRules) {
    return ProjectLintYamlExporter.export(desiredRules);
  }

  /** Export current effective rules using the rule manager list when available. */
  public String exportToYaml() {
    return exportToYaml(getCustomRules());
  }

  /** Save configuration to the specified file path */
  public boolean saveConfiguration() {
    return saveProjectRules(getCustomRules());
  }

  /** Save project hop-lint.yml from the rule manager's desired effective state. */
  public boolean saveProjectRules(List<CustomLintRule> desiredRules) {
    try {
      String yamlContent = exportToYaml(desiredRules);
      if (yamlContent == null) {
        return false;
      }
      String savePath = resolveProjectConfigPath();
      File parentDir = new File(savePath).getParentFile();
      if (parentDir != null && !parentDir.exists()) {
        parentDir.mkdirs();
      }
      java.nio.file.Files.write(
          java.nio.file.Paths.get(savePath),
          yamlContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      log.logBasic("Linter configuration saved to: " + savePath);
      return true;
    } catch (Exception e) {
      log.logError("Error saving linter configuration: " + e.getMessage(), e);
    }
    return false;
  }

  private String resolveProjectConfigPath() {
    if (!Utils.isEmpty(configFilePath)) {
      return configFilePath;
    }
    String projectPath = getProjectPath();
    return projectPath + File.separator + "hop-lint.yml";
  }
}
