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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * GUI plugin that integrates the Hop Linter into the Hop GUI. Adds a "Lint Project" menu item under
 * the Tools menu.
 */
@GuiPlugin(id = "HopLintCheckerGuiPlugin", description = "Hop Lint Checker GUI Plugin")
public class LinterGuiPlugin {

  private static final Class<?> PKG = LinterGuiPlugin.class; // for i18n purposes

  public static final String LINT_SUBMENU_ID = "lint-submenu";

  // Constructor to verify plugin loading
  public LinterGuiPlugin() {
    LogChannel.GENERAL.logBasic("Hop Lint Checker plugin loaded");
    LintCanvasOverlayRefresh.ensureRegistered();
  }

  private static final ILogChannel log = LogChannel.GENERAL;

  /** Create the Lint submenu in Tools */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = LINT_SUBMENU_ID,
      type = GuiMenuElementType.PARENT,
      label = "i18n::LinterGuiPlugin.Menu.Lint.Label",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "lint-check.svg",
      separator = true)
  public void createLintSubmenu() {
    // Submenu container only; items are registered separately below.
  }

  /** Add the "Lint Project" menu item to the Tools menu */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = "menu-tools-lint",
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::LinterGuiPlugin.Menu.LintProject.Label",
      parentId = LINT_SUBMENU_ID,
      image = "lint-check.svg")
  public static void lintProject() {
    // Add debug output at the very start
    LogChannel.GENERAL.logBasic("Lint Project menu item selected");
    HopGui hopGui = HopGui.peekInstance();

    try {
      // Null-safety check for HopGui
      if (hopGui == null) {
        LogChannel.GENERAL.logError("HopGui instance is null");
        return;
      }

      // Get the project path from variables
      IVariables variables = hopGui.getVariables();
      if (variables == null) {
        LogChannel.GENERAL.logError("Variables is null - using default variable space");
        variables = Variables.getADefaultVariableSpace();
      }

      String projectPath = variables.getVariable("PROJECT_HOME");
      String projectName = variables.getVariable("PROJECT_NAME");

      if (Utils.isEmpty(projectPath)) {
        // No project loaded, show error message
        MessageBox messageBox = new MessageBox(hopGui.getShell(), SWT.ICON_WARNING | SWT.OK);
        messageBox.setText(BaseMessages.getString(PKG, "LinterGuiPlugin.Dialog.NoProject.Title"));
        messageBox.setMessage(
            BaseMessages.getString(PKG, "LinterGuiPlugin.Dialog.NoProject.Message"));
        messageBox.open();
        return;
      }

      LogChannel.GENERAL.logBasic(
          "Starting lint check for project: " + (projectName != null ? projectName : "Unknown"));
      LogChannel.GENERAL.logBasic("Project path: " + projectPath);

      // Create progress dialog
      LinterProgressDialog progressDialog = new LinterProgressDialog(hopGui.getShell());

      // Make variables final for lambda
      final String finalProjectPath = projectPath;
      final IVariables finalVariables = variables;
      final HopGui finalHopGui = hopGui;
      final String finalProjectName = projectName != null ? projectName : "Unknown Project";

      // Run linter in background thread
      BackgroundThreadFacade.start(
          () -> {
            try {
              long lintStartTime = System.currentTimeMillis();
              HopLinter linter = new HopLinter();
              IHopMetadataProvider metadataProvider = finalHopGui.getMetadataProvider();

              List<LintResult> results =
                  linter.run(finalProjectPath, metadataProvider, finalVariables, progressDialog);
              long lintEndTime = System.currentTimeMillis();
              long totalLintTime = lintEndTime - lintStartTime;

              // Check if cancelled
              if (progressDialog.isCancelled()) {
                LogChannel.GENERAL.logBasic("Linting cancelled by user");
                return;
              }

              // Generate rule summary
              Map<String, HopLinter.RuleSummary> ruleSummary = linter.generateRuleSummary(results);

              // Process and display results on UI thread
              Display.getDefault()
                  .asyncExec(
                      () -> {
                        try {
                          // Update results manager for GUI integration
                          LintResultsManager.getInstance().updateResults(results);
                          LintProblemsBarManager.getInstance().refreshAllOpenEditors();

                          LinterGuiPlugin plugin = new LinterGuiPlugin();
                          plugin.displayResults(
                              finalHopGui, results, ruleSummary, totalLintTime, finalProjectName);
                        } catch (Exception e) {
                          LogChannel.GENERAL.logError(
                              "Error displaying results: " + e.getMessage(), e);
                        }
                      });

            } catch (Exception e) {
              LogChannel.GENERAL.logError("Error in linter thread: " + e.getMessage(), e);
              Display.getDefault()
                  .asyncExec(
                      () -> {
                        if (finalHopGui != null && finalHopGui.getShell() != null) {
                          new ErrorDialog(
                              finalHopGui.getShell(),
                              "Linting Error",
                              "An error occurred while running the linter: " + e.getMessage(),
                              e);
                        }
                      });
            } finally {
              // Ensure progress dialog closes
              Display.getDefault()
                  .asyncExec(
                      () -> {
                        if (!progressDialog.isComplete()) {
                          progressDialog.close();
                        }
                      });
            }
          },
          "HopLinter-Project");

      // Show the progress dialog while the linter runs
      progressDialog.open();

    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error during linting process: " + e.getMessage(), e);

      // Show error dialog with more details
      if (hopGui != null && hopGui.getShell() != null) {
        new ErrorDialog(
            hopGui.getShell(),
            "Linting Error",
            "An error occurred while running the linter: " + e.getMessage(),
            e);
      }
    }
  }

  /**
   * Display the linting results to the user
   *
   * @param hopGui The HopGui instance
   * @param results List of lint results
   * @param ruleSummary Summary of results by rule
   * @param executionTimeMs Total execution time in milliseconds
   * @param projectName Name of the project that was linted
   */
  private void displayResults(
      HopGui hopGui,
      List<LintResult> results,
      Map<String, HopLinter.RuleSummary> ruleSummary,
      long executionTimeMs,
      String projectName) {
    // Write detailed results to file instead of console
    String projectPath = writeResultsToFile(results, ruleSummary, executionTimeMs, projectName);

    // Log summary to the Hop log
    LogChannel.GENERAL.logBasic("=== LINT RESULTS FOR PROJECT: " + projectName + " ===");
    LogChannel.GENERAL.logBasic("Execution time: " + formatExecutionTime(executionTimeMs));
    LogChannel.GENERAL.logBasic("Detailed results written to: " + projectPath);

    if (results.isEmpty()) {
      LogChannel.GENERAL.logBasic(
          "✓ No issues found! Your project follows all configured best practices.");
    } else {
      // Group results by severity
      Map<String, List<LintResult>> resultsBySeverity =
          results.stream().collect(Collectors.groupingBy(LintResult::getSeverity));

      int errorCount = resultsBySeverity.getOrDefault("ERROR", List.of()).size();
      int warningCount = resultsBySeverity.getOrDefault("WARNING", List.of()).size();

      LogChannel.GENERAL.logBasic(
          String.format(
              "Found %d issues (%d errors, %d warnings)",
              results.size(), errorCount, warningCount));
    }

    LogChannel.GENERAL.logBasic("=== END LINT RESULTS ===");

    if (results.isEmpty()) {
      LintResultsUi.logSummary(results, projectName);
    } else {
      LintResultsUi.logSummary(results, projectName);
    }
  }

  /**
   * Write detailed lint results to a file in the project root
   *
   * @param results All lint results
   * @param ruleSummary Summary by rule
   * @param executionTimeMs Execution time
   * @param projectName Project name
   * @return Path to the results file
   */
  private String writeResultsToFile(
      List<LintResult> results,
      Map<String, HopLinter.RuleSummary> ruleSummary,
      long executionTimeMs,
      String projectName) {
    try {
      // Get project path from HopGui variables
      HopGui hopGui = HopGui.peekInstance();
      IVariables variables = hopGui.getVariables();
      String projectPath = variables.getVariable("PROJECT_HOME");

      if (Utils.isEmpty(projectPath)) {
        // Fallback to current working directory
        projectPath = System.getProperty("user.dir");
      }

      // Create results file with timestamp
      String timestamp =
          java.time.LocalDateTime.now()
              .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
      String fileName = "hop-lint-results_" + timestamp + ".txt";
      java.io.File resultsFile = new java.io.File(projectPath, fileName);

      // Write results to file
      try (java.io.PrintWriter writer =
          new java.io.PrintWriter(
              new java.io.FileWriter(resultsFile, java.nio.charset.StandardCharsets.UTF_8))) {

        writer.println("=== HOP LINT CHECKER RESULTS ===");
        writer.println("Project: " + projectName);
        writer.println("Execution time: " + formatExecutionTime(executionTimeMs));
        writer.println(
            "Generated: "
                + java.time.LocalDateTime.now()
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        writer.println();

        // Write rule summary
        writer.println("RULE SUMMARY:");
        ruleSummary.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> writer.println("  " + entry.getValue().toString()));
        writer.println();

        if (results.isEmpty()) {
          writer.println("✓ No issues found! Your project follows all configured best practices.");
        } else {
          // Group results by severity
          Map<String, List<LintResult>> resultsBySeverity =
              results.stream().collect(Collectors.groupingBy(LintResult::getSeverity));

          int errorCount = resultsBySeverity.getOrDefault("ERROR", List.of()).size();
          int warningCount = resultsBySeverity.getOrDefault("WARNING", List.of()).size();

          writer.println("SUMMARY:");
          writer.println("  Total issues: " + results.size());
          writer.println("  Errors: " + errorCount);
          writer.println("  Warnings: " + warningCount);
          writer.println();

          // Write detailed results
          writer.println("DETAILED RESULTS:");
          writer.println();

          // Write errors first
          List<LintResult> errors = resultsBySeverity.get("ERROR");
          if (errors != null && !errors.isEmpty()) {
            writer.println("ERRORS:");
            for (LintResult error : errors) {
              writer.println(
                  "  [ERROR] "
                      + error.getRuleId()
                      + " - "
                      + error.getRuleName()
                      + ": "
                      + error.getMessage()
                      + " (File: "
                      + error.getFileName()
                      + ")");
            }
            writer.println();
          }

          // Write warnings
          List<LintResult> warnings = resultsBySeverity.get("WARNING");
          if (warnings != null && !warnings.isEmpty()) {
            writer.println("WARNINGS:");
            for (LintResult warning : warnings) {
              writer.println(
                  "  [WARNING] "
                      + warning.getRuleId()
                      + " - "
                      + warning.getRuleName()
                      + ": "
                      + warning.getMessage()
                      + " (File: "
                      + warning.getFileName()
                      + ")");
            }
            writer.println();
          }

          // Write any other severity levels
          for (Map.Entry<String, List<LintResult>> entry : resultsBySeverity.entrySet()) {
            if (!"ERROR".equals(entry.getKey()) && !"WARNING".equals(entry.getKey())) {
              writer.println(entry.getKey() + ":");
              for (LintResult result : entry.getValue()) {
                writer.println(
                    "  ["
                        + result.getSeverity()
                        + "] "
                        + result.getRuleId()
                        + " - "
                        + result.getRuleName()
                        + ": "
                        + result.getMessage()
                        + " (File: "
                        + result.getFileName()
                        + ")");
              }
              writer.println();
            }
          }
        }

        writer.println("=== END LINT RESULTS ===");
      }

      return resultsFile.getAbsolutePath();

    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error writing results to file: " + e.getMessage(), e);
      return "Error writing to file: " + e.getMessage();
    }
  }

  /**
   * Format execution time in a human-readable format
   *
   * @param timeMs Time in milliseconds
   * @return Formatted time string
   */
  private String formatExecutionTime(long timeMs) {
    if (timeMs < 1000) {
      return timeMs + "ms";
    } else if (timeMs < 60000) {
      return String.format("%.1fs", timeMs / 1000.0);
    } else {
      long minutes = timeMs / 60000;
      long seconds = (timeMs % 60000) / 1000;
      return String.format("%dm %ds", minutes, seconds);
    }
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = "manage-custom-rules",
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::LinterGuiPlugin.Menu.ManageRules.Label",
      parentId = LINT_SUBMENU_ID,
      image = "lint-check.svg")
  public static void manageCustomRules() {
    try {
      HopGui hopGui = HopGui.peekInstance();
      if (hopGui == null) {
        LogChannel.GENERAL.logError("HopGui instance not available");
        return;
      }

      // Get the configuration plugin instance to access the rules
      LinterConfigPlugin configPlugin = LinterConfigPlugin.getInstance();

      // Load custom rules from the project's hop-lint.yml file first
      loadCustomRulesFromProject(hopGui, configPlugin);

      // Create and show the rule management dialog
      Shell shell = hopGui.getShell();
      RuleManagerDialog dialog =
          new RuleManagerDialog(
              shell,
              new java.util.ArrayList<>(
                  RuleRegistry.getInstance().resolveForCurrentProject().getRules()));
      dialog.open();

    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error opening rule manager: " + e.getMessage(), e);
    }
  }

  /** Load custom rules from the project's hop-lint.yml file into the configuration plugin */
  private static void loadCustomRulesFromProject(HopGui hopGui, LinterConfigPlugin configPlugin) {
    try {
      // Find the project's hop-lint.yml file
      String projectPath = null;
      if (hopGui.getVariables() != null) {
        projectPath = hopGui.getVariables().getVariable("PROJECT_HOME");
        if (Utils.isEmpty(projectPath)) {
          projectPath = hopGui.getVariables().getVariable("HOP_PROJECT_FOLDER");
        }
        if (Utils.isEmpty(projectPath)) {
          projectPath = hopGui.getVariables().getVariable("PROJECT_FOLDER");
        }
      }

      if (Utils.isEmpty(projectPath)) {
        projectPath = System.getProperty("user.dir");
      }

      String configFilePath = projectPath + File.separator + "hop-lint.yml";
      File configFile = new File(configFilePath);

      if (configFile.exists()) {
        // Create a temporary HopLinter to load the configuration
        HopLinter tempLinter = new HopLinter();
        tempLinter.loadConfig(configFilePath);
      }

    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error loading custom rules from project: " + e.getMessage(), e);
    }
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = "show-lint-results",
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::LinterGuiPlugin.Menu.ShowResults.Label",
      parentId = LINT_SUBMENU_ID,
      image = "lint-check.svg")
  public static void showLintResults() {
    LintResultsUi.showResults();
  }
}
