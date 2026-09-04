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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lint.registry.EffectiveRuleSet;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * Main engine class that orchestrates the entire linting process. This class loads configuration,
 * registers rules, and executes linting on Hop projects.
 */
public class HopLinter {

  private static final ILogChannel log = LogChannel.GENERAL;

  private LinterConfig config;
  private EffectiveRuleSet effectiveRuleSet;

  /**
   * Rules are not registered in code: they are resolved on demand from the rule registry, which
   * merges the installed YAML packs with the project's own hop-lint.yml.
   */
  public HopLinter() {
    // Nothing to set up; see ensureEffectiveRuleSet().
  }

  /** Load configuration from the configuration plugin */
  public void loadConfigFromPlugin() {
    log.logBasic("Loading linter configuration from rule registry");
    try {
      LinterConfigPlugin configPlugin = LinterConfigPlugin.getInstance();
      if (!configPlugin.isLinterEnabled()) {
        log.logBasic("Linter is disabled in configuration");
        this.config = new LinterConfig();
        this.effectiveRuleSet = new EffectiveRuleSet(List.of(), this.config);
        return;
      }
      applyEffectiveRuleSet(RuleRegistry.getInstance().resolveForCurrentProject());
    } catch (Exception e) {
      log.logError(
          "Error loading configuration from plugin, falling back to registry: " + e.getMessage(),
          e);
      loadDefaultConfig();
    }
  }

  /** Load default configuration when no config file is found */
  public void loadDefaultConfig() {
    log.logBasic("Loading default linter configuration from rule registry");
    applyEffectiveRuleSet(RuleRegistry.getInstance().resolve(null));
  }

  /**
   * Load configuration from a YAML file
   *
   * @param filePath Path to the hop-lint.yml configuration file
   * @throws IOException if the file cannot be read or parsed
   */
  public void loadConfig(String filePath) throws IOException {
    File configFile = new File(filePath);
    if (!configFile.exists()) {
      log.logBasic("Configuration file not found: " + filePath + ". Using rule packs only.");
      applyEffectiveRuleSet(RuleRegistry.getInstance().resolve(null));
      return;
    }
    try {
      applyEffectiveRuleSet(RuleRegistry.getInstance().resolve(configFile));
      log.logBasic("Loaded configuration from: " + filePath);
    } catch (Exception e) {
      log.logError("Failed to load configuration from: " + filePath, e);
      throw new IOException("Failed to load configuration from: " + filePath, e);
    }
  }

  private void applyEffectiveRuleSet(EffectiveRuleSet resolved) {
    this.effectiveRuleSet = resolved;
    this.config = resolved.getConfig();
    log.logBasic(
        "Effective lint rules loaded: "
            + resolved.getRules().size()
            + " ("
            + resolved.getEnabledRules().size()
            + " enabled)");
  }

  private void ensureEffectiveRuleSet() {
    if (effectiveRuleSet == null) {
      applyEffectiveRuleSet(RuleRegistry.getInstance().resolve(null));
    }
  }

  /**
   * Run the linter on a Hop project
   *
   * @param projectPath Path to the Hop project directory
   * @return List of all lint results found
   */
  public List<LintResult> run(String projectPath) {
    return run(projectPath, null, null);
  }

  /**
   * Run the linter on a Hop project with metadata provider and variables
   *
   * @param projectPath Path to the Hop project directory
   * @param metadataProvider The metadata provider (can be null)
   * @param variables The variables (can be null, will create default)
   * @return List of all lint results found
   */
  public List<LintResult> run(
      String projectPath, IHopMetadataProvider metadataProvider, IVariables variables) {
    return run(projectPath, metadataProvider, variables, null);
  }

  /**
   * Run the linter on a Hop project with metadata provider, variables, and progress callback
   *
   * @param projectPath Path to the Hop project directory
   * @param metadataProvider The metadata provider (can be null)
   * @param variables The variables (can be null, will create default)
   * @param progressCallback Callback to report progress (can be null)
   * @return List of all lint results found
   */
  public List<LintResult> run(
      String projectPath,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ProgressCallback progressCallback) {
    List<LintResult> allResults = new ArrayList<>();
    long startTime = System.currentTimeMillis();

    try {
      log.logBasic("Starting linter with project path: " + projectPath);

      // Try to load configuration in order of preference:
      // 1. Project-specific hop-lint.yml file
      // 2. hop-lint.yml in parent directories of the project
      // 3. Configuration plugin settings
      // 4. Default configuration
      loadConfigurationForContext(new File(projectPath));

      // Find all .hpl and .hwf files in the project
      if (progressCallback != null) {
        progressCallback.updateProgress("Discovering files...", 0, 100);
      }

      long fileDiscoveryStart = System.currentTimeMillis();
      List<String> hopFilePaths = findLintableFiles(projectPath, includeMetadataInProjectLint());
      long fileDiscoveryTime = System.currentTimeMillis() - fileDiscoveryStart;
      log.logBasic(
          "Found "
              + hopFilePaths.size()
              + " Hop files to analyze (discovery took "
              + fileDiscoveryTime
              + "ms)");

      // Create default variables if not provided
      if (variables == null) {
        log.logBasic("Variables is null, creating default variable space");
        variables = Variables.getADefaultVariableSpace();
      }

      // Process each file
      long fileProcessingStart = System.currentTimeMillis();
      int processedFiles = 0;

      for (String filePath : hopFilePaths) {
        File file = new File(filePath);
        // Check for cancellation
        if (progressCallback != null && progressCallback.isCancelled()) {
          log.logBasic("Linting cancelled by user");
          break;
        }

        try {
          long fileStart = System.currentTimeMillis();
          List<LintResult> fileResults = processFile(file, metadataProvider, variables);
          allResults.addAll(fileResults);
          processedFiles++;

          // Update progress
          if (progressCallback != null) {
            int progressPercent = (processedFiles * 100) / hopFilePaths.size();
            progressCallback.updateProgress(
                "Processing file: "
                    + file.getName()
                    + " ("
                    + processedFiles
                    + "/"
                    + hopFilePaths.size()
                    + ")",
                processedFiles,
                hopFilePaths.size());
          }

          long fileTime = System.currentTimeMillis() - fileStart;
          if (fileTime > 100) { // Log slow files
            log.logDetailed(
                "Processed file: "
                    + file.getName()
                    + " ("
                    + fileTime
                    + "ms, "
                    + fileResults.size()
                    + " issues)");
          }

          // Progress logging every 50 files
          if (processedFiles % 50 == 0) {
            log.logBasic(
                "Progress: " + processedFiles + "/" + hopFilePaths.size() + " files processed");
          }

        } catch (Exception e) {
          log.logError(
              "Error processing file: " + file.getAbsolutePath() + " - " + e.getMessage(), e);

          // Add an error result for the file that couldn't be processed
          allResults.add(
              new LintResult(
                  "SYSTEM-001",
                  "File Processing Error",
                  "ERROR",
                  "Failed to process file: " + e.getMessage(),
                  file.getName()));
        }
      }

      long fileProcessingTime = System.currentTimeMillis() - fileProcessingStart;

      long totalTime = System.currentTimeMillis() - startTime;
      log.logBasic(
          "Linting completed in "
              + totalTime
              + "ms. Found "
              + allResults.size()
              + " issues. File processing: "
              + fileProcessingTime
              + "ms");

      if (progressCallback != null) {
        progressCallback.setComplete("Linting completed. Found " + allResults.size() + " issues.");
      }

    } catch (Exception e) {
      log.logError("Error during linting process: " + e.getMessage(), e);
      allResults.add(
          new LintResult(
              "SYSTEM-002",
              "Configuration Error",
              "ERROR",
              "Failed to complete linting: " + e.getMessage(),
              "system"));
    }

    return allResults;
  }

  /**
   * Find all .hpl and .hwf files in the project directory
   *
   * @param projectPath Path to the project directory
   * @return List of Hop file paths as strings
   */
  public List<String> findHopFiles(String projectPath) {
    return findLintableFiles(projectPath, false);
  }

  /** Find Hop pipelines/workflows and optionally project metadata JSON files. */
  public List<String> findLintableFiles(String projectPath, boolean includeMetadata) {
    List<String> lintableFiles = new ArrayList<>();
    Path projectDir = Paths.get(projectPath).toAbsolutePath();
    LintPolicy policy = getPolicy();

    try (Stream<Path> paths = Files.walk(projectDir, 10)) {
      lintableFiles =
          paths
              .filter(Files::isRegularFile)
              .filter(
                  path -> {
                    String fileName = path.getFileName().toString().toLowerCase();
                    if (fileName.endsWith(".hpl") || fileName.endsWith(".hwf")) {
                      return true;
                    }
                    return includeMetadata
                        && HopMetadataFileLoader.isMetadataJsonFile(path.toString());
                  })
              .filter(path -> !path.toString().contains("/.git/"))
              .filter(path -> !path.toString().contains("/target/"))
              .filter(path -> !path.toString().contains("/node_modules/"))
              // The project's own exclude: patterns, so generated or vendored files stay out
              // of the run rather than being reported and then ignored by hand.
              .filter(path -> !policy.isExcluded(path.toString(), projectDir))
              .map(Path::toString)
              .collect(java.util.stream.Collectors.toList());
    } catch (IOException e) {
      log.logError("Error walking project directory: " + projectPath, e);
    }

    int excluded = policy.getExcludes().size();
    if (excluded > 0) {
      log.logDetailed(
          "Applied " + excluded + " exclude pattern(s) while discovering files to lint");
    }

    return lintableFiles;
  }

  /** The project's exclude and suppress configuration, empty when none is set. */
  public LintPolicy getPolicy() {
    ensureEffectiveRuleSet();
    return effectiveRuleSet.getPolicy();
  }

  /** Lint a single file, loading configuration from the project or defaults. */
  public List<LintResult> lintFile(
      String filePath, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    File file = new File(filePath);
    if (!file.exists()) {
      throw new HopException("File not found: " + filePath);
    }
    loadConfigForFile(file);
    ensureConfigLoaded();
    return processFile(file, metadataProvider, variables);
  }

  /**
   * Load linter configuration using the same resolution order as project lint.
   *
   * @param context A file or directory used to locate hop-lint.yml in parent folders
   */
  public void loadConfigurationForContext(File context) {
    try {
      try {
        LinterConfigPlugin configPlugin = LinterConfigPlugin.getInstance();
        if (!configPlugin.isLinterEnabled()) {
          this.config = new LinterConfig();
          this.effectiveRuleSet = new EffectiveRuleSet(List.of(), this.config);
          return;
        }
      } catch (Exception ignored) {
        // Plugin may not be initialized in CLI mode.
      }
      applyEffectiveRuleSet(RuleRegistry.getInstance().resolveForContext(context));
    } catch (Exception e) {
      log.logError("Error loading linter configuration: " + e.getMessage(), e);
      loadDefaultConfig();
    }
  }

  /** Load configuration for a file by searching upward for hop-lint.yml. */
  public void loadConfigForFile(File file) {
    loadConfigurationForContext(file);
  }

  private void ensureConfigLoaded() {
    ensureEffectiveRuleSet();
  }

  /** Process a single Hop file and run all enabled rules against it */
  public List<LintResult> processFile(
      File file, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    String fileName = LintPathUtils.normalizePath(file.getAbsolutePath());
    log.logDetailed("Processing file: " + file.getName());
    ensureConfigLoaded();

    if (HopMetadataFileLoader.isMetadataJsonFile(fileName)) {
      HopMetadataFileLoader.MetadataLoad load = HopMetadataFileLoader.read(file, metadataProvider);

      if (load.isFailure()) {
        // Report it. A metadata file that cannot be read passes every rule, so staying quiet
        // here would let a corrupt connection be reported as a clean one.
        return List.of(
            new LintResult(
                "SYSTEM-003",
                "Unreadable Metadata",
                "ERROR",
                "This metadata file could not be read, so no rule could be checked against"
                    + " it: "
                    + load.error(),
                fileName,
                LintSourceRef.metadata(HopMetadataFileLoader.metadataNameOf(file)),
                LintResult.Origin.LINT));
      }
      if (load.object() == null) {
        return List.of();
      }
      return finalizeResults(
          runPolicyRules(load.object(), fileName), null, fileName, variables, metadataProvider);
    }

    Object hopObject = loadHopObject(file, metadataProvider, variables, fileName);
    if (hopObject == null) {
      return List.of();
    }

    return finalizeResults(
        runPolicyRules(hopObject, fileName), hopObject, fileName, variables, metadataProvider);
  }

  /** Lint an already-loaded pipeline or workflow (used for open editor tabs). */
  public List<LintResult> lintHopObject(
      Object hopObject,
      String fileName,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    String normalizedPath = LintPathUtils.normalizePath(fileName);
    loadConfigurationForContext(new File(normalizedPath));
    ensureConfigLoaded();
    return finalizeResults(
        runPolicyRules(hopObject, normalizedPath),
        hopObject,
        normalizedPath,
        variables,
        metadataProvider);
  }

  /** Run YAML/custom policy rules against an already loaded pipeline or workflow object. */
  public List<LintResult> runPolicyRules(Object hopObject, String fileName) {
    return runPolicyRulesInternal(hopObject, fileName);
  }

  /**
   * Compute pipeline lint results the same way as Hop verify + {@link PipelineVerifyLintExtension}:
   * native {@code checkTransforms} remarks plus optional policy rules, then deduplicated.
   */
  public List<LintResult> lintPipelineLikeVerify(
      PipelineMeta pipelineMeta,
      String fileName,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    loadConfigurationForContext(new File(fileName));

    List<ICheckResult> remarks = new ArrayList<>();
    // Suppressed: this method adds the policy findings itself just below, so letting the verify
    // extension add them too would double every finding.
    LintVerifyReentrancy.runDrivenByLinter(
        () ->
            pipelineMeta.checkTransforms(
                remarks, false, new NullProgressMonitor(), variables, metadataProvider));

    if (shouldIncludeLintInPipelineVerify()) {
      remarks.addAll(
          LintCheckResultAdapter.toCheckResults(
              runPolicyRules(pipelineMeta, fileName), pipelineMeta));
    }

    return LintResultDeduplicator.deduplicate(
        LintCheckResultAdapter.fromCheckResults(remarks, fileName));
  }

  /** Compute workflow lint results the same way as workflow verify plus optional policy rules. */
  public List<LintResult> lintWorkflowLikeVerify(
      WorkflowMeta workflowMeta,
      String fileName,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    loadConfigurationForContext(new File(fileName));

    List<ICheckResult> remarks = new ArrayList<>();
    // Suppressed for the same reason as the pipeline path above.
    LintVerifyReentrancy.runDrivenByLinter(
        () ->
            workflowMeta.checkActions(
                remarks, false, new NullProgressMonitor(), variables, metadataProvider));

    if (shouldIncludeLintInWorkflowVerify()) {
      remarks.addAll(
          WorkflowCheckResultAdapter.toCheckResults(
              runPolicyRules(workflowMeta, fileName), workflowMeta));
    }

    return LintResultDeduplicator.deduplicate(
        LintCheckResultAdapter.fromCheckResults(remarks, fileName));
  }

  private boolean shouldIncludeLintInWorkflowVerify() {
    try {
      LinterConfigPlugin config = LinterConfigPlugin.getInstance();
      return config.isLinterEnabled() && config.isIncludeLintInWorkflowVerify();
    } catch (Exception e) {
      return true;
    }
  }

  private boolean shouldIncludeLintInPipelineVerify() {
    try {
      LinterConfigPlugin config = LinterConfigPlugin.getInstance();
      return config.isLinterEnabled() && config.isIncludeLintInPipelineVerify();
    } catch (Exception e) {
      return true;
    }
  }

  private static final class NullProgressMonitor implements IProgressMonitor {
    @Override
    public void beginTask(String task, int work) {}

    @Override
    public void done() {}

    @Override
    public void setTaskName(String name) {}

    @Override
    public void subTask(String name) {}

    @Override
    public boolean isCanceled() {
      return false;
    }

    @Override
    public void worked(int work) {}
  }

  private Object loadHopObject(
      File file, IHopMetadataProvider metadataProvider, IVariables variables, String fileName)
      throws HopException {
    try {
      if (fileName.toLowerCase().endsWith(".hpl")) {
        return new PipelineMeta(file.getAbsolutePath(), metadataProvider, variables);
      }
      if (fileName.toLowerCase().endsWith(".hwf")) {
        return new WorkflowMeta(variables, file.getAbsolutePath(), metadataProvider);
      }
      log.logError("Unknown file type: " + fileName);
      return null;
    } catch (Exception e) {
      log.logError("Error loading file: " + fileName, e);
      throw new HopException("Failed to load file: " + fileName, e);
    }
  }

  private List<LintResult> finalizeResults(
      List<LintResult> policyResults,
      Object hopObject,
      String fileName,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    List<LintResult> results = new ArrayList<>(policyResults);
    if (shouldIncludeNativeChecks() && hopObject != null) {
      results.addAll(
          HopNativeCheckRunner.runNativeChecks(hopObject, fileName, variables, metadataProvider));
    }
    // Suppressions are applied last, so they cover Hop's native remarks as well as policy
    // findings — a team accepting something should not have to care which produced it.
    return getPolicy()
        .applySuppressions(LintResultDeduplicator.deduplicate(results), projectRootFor(fileName));
  }

  /**
   * The directory that exclude and suppress patterns are relative to.
   *
   * <p>Patterns are written against project-relative paths so a configuration is portable between a
   * developer's machine and CI. The root is the folder holding the project's hop-lint.yml, which is
   * what the user means by "the project".
   */
  private Path projectRootFor(String fileName) {
    File configFile = RuleRegistry.getInstance().findProjectYaml(new File(fileName));
    if (configFile != null && configFile.getParentFile() != null) {
      return configFile.getParentFile().toPath().toAbsolutePath();
    }
    return null;
  }

  private boolean shouldIncludeNativeChecks() {
    try {
      return LinterConfigPlugin.getInstance().isIncludeNativeChecks();
    } catch (Exception e) {
      return true;
    }
  }

  private boolean includeMetadataInProjectLint() {
    try {
      return LinterConfigPlugin.getInstance().isPreCommitIncludeMetadata();
    } catch (Exception e) {
      return true;
    }
  }

  private List<LintResult> runPolicyRulesInternal(Object hopObject, String fileName) {
    List<LintResult> results = new ArrayList<>();
    ensureEffectiveRuleSet();
    try {
      for (CustomLintRule customRule : effectiveRuleSet.getEnabledRules()) {
        List<LintResult> customResults =
            CustomRuleExecutor.executeRule(customRule, hopObject, fileName);
        results.addAll(customResults);
        log.logDetailed(
            "Custom rule "
                + customRule.getName()
                + " found "
                + customResults.size()
                + " issues in "
                + fileName);

        if (customRule.getTarget() == RuleTarget.TRANSFORM && hopObject instanceof PipelineMeta) {
          PipelineMeta pipelineMeta = (PipelineMeta) hopObject;
          for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
            results.addAll(CustomRuleExecutor.executeRule(customRule, transformMeta, fileName));
          }
        }

        if (customRule.getTarget() == RuleTarget.ACTION && hopObject instanceof WorkflowMeta) {
          WorkflowMeta workflowMeta = (WorkflowMeta) hopObject;
          for (ActionMeta actionMeta : workflowMeta.getActions()) {
            results.addAll(CustomRuleExecutor.executeRule(customRule, actionMeta, fileName));
          }
        }

        if (customRule.getTarget() == RuleTarget.HOP && hopObject instanceof PipelineMeta) {
          PipelineMeta pipelineMeta = (PipelineMeta) hopObject;
          for (PipelineHopMeta pipelineHop : pipelineMeta.getPipelineHops()) {
            results.addAll(CustomRuleExecutor.executeRule(customRule, pipelineHop, fileName));
          }
        }

        if (customRule.getTarget() == RuleTarget.HOP && hopObject instanceof WorkflowMeta) {
          WorkflowMeta workflowMeta = (WorkflowMeta) hopObject;
          for (WorkflowHopMeta workflowHop : workflowMeta.getWorkflowHops()) {
            results.addAll(CustomRuleExecutor.executeRule(customRule, workflowHop, fileName));
          }
        }
      }
    } catch (Exception e) {
      log.logError("Error executing custom rules on file " + fileName + ": " + e.getMessage(), e);
    }
    return results;
  }

  public EffectiveRuleSet getEffectiveRuleSet() {
    ensureEffectiveRuleSet();
    return effectiveRuleSet;
  }

  /**
   * Generate a summary of results by rule
   *
   * @param results All lint results
   * @return Map of rule ID to summary info
   */
  public Map<String, RuleSummary> generateRuleSummary(List<LintResult> results) {
    Map<String, RuleSummary> summary = new HashMap<>();

    for (LintResult result : results) {
      String ruleId = result.getRuleId();
      RuleSummary ruleSummary = summary.computeIfAbsent(ruleId, k -> new RuleSummary(ruleId));

      if ("ERROR".equals(result.getSeverity())) {
        ruleSummary.incrementErrors();
      } else if ("WARNING".equals(result.getSeverity())) {
        ruleSummary.incrementWarnings();
      } else {
        ruleSummary.incrementOther();
      }
    }

    return summary;
  }

  /** Inner class to hold rule summary information */
  public static class RuleSummary {
    private final String ruleId;
    private int errors = 0;
    private int warnings = 0;
    private int other = 0;

    public RuleSummary(String ruleId) {
      this.ruleId = ruleId;
    }

    public void incrementErrors() {
      errors++;
    }

    public void incrementWarnings() {
      warnings++;
    }

    public void incrementOther() {
      other++;
    }

    public String getRuleId() {
      return ruleId;
    }

    public int getErrors() {
      return errors;
    }

    public int getWarnings() {
      return warnings;
    }

    public int getOther() {
      return other;
    }

    public int getTotal() {
      return errors + warnings + other;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(ruleId).append(": ").append(getTotal());

      if (errors > 0 || warnings > 0 || other > 0) {
        sb.append(" (");
        boolean first = true;
        if (errors > 0) {
          sb.append(errors).append(" error").append(errors > 1 ? "s" : "");
          first = false;
        }
        if (warnings > 0) {
          if (!first) sb.append(", ");
          sb.append(warnings).append(" warning").append(warnings > 1 ? "s" : "");
          first = false;
        }
        if (other > 0) {
          if (!first) sb.append(", ");
          sb.append(other).append(" other");
        }
        sb.append(")");
      }

      return sb.toString();
    }
  }

  // Database connection deduplication logic moved to custom rule executor
  // Connections are now checked via custom rules targeting DATABASE_CONNECTION

  /**
   * Get the current configuration
   *
   * @return Current linter configuration
   */
  public LinterConfig getConfig() {
    return config;
  }

  /**
   * Check if a rule is enabled
   *
   * @param ruleId The rule ID
   * @return true if the rule is enabled, false otherwise
   */
  public boolean isRuleEnabled(String ruleId) {
    if (config == null) {
      return false;
    }
    return config.isRuleEnabled(ruleId);
  }

  /**
   * Get parameters for a specific rule
   *
   * @param ruleId The rule ID
   * @return Map of rule parameters, or null if rule not found
   */
  public Map<String, Object> getRuleParameters(String ruleId) {
    if (config == null) {
      return null;
    }
    return config.getRuleParameters(ruleId);
  }
}
