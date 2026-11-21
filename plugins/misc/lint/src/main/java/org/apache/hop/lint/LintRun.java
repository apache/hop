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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lint.registry.EffectiveRuleSet;
import org.apache.hop.lint.registry.LintConfigurationException;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;

/**
 * One lint of a file or a folder, with the decision whether it fails.
 *
 * <p>{@code hop lint} and the Run Linter action both go through this, so the two cannot disagree
 * about the findings or about the outcome on the same project. What each does with the outcome —
 * print it, or hand it to the next action — is theirs.
 *
 * <p>Headless: nothing here touches Hop Gui.
 */
@Getter
@Setter
public class LintRun {

  /** The file or folder to lint. */
  private File target;

  /** A hop-lint.yml to use instead of the project's, or null. */
  private File configFile;

  /** Whether a folder run lints the metadata files in it too. */
  private boolean includeMetadata = true;

  /** The lowest severity reported; null reports everything. Never changes the outcome. */
  private LintSeverity.Level severityFilter;

  private LintSeverity.FailOn failOn = LintSeverity.FailOn.ERROR;

  /** Fail when more warnings than this are found; negative for no limit. */
  private int maxWarnings = -1;

  /** The findings already accepted, or null. */
  private LintBaseline baseline;

  /** Where {@link #baseline} was read from, for the report; null without a baseline. */
  private String baselineName;

  /** What a run found, and whether that fails it. */
  @Getter
  public static class Outcome {
    /** The findings, less those in the baseline. These decide the outcome. */
    private final List<LintResult> results;

    /** The findings reported: {@link #results} at the severity filter or above. */
    private final List<LintResult> shown;

    private final int baselineHidden;
    private final int baselineStale;

    /** Whether a finding reached the fail-on severity. */
    private final boolean failedOnSeverity;

    /** Whether the warnings exceeded the maximum. */
    private final boolean failedOnWarnings;

    /** The rules the run evaluated, after the project's hop-lint.yml. */
    private final EffectiveRuleSet ruleSet;

    /** The hop-lint.yml the rules were read with, or null when there was none. */
    private final File configFile;

    Outcome(
        List<LintResult> results,
        List<LintResult> shown,
        int baselineHidden,
        int baselineStale,
        boolean failedOnSeverity,
        boolean failedOnWarnings,
        EffectiveRuleSet ruleSet,
        File configFile) {
      this.ruleSet = ruleSet;
      this.configFile = configFile;
      this.results = results;
      this.shown = shown;
      this.baselineHidden = baselineHidden;
      this.baselineStale = baselineStale;
      this.failedOnSeverity = failedOnSeverity;
      this.failedOnWarnings = failedOnWarnings;
    }

    public boolean isFailed() {
      return failedOnSeverity || failedOnWarnings;
    }

    /** How many of the findings that decide the outcome have this severity. */
    public long count(LintSeverity.Level level) {
      return results.stream().filter(r -> level.name().equalsIgnoreCase(r.getSeverity())).count();
    }
  }

  /**
   * Lint the target.
   *
   * @param metadataProvider the metadata the target's files refer to
   * @param variables the variables to resolve them with
   * @throws LintConfigurationException when an installed rule pack or the project's hop-lint.yml
   *     cannot be read: a run that passes without those rules has passed for the wrong reason
   * @throws IOException when the configuration file is missing
   */
  public Outcome execute(IHopMetadataProvider metadataProvider, IVariables variables)
      throws Exception {
    if (target == null || !target.exists()) {
      throw new IllegalArgumentException("Target does not exist: " + target);
    }
    checkRulePacks();

    HopLinter linter = new HopLinter();
    linter.setIncludeMetadata(includeMetadata);
    linter.setProjectHome(variables == null ? null : variables.getVariable("PROJECT_HOME"));
    if (configFile != null) {
      if (!configFile.isFile()) {
        throw new IOException("Configuration file not found: " + configFile);
      }
      linter.loadConfig(configFile.getPath());
    } else {
      linter.loadConfigurationStrictly(target);
    }

    List<LintResult> all = lint(linter, metadataProvider, variables);
    File usedConfigFile =
        configFile != null
            ? configFile
            : RuleRegistry.getInstance()
                .findProjectYaml(
                    target,
                    variables == null ? null : variables.getVariable("PROJECT_HOME"),
                    false);

    List<LintResult> results = all;
    int hidden = 0;
    int stale = 0;
    if (baseline != null) {
      results = baseline.filter(all, baseDirectory());
      hidden = all.size() - results.size();
      stale = baseline.countStaleEntries(all, baseDirectory());
    }

    // The severity filter narrows what is reported, never what fails: deciding from the filtered
    // list would let "--severity WARNING" pass a project full of errors.
    long warnings = results.stream().filter(r -> "WARNING".equals(r.getSeverity())).count();
    return new Outcome(
        results,
        filterForDisplay(results, severityFilter),
        hidden,
        stale,
        meetsFailOn(results, failOn),
        maxWarnings >= 0 && warnings > maxWarnings,
        linter.getEffectiveRuleSet(),
        usedConfigFile);
  }

  private List<LintResult> lint(
      HopLinter linter, IHopMetadataProvider metadataProvider, IVariables variables)
      throws Exception {
    String targetPath = target.getPath();
    if (target.isFile()) {
      // A file in a project is judged against the whole project, so it can be reported as
      // called by nothing; outside one there is nothing to judge it against.
      CustomRuleExecutor.setProjectIndex(
          linter.buildProjectIndex(targetPath, metadataProvider, variables));
      try {
        return new ArrayList<>(linter.processFile(target, metadataProvider, variables));
      } finally {
        CustomRuleExecutor.setProjectIndex(null);
      }
    }
    // The run indexes the project's references itself, for the rules that need the project
    // as a whole: whether a pipeline is called by anything, whether a connection is used.
    return new ArrayList<>(linter.lintFolder(targetPath, metadataProvider, variables, null));
  }

  /**
   * Fail when an installed rule pack could not be loaded. Hop Gui carries on without it; a run that
   * passes without its rules has passed for the wrong reason.
   *
   * @throws LintConfigurationException naming each pack and what is wrong with it
   */
  public static void checkRulePacks() {
    List<String> packErrors = RuleRegistry.getInstance().getPackErrors();
    if (!packErrors.isEmpty()) {
      throw new LintConfigurationException(String.join("\n", packErrors));
    }
  }

  /**
   * The metadata a target's files refer to.
   *
   * <p>The project's when it applies to the target. Otherwise the {@code metadata/} folder the
   * target belongs to, so linting another project reads that project's connections, and the
   * standard metadata when there is none.
   *
   * @param projectMetadata the project's metadata, or null when no project applies to the target
   * @param variables {@code HOP_METADATA_FOLDER} is set on these when a folder is used
   */
  public static IHopMetadataProvider metadataProviderFor(
      File target, IVariables variables, IHopMetadataProvider projectMetadata) {
    if (projectMetadata != null) {
      return projectMetadata;
    }
    File metadataFolder = findMetadataFolder(target);
    if (metadataFolder == null) {
      return HopMetadataUtil.getStandardHopMetadataProvider(variables);
    }
    variables.setVariable(Const.HOP_METADATA_FOLDER, metadataFolder.getAbsolutePath());
    return new JsonMetadataProvider(Encr.getEncoder(), metadataFolder.getAbsolutePath(), variables);
  }

  /** Whether any finding reaches the fail-on threshold. */
  public static boolean meetsFailOn(List<LintResult> results, LintSeverity.FailOn failOn) {
    if (failOn == null || failOn == LintSeverity.FailOn.NONE) {
      return false;
    }
    return results.stream()
        .anyMatch(result -> LintSeverity.meetsFailOnThreshold(result.getSeverity(), failOn));
  }

  /**
   * The findings at the given severity or above: {@code WARNING} on a project with errors has to
   * show the errors. A severity this build does not know is shown rather than hidden.
   */
  public static List<LintResult> filterForDisplay(
      List<LintResult> results, LintSeverity.Level minimum) {
    if (minimum == null) {
      return results;
    }
    return results.stream().filter(result -> atOrAbove(result.getSeverity(), minimum)).toList();
  }

  private static boolean atOrAbove(String severity, LintSeverity.Level minimum) {
    for (LintSeverity.Level level : LintSeverity.Level.values()) {
      if (level.name().equalsIgnoreCase(severity)) {
        return level.ordinal() <= minimum.ordinal();
      }
    }
    return true;
  }

  /**
   * The project {@code metadata/} folder a target belongs to, found by walking up from it: point
   * the linter at a pipeline deep in a project and it still finds the project's connections.
   */
  public static File findMetadataFolder(File target) {
    File start = target.getAbsoluteFile();
    File directory = start.isDirectory() ? start : start.getParentFile();
    while (directory != null) {
      File candidate = new File(directory, "metadata");
      if (candidate.isDirectory()) {
        return candidate;
      }
      directory = directory.getParentFile();
    }
    return null;
  }

  /** Findings are reported relative to the lint target, so CI paths match the repository. */
  public Path baseDirectory() {
    return baseDirectoryOf(target);
  }

  public static Path baseDirectoryOf(File target) {
    if (target == null) {
      return null;
    }
    File directory = target.isDirectory() ? target : target.getAbsoluteFile().getParentFile();
    return directory != null ? directory.toPath().toAbsolutePath().normalize() : null;
  }

  /**
   * The report of a run in the given format, as {@code hop lint} writes it: the findings shown, and
   * in SARIF how the run was set up.
   */
  public String renderReport(Outcome outcome, LintReportFormat format)
      throws JsonProcessingException {
    return LintReportWriter.render(
        format, outcome.getShown(), toolVersion(), baseDirectory(), invocation(outcome));
  }

  /**
   * The report of a run's findings, without how the run was set up.
   *
   * @deprecated since 2.21; use {@link #renderReport(Outcome, LintReportFormat)}, which also writes
   *     the run's rules and set-up to SARIF
   */
  @Deprecated(since = "2.21")
  public String renderReport(List<LintResult> results, LintReportFormat format)
      throws JsonProcessingException {
    return LintReportWriter.render(format, results, toolVersion(), baseDirectory());
  }

  /**
   * A configuration or baseline file as a report records it: relative to the lint target's folder,
   * with {@code ../} where it lies outside it. A report is uploaded and kept, so it must not carry
   * the absolute path of a developer's home folder or a CI workspace. A local {@code file:} URL, as
   * the Run Linter action may be given, is read as the file it names; a remote VFS location is kept
   * as written.
   *
   * @param name the file as given, relative to the working directory or absolute
   * @param base the lint target's folder, or null
   */
  static String reportPath(String name, Path base) {
    if (Utils.isEmpty(name)) {
      return "";
    }
    Path path;
    try {
      if (name.regionMatches(true, 0, "file:", 0, 5)) {
        path = Paths.get(URI.create(name));
      } else if (name.matches("^[A-Za-z][A-Za-z0-9+.-]+://.*")) {
        return name;
      } else {
        path = Paths.get(name);
      }
      path = path.toAbsolutePath().normalize();
    } catch (Exception e) {
      // Not a path we can place: the file name tells the reader which file without the folders.
      int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
      return name.substring(slash + 1);
    }
    if (base == null || !Objects.equals(path.getRoot(), base.getRoot())) {
      return path.getFileName() == null ? "" : path.getFileName().toString();
    }
    return base.relativize(path).toString().replace('\\', '/');
  }

  /**
   * How this run was set up and how it ended: the rules it evaluated, the packs they came from, and
   * the settings that decided the findings and the outcome.
   */
  public LintInvocation invocation(Outcome outcome) {
    Map<String, Object> configuration = new LinkedHashMap<>();
    if (outcome.getConfigFile() != null) {
      configuration.put(
          "configFile", reportPath(outcome.getConfigFile().getAbsolutePath(), baseDirectory()));
    }
    // No filter reports everything, as INFO does: the Run Linter action defaults to INFO, hop lint
    // to no filter, and the two runs are the same.
    configuration.put(
        "severity",
        severityFilter == null ? LintSeverity.Level.INFO.name() : severityFilter.name());
    configuration.put("failOn", failOn == null ? LintSeverity.FailOn.NONE.name() : failOn.name());
    if (maxWarnings >= 0) {
      configuration.put("maxWarnings", maxWarnings);
    }
    configuration.put("includeMetadata", includeMetadata);
    if (baseline != null) {
      configuration.put("baseline", reportPath(baselineName, baseDirectory()));
      configuration.put("baselineHidden", outcome.getBaselineHidden());
    }
    List<CustomLintRule> enabled =
        outcome.getRuleSet() == null
            ? List.of()
            : outcome.getRuleSet().getEnabledRules().stream()
                .sorted(Comparator.comparing(CustomLintRule::generateRuleId))
                .toList();
    return new LintInvocation(
        true,
        outcome.isFailed() ? 1 : 0,
        enabled,
        RuleRegistry.getInstance().getLoadedPacks(),
        configuration);
  }

  /** The Hop version, as {@code hop --version} and {@code hop lint --version} print it. */
  public static String toolVersion() {
    String[] version = new HopVersionProvider().getVersion();
    if (version.length > 0 && !Utils.isEmpty(version[0])) {
      return version[0];
    }
    String lintVersion = LintRun.class.getPackage().getImplementationVersion();
    return lintVersion != null ? lintVersion : "development build";
  }
}
