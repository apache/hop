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
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.lint.registry.EffectiveRuleSet;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * {@code hop lint} - run the Hop linter over a file, a folder or a project.
 *
 * <p>Static analysis only: pipelines, workflows and metadata are read, never run, so this is safe
 * to use as a build step.
 */
@Getter
@Setter
@Command(
    name = "lint",
    mixinStandardHelpOptions = true,
    description =
        "Check pipelines, workflows and metadata against the lint rules. Exits 1 when a finding "
            + "reaches the --fail-on threshold (ERROR by default) or warnings exceed "
            + "--max-warnings.")
@HopCommand(id = "lint", description = "Check Hop files against the lint rules")
public class LintCommand implements Callable<Integer>, IHopCommand {

  // Deliberately no static ILogChannel field here. Touching LogChannel loads Hop's configuration
  // during class initialisation, which prints to stdout before main() gets a chance to run — and
  // that line lands at the top of an otherwise valid SARIF or JSON document.

  @Parameters(
      index = "0",
      arity = "0..1",
      paramLabel = "<file|directory>",
      description = "What to lint. Defaults to the current directory.")
  private String target;

  @Option(
      names = {"-v", "--verbose"},
      description = "Report in more detail")
  private boolean verbose;

  @Option(
      names = {"-q", "--quiet"},
      description = "Report findings only, leaving out the summary")
  private boolean quiet;

  @Option(
      names = {"-c", "--config"},
      paramLabel = "<file>",
      description = "Use this hop-lint.yml instead of the one in the project root")
  private String configFile;

  @Option(
      names = {"-o", "--output"},
      paramLabel = "<file>",
      description = "Write the report to this file instead of stdout")
  private String outputFile;

  @Option(
      names = {"-s", "--severity"},
      paramLabel = "<severity>",
      description =
          "Report only findings at this severity: ${COMPLETION-CANDIDATES}. Narrows the report "
              + "only; --fail-on decides the exit code.")
  private LintSeverity.Level severityFilter;

  @Option(
      names = "--fail-on",
      paramLabel = "<severity>",
      description = "Exit 1 when a finding reaches this severity. Default: ${DEFAULT-VALUE}.")
  private String failOn = "ERROR";

  @Option(
      names = {"-f", "--format"},
      paramLabel = "<format>",
      description = "Report format: ${COMPLETION-CANDIDATES}. Default: ${DEFAULT-VALUE}.")
  private LintReportFormat format = LintReportFormat.TEXT;

  @Option(
      names = "--max-warnings",
      paramLabel = "<n>",
      description = "Exit 1 when more than this many warnings are reported")
  private int maxWarnings = -1;

  @Option(
      names = {"-l", "--list-rules"},
      description = "Print the effective rule set, with the pack each rule came from, and exit")
  private boolean listRules;

  @Option(
      names = "--list-fields",
      paramLabel = "<plugin-id>",
      description =
          "Print the field names a rule can use for this transform or action plugin id, and exit")
  private String listFieldsFor;

  @Option(
      names = "--list-metadata-types",
      description = "Print the metadata type keys this installation has registered, and exit")
  private boolean listMetadataTypes;

  @Option(
      names = "--baseline",
      paramLabel = "<file>",
      description = "Report only findings which are not in this baseline")
  private String baselineFile;

  @Option(
      names = "--write-baseline",
      paramLabel = "<file>",
      description = "Record the current findings as accepted in this file, and exit")
  private String writeBaselineFile;

  @Option(
      names = "--install-hook",
      description = "Write a git pre-commit hook into .git/hooks, and exit")
  private boolean installHook;

  @Option(names = "--pre-commit", hidden = true, description = "Used by the generated git hook")
  private boolean preCommitMode;

  @Option(
      names = "--staged-file",
      hidden = true,
      paramLabel = "<file>",
      description = "Used by the generated git hook: the list of staged files to lint")
  private String stagedFileList;

  private CommandLine cmd;
  private IVariables commandVariables;
  private MultiMetadataProvider metadataProvider;

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider) {
    this.cmd = cmd;
    this.commandVariables = variables;
    this.metadataProvider = metadataProvider;
    // The hand-rolled parser upper-cased these, so "--severity warning" has to keep working.
    cmd.setCaseInsensitiveEnumValuesAllowed(true);
  }

  @Override
  public Integer call() {
    try {
      return run();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      if (verbose) {
        e.printStackTrace();
      }
      return 1;
    }
  }

  private int run() throws Exception {
    if (installHook) {
      installGitHook();
      return 0;
    }

    if (preCommitMode) {
      return runPreCommit();
    }

    if (!Utils.isEmpty(listFieldsFor)) {
      initializeHopEnvironment();
      return printFields(listFieldsFor);
    }

    if (listMetadataTypes) {
      initializeHopEnvironment();
      return printMetadataTypes();
    }

    if (listRules) {
      initializeHopEnvironment();
      printRuleList();
      return 0;
    }

    if (Utils.isEmpty(target)) {
      target = System.getProperty("target");
    }
    if (Utils.isEmpty(target)) {
      target = userDirectory();
    }

    // A machine-readable report piped to another tool has to be the only thing on stdout, and
    // Hop writes to stdout on its own account during start-up. Divert stdout to stderr for the
    // duration of the run and restore it just to emit the report.
    PrintStream stdout = System.out;
    boolean reportOwnsStdout = format != LintReportFormat.TEXT && outputFile == null;
    if (reportOwnsStdout) {
      System.setOut(System.err);
    }

    try {
      printRunHeader(target);
      initializeHopEnvironment();

      HopLinter linter = new HopLinter();
      loadConfiguration(linter, target);

      List<LintResult> results = applyBaseline(runLinting(linter, target));

      if (writeBaselineFile != null) {
        // Written before the severity filter: baselining a filtered run would mark
        // everything it hid as new on the next unfiltered one.
        Path baselinePath = Paths.get(writeBaselineFile);
        LintBaseline.write(baselinePath, results, reportBaseDirectory());
        if (reportOwnsStdout) {
          System.setOut(stdout);
        }
        System.out.println(
            "Recorded "
                + results.size()
                + " finding(s) as accepted in "
                + writeBaselineFile
                + ". Future runs report only what is new.");
        return 0;
      }

      // --severity narrows what is printed, never what fails the build. Deciding the exit
      // code from the filtered list would mean "--severity WARNING" exits 0 on a project full
      // of errors, which is exactly the sort of quiet pass a linter exists to prevent.
      int exitCode = exitCode(results);

      if (reportOwnsStdout) {
        System.setOut(stdout);
      }
      outputResults(filterForDisplay(results));
      return exitCode;
    } finally {
      System.setOut(stdout);
    }
  }

  /** Apply {@code --severity}, which narrows the report only. */
  private List<LintResult> filterForDisplay(List<LintResult> results) {
    if (severityFilter == null) {
      return results;
    }
    return results.stream()
        .filter(result -> severityFilter.name().equals(result.getSeverity()))
        .collect(Collectors.toList());
  }

  /**
   * Hide the findings a project has already accepted, so a run reports only what is new.
   *
   * <p>A missing baseline file is an error rather than an empty baseline: silently treating every
   * finding as new would fail a build for the wrong reason, and silently treating none as new would
   * pass one that should have failed.
   */
  private List<LintResult> applyBaseline(List<LintResult> results) throws IOException {
    if (baselineFile == null) {
      return results;
    }
    Path path = Paths.get(baselineFile);
    if (!Files.isRegularFile(path)) {
      throw new IOException(
          "Baseline file not found: " + baselineFile + ". Create it with --write-baseline.");
    }

    LintBaseline baseline = LintBaseline.read(path);
    List<LintResult> fresh = baseline.filter(results, reportBaseDirectory());

    if (!quiet) {
      int hidden = results.size() - fresh.size();
      int stale = baseline.countStaleEntries(results, reportBaseDirectory());
      StringBuilder note = new StringBuilder();
      note.append("Baseline: ").append(hidden).append(" accepted finding(s) hidden");
      if (stale > 0) {
        note.append(", ")
            .append(stale)
            .append(" baseline entr(y/ies) no longer occur and can be removed");
      }
      System.err.println(note + ".");
    }
    return fresh;
  }

  /**
   * Exit non-zero when the run should fail the build, either because a finding met the {@code
   * --fail-on} threshold or because warnings exceeded {@code --max-warnings}.
   */
  private int exitCode(List<LintResult> results) {
    if (shouldFail(results, LintSeverity.parseFailOn(failOn))) {
      return 1;
    }
    if (maxWarnings >= 0) {
      long warnings = results.stream().filter(r -> "WARNING".equals(r.getSeverity())).count();
      if (warnings > maxWarnings) {
        if (!quiet) {
          System.err.println(
              "Failing: " + warnings + " warnings exceeds --max-warnings " + maxWarnings + ".");
        }
        return 1;
      }
    }
    return 0;
  }

  private static int parseMaxWarnings(String value) {
    try {
      int parsed = Integer.parseInt(value.trim());
      if (parsed < 0) {
        throw new IllegalArgumentException("--max-warnings must be zero or greater");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("--max-warnings expects a number, got: " + value);
    }
  }

  /** Print the effective rule set, which is the merge of every installed pack plus overrides. */
  /**
   * Print the metadata keys this installation registers, which are the values a rule names in
   * {@code appliesTo} and the folder names under {@code metadata/}.
   *
   * <p>Worth having because guessing is easy to get wrong: Hop servers live under {@code server},
   * not {@code hop-server}, and a rule scoped to a key that does not exist matches nothing at all.
   */
  private int printMetadataTypes() {
    try {
      IVariables variables = Variables.getADefaultVariableSpace();
      String targetPath = Utils.isEmpty(target) ? userDirectory() : target;
      IHopMetadataProvider provider = resolveMetadataProvider(new File(targetPath), variables);
      if (provider == null) {
        System.err.println("No metadata provider available; cannot list metadata types.");
        return 1;
      }

      String[] keys = HopMetadataUtil.getHopMetadataKeys(provider);
      System.out.println("Metadata types registered (" + keys.length + "):");
      System.out.println();
      for (String key : keys) {
        System.out.println("  " + key);
      }
      System.out.println();
      System.out.println(
          "Use a key in a rule's appliesTo with target: METADATA. Files are read from"
              + " metadata/<key>/<name>.json.");
      return 0;
    } catch (Exception e) {
      System.err.println("Could not list metadata types: " + e.getMessage());
      return 1;
    }
  }

  /**
   * Print the field names a rule can use against a given transform or action plugin.
   *
   * <p>Worth having because the alternative is guessing: an unscoped rule naming a field which does
   * not exist simply matches nothing, which reads exactly like a clean result.
   *
   * @param pluginId the transform or action plugin id, as it appears in a rule's appliesTo
   * @return the exit code
   */
  private int printFields(String pluginId) {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin(TransformPluginType.class, pluginId);
    String kind = "transform";
    if (plugin == null) {
      plugin = registry.getPlugin(ActionPluginType.class, pluginId);
      kind = "action";
    }
    if (plugin == null) {
      System.err.println(
          "No transform or action plugin with id '"
              + pluginId
              + "'. The id is the one used in a rule's appliesTo.");
      return 1;
    }

    Object meta;
    try {
      meta = registry.loadClass(plugin);
    } catch (Exception e) {
      System.err.println("Could not load " + kind + " '" + pluginId + "': " + e.getMessage());
      return 1;
    }

    Map<String, String> byName = new TreeMap<>();
    for (Field field : CustomRuleExecutor.getAllFields(meta.getClass())) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      String serialised = CustomRuleExecutor.serialisedNameOf(field);
      String type = field.getType().getSimpleName();
      if (serialised != null) {
        byName.put(serialised, type);
      }
      // The Java field name resolves too, as a fallback, so list it when it differs. Only for
      // fields the plugin itself declares: listing the plumbing every meta inherits (log, lock,
      // parentTransformMeta) would bury the handful of names a rule actually wants.
      if (serialised != null || field.getDeclaringClass().equals(meta.getClass())) {
        byName.putIfAbsent(field.getName(), type);
      }
    }

    System.out.println(
        "Fields on " + kind + " '" + pluginId + "' (" + byName.size() + "), for targetField:");
    System.out.println();
    for (Map.Entry<String, String> entry : byName.entrySet()) {
      System.out.printf("  %-40s %s%n", entry.getKey(), entry.getValue());
    }
    System.out.println();
    System.out.println(
        "Nested values are reached with a dotted path, for example fileSettings.fileName.");
    return 0;
  }

  private void printRuleList() {
    // Resolve for the target, not the working directory: the effective set is what the project
    // being linted would use, and "why did my rule not fire" is the question this answers.
    String context = Utils.isEmpty(target) ? userDirectory() : target;
    EffectiveRuleSet ruleSet =
        configFile != null
            ? RuleRegistry.getInstance().resolve(new File(configFile))
            : RuleRegistry.getInstance().resolveForContext(new File(context));

    List<CustomLintRule> rules = new ArrayList<>(ruleSet.getRules());
    rules.sort(Comparator.comparing(CustomLintRule::generateRuleId));

    System.out.println("Effective lint rules (" + rules.size() + "):");
    System.out.println();
    for (CustomLintRule rule : rules) {
      System.out.printf(
          "  %-20s %-8s %-9s %-12s %s%n",
          rule.generateRuleId(),
          rule.isEnabled() ? "enabled" : "disabled",
          rule.getSeverity(),
          rule.getPackId(),
          rule.getName());
      if (verbose && !Utils.isEmpty(rule.getDescription())) {
        System.out.println("      " + rule.getDescription());
      }
    }
  }

  /** Version from the jar manifest, so it cannot drift from the build the way a literal does. */
  private static String toolVersion() {
    String version = LintCommand.class.getPackage().getImplementationVersion();
    return version != null ? version : "development build";
  }

  private int runPreCommit() throws Exception {
    String envFailOn = System.getenv("HOP_LINT_FAIL_ON");
    if (!Utils.isEmpty(envFailOn)) {
      failOn = envFailOn.trim().toUpperCase();
    }

    if (Utils.isEmpty(stagedFileList)) {
      throw new IllegalArgumentException("--pre-commit requires --staged-file <path>");
    }

    initializeHopEnvironment();
    List<File> stagedFiles = PreCommitLintService.readStagedFiles(stagedFileList);
    if (stagedFiles.isEmpty()) {
      if (!quiet) {
        System.out.println("No staged Hop files to lint.");
      }
      return 0;
    }

    HopLinter linter = new HopLinter();
    linter.loadConfigurationForContext(stagedFiles.get(0));

    // The lint target for path-relative purposes is the project the staged files live in.
    target = projectRootOf(stagedFiles.get(0));

    IVariables variables = Variables.getADefaultVariableSpace();
    // Without a metadata provider a pipeline will not load at all, and connection rules cannot
    // resolve — the hook would pass commits it should have blocked.
    IHopMetadataProvider metadataProvider = resolveMetadataProvider(stagedFiles.get(0), variables);

    // A file the project excludes should not block a commit either.
    List<File> toLint = new ArrayList<>();
    for (File staged : stagedFiles) {
      if (linter.getPolicy().isExcluded(staged.getAbsolutePath(), reportBaseDirectory())) {
        if (verbose) {
          System.out.println("Skipping excluded file: " + staged);
        }
        continue;
      }
      toLint.add(staged);
    }
    if (toLint.isEmpty()) {
      if (!quiet) {
        System.out.println("No staged Hop files to lint.");
      }
      return 0;
    }

    PreCommitLintService.Result result =
        PreCommitLintService.lintFiles(
            toLint, LintSeverity.parseFailOn(failOn), variables, metadataProvider);

    // The baseline matters most here: touching a legacy file would otherwise block the commit
    // on findings that were already there before the change.
    List<LintResult> results = applyBaseline(result.getResults());

    outputResults(filterForDisplay(results));

    if (shouldFail(results, LintSeverity.parseFailOn(failOn))) {
      long blocking =
          results.stream()
              .filter(
                  r ->
                      LintSeverity.meetsFailOnThreshold(
                          r.getSeverity(), LintSeverity.parseFailOn(failOn)))
              .count();
      if (!quiet) {
        System.err.println(
            "Pre-commit lint failed: "
                + blocking
                + " blocking issue(s) at "
                + failOn
                + " threshold.");
      }
      return 1;
    }
    return 0;
  }

  /** The project folder a staged file belongs to, used to root relative paths. */
  private String projectRootOf(File staged) {
    File metadataFolder = findMetadataFolder(staged);
    if (metadataFolder != null && metadataFolder.getParentFile() != null) {
      return metadataFolder.getParentFile().getAbsolutePath();
    }
    File parent = staged.isDirectory() ? staged : staged.getParentFile();
    return parent != null ? parent.getAbsolutePath() : userDirectory();
  }

  private boolean shouldFail(List<LintResult> results, LintSeverity.FailOn threshold) {
    if (threshold == LintSeverity.FailOn.NONE) {
      return false;
    }
    return results.stream()
        .anyMatch(result -> LintSeverity.meetsFailOnThreshold(result.getSeverity(), threshold));
  }

  private void printRunHeader(String targetPath) {
    if (quiet || format != LintReportFormat.TEXT) {
      return;
    }
    System.out.println("Apache Hop Lint Checker CLI");
    System.out.println("Target: " + targetPath);
    if (configFile != null) {
      System.out.println("Config: " + configFile);
    }
    if (outputFile != null) {
      System.out.println("Output: " + outputFile);
    }
    if (severityFilter != null) {
      System.out.println("Severity Filter: " + severityFilter);
    }
    System.out.println();
  }

  /**
   * Bring up Hop far enough to load pipelines and workflows properly.
   *
   * <p>This is not optional bookkeeping. Without it {@code LogChannel.GENERAL} throws "Central Log
   * Store is not initialized" on the first log call, and — worse, because it fails quietly — the
   * transform and action plugin registries are empty, so {@code TransformMeta.getTransform()}
   * returns null and every rule that inspects a transform's or action's own fields silently finds
   * nothing. A lint run that reports "no issues" because the engine never loaded is more damaging
   * than one that crashes.
   */
  private void initializeHopEnvironment() throws HopException {
    quietenHopLogging();
    HopEnvironment.init();

    String projectHome = System.getProperty("HOP_PROJECT_HOME");
    if (!Utils.isEmpty(projectHome) && verbose) {
      System.out.println("Project home: " + projectHome);
    }
  }

  /**
   * Keep Hop's engine logging off stdout when stdout is carrying a report.
   *
   * <p>Hop logs to the console by default. That is fine for the text report, but {@code -f sarif}
   * piped to another tool has to be a valid document, and interleaved log lines make it garbage. An
   * explicit {@code --output} file keeps the two streams apart, so logging is left alone there.
   */
  private void quietenHopLogging() {
    boolean reportOwnsStdout = format != LintReportFormat.TEXT && outputFile == null;
    if (verbose) {
      return;
    }
    if (quiet || reportOwnsStdout) {
      DefaultLogLevel.setLogLevel(LogLevel.NOTHING);
    }
  }

  private void loadConfiguration(HopLinter linter, String targetPath) throws IOException {
    if (configFile != null) {
      if (!new File(configFile).exists()) {
        throw new IOException("Configuration file not found: " + configFile);
      }
      linter.loadConfig(configFile);
      if (verbose) {
        System.out.println("Loaded configuration from: " + configFile);
      }
      return;
    }

    linter.loadConfigurationForContext(new File(targetPath));
    if (verbose) {
      System.out.println("Loaded linter configuration for: " + targetPath);
    }
  }

  private List<LintResult> runLinting(HopLinter linter, String targetPath) throws Exception {
    File targetFile = new File(targetPath);
    IVariables variables = Variables.getADefaultVariableSpace();
    IHopMetadataProvider metadataProvider = resolveMetadataProvider(targetFile, variables);

    if (targetFile.isFile()) {
      if (verbose) {
        System.out.println("Linting file: " + targetPath);
      }
      return new ArrayList<>(linter.processFile(targetFile, metadataProvider, variables));
    }
    if (targetFile.isDirectory()) {
      if (verbose) {
        System.out.println("Linting directory: " + targetPath);
      }
      // Index the project's references first, so that rules which depend on the project as a whole
      // — whether a pipeline is called by anything, whether a connection is used — have something
      // to read. Only a directory lint can build this; a single file has no project to see.
      List<String> projectFiles = linter.findHopFiles(targetPath);
      LintProjectIndex index = LintProjectIndex.build(projectFiles, metadataProvider, variables);
      if (verbose) {
        System.out.println("Indexed " + index.getIndexedFiles().size() + " file(s) for references");
      }
      CustomRuleExecutor.setProjectIndex(index);
      try {
        return new ArrayList<>(linter.run(targetPath, metadataProvider, variables, null));
      } finally {
        CustomRuleExecutor.setProjectIndex(null);
      }
    }
    throw new IllegalArgumentException("Target does not exist: " + targetPath);
  }

  /**
   * Build a metadata provider over the project's {@code metadata/} folder.
   *
   * <p>Loading a pipeline without one fails outright — Hop needs it to resolve named connections
   * and other referenced metadata — so passing null here meant the CLI could not read a single
   * file. The folder is located by walking up from the lint target, which is how a user thinks
   * about it: point the linter at a pipeline deep in a project and it still finds the project.
   */
  private IHopMetadataProvider resolveMetadataProvider(File target, IVariables variables) {
    File metadataFolder = findMetadataFolder(target);
    if (metadataFolder == null) {
      if (verbose) {
        System.out.println("No metadata/ folder found; linting without a metadata provider.");
      }
      return HopMetadataUtil.getStandardHopMetadataProvider(variables);
    }
    if (verbose) {
      System.out.println("Using metadata folder: " + metadataFolder.getAbsolutePath());
    }
    variables.setVariable(Const.HOP_METADATA_FOLDER, metadataFolder.getAbsolutePath());
    return new JsonMetadataProvider(Encr.getEncoder(), metadataFolder.getAbsolutePath(), variables);
  }

  private static File findMetadataFolder(File target) {
    File directory = target.isDirectory() ? target : target.getParentFile();
    while (directory != null) {
      File candidate = new File(directory, "metadata");
      if (candidate.isDirectory()) {
        return candidate;
      }
      directory = directory.getParentFile();
    }
    return null;
  }

  /**
   * The directory the user was in when they ran the command.
   *
   * <p>The {@code hop} launcher runs the JVM from the Hop installation, so {@code user.dir} is the
   * install folder rather than where the command was typed. The launcher passes the real one in
   * {@code hop.origin.dir}; the fallback covers being called any other way.
   */
  private static String userDirectory() {
    String origin = System.getProperty("hop.origin.dir");
    return Utils.isEmpty(origin) ? System.getProperty("user.dir") : origin;
  }

  private void installGitHook() throws IOException {
    // Resolve from what is being linted, never from the working directory: under the launcher that
    // is the Hop installation, and the hook would land in whatever repository Hop itself sits in.
    File from = new File(Utils.isEmpty(target) ? userDirectory() : target).getAbsoluteFile();
    File gitDir = findGitDirectory(from);
    if (gitDir == null) {
      throw new IOException("No git repository found at or above " + from.getPath() + ".");
    }

    File hooksDir = new File(gitDir, "hooks");
    if (!hooksDir.exists() && !hooksDir.mkdirs()) {
      throw new IOException("Could not create hooks directory: " + hooksDir.getAbsolutePath());
    }

    File hookFile = new File(hooksDir, "pre-commit");
    java.nio.file.Files.writeString(
        hookFile.toPath(), hookScript(), java.nio.charset.StandardCharsets.UTF_8);
    hookFile.setExecutable(true);
    System.out.println("Installed git pre-commit hook: " + hookFile.getAbsolutePath());
  }

  private File findGitDirectory(File start) {
    File current = start;
    while (current != null) {
      File git = new File(current, ".git");
      if (git.exists()) {
        return git;
      }
      current = current.getParentFile();
    }
    return null;
  }

  /**
   * The pre-commit hook this installs.
   *
   * <p>It resolves the launcher at commit time rather than baking in a path, so the hook keeps
   * working when Hop is upgraded or moved, and so the same hook can be committed to a repository
   * that several people clone.
   */
  private String hookScript() {
    return """
        #!/bin/sh
        set -e
        STAGED_LIST="$(mktemp)"
        git diff --cached --name-only --diff-filter=ACM > "$STAGED_LIST"
        if ! grep -E '\\.(hpl|hwf)$|/metadata/.*\\.json$' "$STAGED_LIST" > /dev/null 2>&1; then
          rm -f "$STAGED_LIST"
          exit 0
        fi
        HOP="${HOP_HOME:-}/hop"
        if [ ! -x "$HOP" ]; then
          HOP="$(command -v hop || true)"
        fi
        if [ -z "$HOP" ] || [ ! -x "$HOP" ]; then
          echo "The hop launcher was not found. Set HOP_HOME, or put hop on the PATH." >&2
          rm -f "$STAGED_LIST"
          exit 1
        fi
        "$HOP" lint --pre-commit --staged-file "$STAGED_LIST" \
          ${HOP_LINT_FAIL_ON:+--fail-on "$HOP_LINT_FAIL_ON"}
        STATUS=$?
        rm -f "$STAGED_LIST"
        exit $STATUS
        """;
  }

  private void outputResults(List<LintResult> results) {
    String report;
    try {
      report = LintReportWriter.render(format, results, toolVersion(), reportBaseDirectory());
    } catch (Exception e) {
      System.err.println("Error rendering the " + format.getId() + " report: " + e.getMessage());
      report = LintReportWriter.renderText(results);
    }

    if (outputFile != null) {
      try {
        Files.writeString(Paths.get(outputFile), report, StandardCharsets.UTF_8);
        if (!quiet) {
          System.out.println(
              "Wrote "
                  + results.size()
                  + " finding(s) to "
                  + outputFile
                  + " ("
                  + format.getId()
                  + ").");
        }
        return;
      } catch (IOException e) {
        // Falling through to stdout keeps the findings visible rather than losing them
        // because a path was wrong.
        System.err.println("Error writing results to " + outputFile + ": " + e.getMessage());
      }
    }

    // In quiet mode a clean text run says nothing at all; machine formats always print, because
    // a consumer expects a parseable document even when there is nothing to report.
    if (quiet && results.isEmpty() && format == LintReportFormat.TEXT) {
      return;
    }
    System.out.print(report);
  }

  /** Findings are reported relative to the lint target, so CI paths match the repository. */
  private Path reportBaseDirectory() {
    if (Utils.isEmpty(target)) {
      return null;
    }
    File targetFile = new File(target);
    File directory = targetFile.isDirectory() ? targetFile : targetFile.getParentFile();
    return directory != null ? directory.toPath().toAbsolutePath() : null;
  }
}
