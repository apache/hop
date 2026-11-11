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
package org.apache.hop.lint.registry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lint.CustomLintRule;
import org.apache.hop.lint.LinterConfig;
import org.apache.hop.lint.LinterConfigPlugin;
import org.apache.hop.lint.RuleClause;
import org.apache.hop.lint.RuleConfig;

/**
 * Discovers rule packs, merges them in priority order, and applies project hop-lint.yml overlays.
 */
public class RuleRegistry {

  private static final RuleRegistry INSTANCE = new RuleRegistry();

  public static final String LINT_CONFIG_FILENAME = "hop-lint.yml";

  /**
   * The files that mark a Hop project's home folder, the ones the projects plugin looks for. A
   * project with a configuration file of another name is recognised by its home folder instead.
   */
  private static final List<String> PROJECT_CONFIG_FILENAMES =
      List.of("project-config.json", "hop-project.config");

  private final RulePackDiscovery packDiscovery;

  /**
   * Rules contributed by the installed packs, cached after the first resolution.
   *
   * <p>Discovery walks the whole plugins tree and opens a classloader per candidate jar, and it
   * used to run on every resolution — which is once per file during a project lint, and again for
   * every keystroke-triggered background check. Installed packs cannot change without a restart, so
   * this is computed once. Project overlays are applied per call, on a copy, because those do
   * change while Hop is running.
   */
  private volatile Map<String, CustomLintRule> packRules;

  /**
   * Why each installed pack that could not be loaded failed, recorded with {@link #packRules}.
   *
   * <p>Hop Gui carries on without such a pack, but {@code hop lint} and the Run Linter action fail
   * on it: a build that passes because a pack's rules never ran is a build that passed for the
   * wrong reason.
   */
  private volatile List<String> packErrors = List.of();

  private RuleRegistry() {
    this(new RulePackDiscovery());
  }

  /** A registry over the packs this discovery finds; for tests. */
  RuleRegistry(RulePackDiscovery packDiscovery) {
    this.packDiscovery = packDiscovery;
  }

  public static RuleRegistry getInstance() {
    return INSTANCE;
  }

  private Map<String, CustomLintRule> loadPackRules() {
    Map<String, CustomLintRule> cached = packRules;
    if (cached != null) {
      return cached;
    }
    synchronized (this) {
      if (packRules != null) {
        return packRules;
      }
      Map<String, CustomLintRule> loaded = new LinkedHashMap<>();
      List<String> errors = new ArrayList<>();
      for (IHopLintRulePack pack : packDiscovery.discoverAll()) {
        try {
          mergePack(loaded, pack);
          LogChannel.GENERAL.logDetailed(
              "Loaded rule pack "
                  + pack.getPackId()
                  + " ("
                  + pack.getOwner().getDisplayName()
                  + ")");
        } catch (Exception e) {
          String error =
              "Rule pack '" + pack.getPackId() + "' could not be loaded: " + messagesOf(e);
          errors.add(error);
          LogChannel.GENERAL.logError(error, e);
        }
      }
      packErrors = List.copyOf(errors);
      packRules = loaded;
      return loaded;
    }
  }

  /** Why each installed pack that could not be loaded failed, empty when all of them loaded. */
  public List<String> getPackErrors() {
    loadPackRules();
    return packErrors;
  }

  /**
   * The messages along an exception's cause chain, so a YAML error wrapped in "Failed to load rule
   * pack from ..." still names the line that is wrong.
   */
  static String messagesOf(Throwable throwable) {
    List<String> messages = new ArrayList<>();
    // getThrowableList stops at a cause seen before, so a cause chain that loops cannot hang the
    // registry while it holds its lock.
    for (Throwable t : ExceptionUtils.getThrowableList(throwable)) {
      String message = t.getMessage();
      if (!Utils.isEmpty(message)
          && messages.stream().noneMatch(known -> known.contains(message))) {
        messages.add(message);
      }
    }
    return messages.isEmpty() ? throwable.toString() : String.join(": ", messages);
  }

  /**
   * Add a pack's rules to the merge, refusing any that would take over another pack's rule id
   * without saying so.
   *
   * <p>Packs are merged in priority order, so a later pack would otherwise simply win: a
   * third-party pack could ship its own {@code DB-001} and quietly stand in for Apache's
   * hardcoded-password rule, leaving a rule list that looks unchanged. Replacing another pack's
   * rule now has to be asked for by name in the pack's {@code overrides:} block.
   *
   * <p>Package-private so the merge can be tested directly rather than through discovery.
   */
  static void mergePack(Map<String, CustomLintRule> merged, IHopLintRulePack pack) {
    for (CustomLintRule rule : pack.loadRules()) {
      String ruleId = rule.generateRuleId();
      CustomLintRule incumbent = merged.get(ruleId);
      boolean foreignCollision =
          incumbent != null && !incumbent.getPackId().equals(pack.getPackId());

      if (foreignCollision && !declaresOverride(pack, ruleId)) {
        LogChannel.GENERAL.logError(
            "Rule pack '"
                + pack.getPackId()
                + "' defines rule '"
                + ruleId
                + "', which already belongs to pack '"
                + incumbent.getPackId()
                + "'. Keeping the existing rule. Give the rule its own id, or declare the"
                + " replacement in the pack's overrides: block.");
        continue;
      }
      if (foreignCollision) {
        LogChannel.GENERAL.logBasic(
            "Rule pack '"
                + pack.getPackId()
                + "' overrides rule '"
                + ruleId
                + "' from pack '"
                + incumbent.getPackId()
                + "' as declared.");
      }
      merged.put(ruleId, rule);
    }
  }

  private static boolean declaresOverride(IHopLintRulePack pack, String ruleId) {
    return pack.getOverrides().stream().anyMatch(id -> id.equalsIgnoreCase(ruleId));
  }

  public EffectiveRuleSet resolve(File projectYaml) {
    Map<String, CustomLintRule> merged = new LinkedHashMap<>();

    // Copy: callers and the project overlay below both mutate what they get back, and the
    // cached pack rules have to stay pristine for the next resolution.
    for (Map.Entry<String, CustomLintRule> entry : loadPackRules().entrySet()) {
      merged.put(entry.getKey(), entry.getValue().copy());
    }

    ProjectYamlOverlay overlay = ProjectYamlOverlay.empty();
    List<String> warnings = new ArrayList<>();
    if (projectYaml != null && projectYaml.exists()) {
      try {
        overlay = YamlRulePackParser.parseProjectYaml(projectYaml);
        for (CustomLintRule projectRule : overlay.getProjectRules()) {
          merged.put(projectRule.generateRuleId(), projectRule.copy());
        }
        for (Map.Entry<String, ProjectYamlOverlay.ProjectRuleOverlay> entry :
            overlay.getOverlays().entrySet()) {
          CustomLintRule existing = findRule(merged, entry.getKey());
          if (existing != null) {
            entry.getValue().applyTo(existing);
          } else {
            // Applied to nothing, a typo such as SQL-02 for SQL-002 left the rule as it was with
            // no sign anything had gone wrong.
            String warning = unknownRuleWarning(entry.getKey(), merged.keySet(), projectYaml);
            warnings.add(warning);
            LintWarnings.logOnce(projectYaml.getAbsolutePath() + '\n' + warning, warning);
          }
        }
        LogChannel.GENERAL.logDetailed(
            "Applied project lint overlay from: " + projectYaml.getAbsolutePath());
      } catch (Exception e) {
        // The user owns this file: report it instead of silently falling back to defaults.
        LogChannel.GENERAL.logError(
            "Failed to load project hop-lint.yml: " + projectYaml.getAbsolutePath(), e);
        throw new LintConfigurationException(
            "Invalid lint configuration in " + projectYaml.getAbsolutePath() + ": " + messagesOf(e),
            e);
      }
    }

    warnAboutRulesThatCannotReport(merged.values(), warnings);

    LinterConfig config = buildLinterConfig(merged);
    config.setEnabled(true);
    return new EffectiveRuleSet(
        new ArrayList<>(merged.values()), config, overlay.getPolicy(), warnings);
  }

  /**
   * Warn about each enabled rule whose value its condition cannot use, such as a maximum of {@code
   * abc} or a pattern of {@code [a-}. Such a rule never reports anything, which reads as a clean
   * result, so it is named the way an unknown rule id is.
   */
  private static void warnAboutRulesThatCannotReport(
      Collection<CustomLintRule> rules, List<String> warnings) {
    for (CustomLintRule rule : rules) {
      if (!rule.isEnabled() || rule.isReportOnly() || rule.getTarget() == null) {
        continue;
      }
      String problem =
          RuleClause.valueProblem(
              rule.getClauses(), rule.getTarget(), !rule.getAppliesTo().isEmpty());
      if (problem == null) {
        continue;
      }
      String warning =
          "Warning: rule '"
              + rule.generateRuleId()
              + "' "
              + YamlRulePackParser.inPack(rule.getPackId())
              + " cannot report anything: "
              + problem;
      warnings.add(warning);
      LintWarnings.logOnce(warning, warning);
    }
  }

  /** The rule of this id, ignoring case: {@code sql-002} in hop-lint.yml means SQL-002. */
  private static CustomLintRule findRule(Map<String, CustomLintRule> rules, String ruleId) {
    CustomLintRule exact = rules.get(ruleId);
    if (exact != null) {
      return exact;
    }
    for (Map.Entry<String, CustomLintRule> entry : rules.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(ruleId)) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * A warning, not an error: a project may tune a rule from a pack that is not installed on every
   * machine that lints it, and that should not stop the run.
   */
  static String unknownRuleWarning(String ruleId, Collection<String> knownIds, File projectYaml) {
    StringBuilder warning =
        new StringBuilder("Warning: ")
            .append(projectYaml.getName())
            .append(" changes rule '")
            .append(ruleId)
            .append("', which no installed rule pack defines; the change is ignored.");
    String closest = closestId(ruleId, knownIds);
    if (closest != null) {
      warning.append(" Did you mean ").append(closest).append("?");
    }
    return warning.toString();
  }

  /** The known id within two edits of the one given, or null. */
  static String closestId(String ruleId, Collection<String> knownIds) {
    String best = null;
    int bestDistance = 3;
    for (String known : knownIds) {
      int distance = editDistance(ruleId.toUpperCase(), known.toUpperCase());
      if (distance < bestDistance) {
        bestDistance = distance;
        best = known;
      }
    }
    return best;
  }

  private static int editDistance(String a, String b) {
    int[] previous = new int[b.length() + 1];
    int[] current = new int[b.length() + 1];
    for (int j = 0; j <= b.length(); j++) {
      previous[j] = j;
    }
    for (int i = 1; i <= a.length(); i++) {
      current[0] = i;
      for (int j = 1; j <= b.length(); j++) {
        int substitution = previous[j - 1] + (a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1);
        current[j] = Math.min(substitution, Math.min(previous[j] + 1, current[j - 1] + 1));
      }
      int[] swap = previous;
      previous = current;
      current = swap;
    }
    return previous[b.length()];
  }

  public EffectiveRuleSet resolveForContext(File context) {
    return resolveForContext(context, null);
  }

  /**
   * @param projectHome the home folder of the project in use, {@code PROJECT_HOME}, or null
   * @see #findProjectYaml(File, String, boolean)
   */
  public EffectiveRuleSet resolveForContext(File context, String projectHome) {
    return resolve(findProjectYaml(context, projectHome, true));
  }

  /** The rules of the project Hop Gui has open, or of the rule packs alone when none is open. */
  public EffectiveRuleSet resolveForCurrentProject() {
    return resolve(findProjectYaml(null));
  }

  /**
   * Locate the project hop-lint.yml governing a file, or null. Used to root relative patterns.
   *
   * @see #findProjectYaml(File, String, boolean)
   */
  public File findProjectYaml(File context) {
    return findProjectYaml(context, null, true);
  }

  /**
   * Locate the project hop-lint.yml governing a file, or null. Used to root relative patterns.
   *
   * <p>The nearest hop-lint.yml in the file's folder or above it is used, up to the project root:
   * the folder holding {@code project-config.json} or {@code hop-project.config}, the project's
   * home folder, or the project Hop Gui has open. A hop-lint.yml above the project, left in a home
   * or workspace folder, changed the findings of every project below it and rooted their exclude
   * and suppress paths outside the project, so a CI checkout linted differently from a developer's
   * machine. A file in no project still uses the nearest one above it.
   *
   * <p>The working directory is not a project root: hop-run, hop-server or a container started in a
   * project's subfolder would otherwise stop the search there and miss the project's own file.
   *
   * @param context the file or folder being linted; null for the project Hop Gui has open
   * @param projectHome the home folder of the project in use, {@code PROJECT_HOME}, or null
   * @param warnAboutOneAbove whether to say once that a hop-lint.yml above the project root is not
   *     used; off when the run has a configuration file of its own
   */
  public File findProjectYaml(File context, String projectHome, boolean warnAboutOneAbove) {
    Set<Path> projectHomes = new LinkedHashSet<>();
    addProjectHome(projectHomes, projectHome);
    String guiProjectHome = null;
    try {
      guiProjectHome = LinterConfigPlugin.getInstance().getProjectHome();
    } catch (Exception | LinkageError ignored) {
      // Plugin may not be initialized in CLI mode.
    }
    addProjectHome(projectHomes, guiProjectHome);

    Path directory;
    if (context != null) {
      // Absolute first: the parents of a relative path run out before the project root.
      Path path = normalised(context);
      directory = context.isDirectory() ? path : path.getParent();
    } else {
      directory = Utils.isEmpty(guiProjectHome) ? null : normalised(new File(guiProjectHome));
    }
    while (directory != null) {
      File configFile = directory.resolve(LINT_CONFIG_FILENAME).toFile();
      if (configFile.isFile()) {
        return configFile;
      }
      if (projectHomes.contains(directory) || hasProjectConfig(directory.toFile())) {
        if (warnAboutOneAbove) {
          warnAboutHopLintYmlAbove(directory.toFile());
        }
        return null;
      }
      directory = directory.getParent();
    }
    return null;
  }

  /**
   * Add a project home folder as written and, when it differs, as it is on disk: a home reached
   * through a symbolic link never equals the folder a file's path walks through otherwise.
   */
  private static void addProjectHome(Set<Path> projectHomes, String projectHome) {
    if (Utils.isEmpty(projectHome)) {
      return;
    }
    Path path = normalised(new File(projectHome));
    projectHomes.add(path);
    try {
      projectHomes.add(path.toRealPath());
    } catch (IOException | SecurityException e) {
      // A home folder that does not exist marks no files.
    }
  }

  private static Path normalised(File file) {
    return file.toPath().toAbsolutePath().normalize();
  }

  /**
   * Say once that a hop-lint.yml above the project root is not used, so that one shared above
   * several projects, at the root of a monorepo for example, does not stop applying without a word.
   * The hop-lint.yml of an enclosing project is that project's own and is not mentioned.
   */
  private static void warnAboutHopLintYmlAbove(File projectRoot) {
    for (File directory = projectRoot.getAbsoluteFile().getParentFile();
        directory != null;
        directory = directory.getParentFile()) {
      if (hasProjectConfig(directory)) {
        return;
      }
      File configFile = new File(directory, LINT_CONFIG_FILENAME);
      if (configFile.isFile()) {
        String warning =
            "Warning: "
                + configFile.getAbsolutePath()
                + " is above the project root "
                + projectRoot.getAbsolutePath()
                + " and is not used. Pass it with --config or in the Config file field of the"
                + " Run Linter action, or copy it into the project.";
        LintWarnings.logOnce(warning, warning);
        return;
      }
    }
  }

  /** Whether this folder holds a project configuration file, which makes it a project's root. */
  private static boolean hasProjectConfig(File directory) {
    for (String filename : PROJECT_CONFIG_FILENAMES) {
      if (new File(directory, filename).isFile()) {
        return true;
      }
    }
    return false;
  }

  private LinterConfig buildLinterConfig(Map<String, CustomLintRule> merged) {
    LinterConfig config = new LinterConfig();
    for (CustomLintRule rule : merged.values()) {
      RuleConfig ruleConfig = new RuleConfig();
      ruleConfig.setEnabled(rule.isEnabled());
      ruleConfig.setSeverity(rule.getSeverity());
      ruleConfig.setParameters(new java.util.HashMap<>(rule.getAdditionalParameters()));
      config.setRuleConfig(rule.generateRuleId(), ruleConfig);
    }
    return config;
  }
}
