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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lint.CustomLintRule;
import org.apache.hop.lint.LinterConfig;
import org.apache.hop.lint.LinterConfigPlugin;
import org.apache.hop.lint.RuleConfig;

/**
 * Discovers rule packs, merges them in priority order, and applies project hop-lint.yml overlays.
 */
public class RuleRegistry {

  private static final RuleRegistry INSTANCE = new RuleRegistry();

  private final RulePackDiscovery packDiscovery = new RulePackDiscovery();

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
   * The unknown rule id warnings already logged, per hop-lint.yml. Resolution runs once per file
   * and on every background check, so logging each time repeated the same line for as long as the
   * project kept the id.
   */
  private final Set<String> loggedWarnings = ConcurrentHashMap.newKeySet();

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
          LogChannel.GENERAL.logError(
              "Failed to load rule pack " + pack.getPackId() + ": " + e.getMessage(), e);
        }
      }
      packRules = loaded;
      return loaded;
    }
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
            if (loggedWarnings.add(projectYaml.getAbsolutePath() + '\n' + warning)) {
              LogChannel.GENERAL.logMinimal(warning);
            } else {
              LogChannel.GENERAL.logDetailed(warning);
            }
          }
        }
        LogChannel.GENERAL.logDetailed(
            "Applied project lint overlay from: " + projectYaml.getAbsolutePath());
      } catch (Exception e) {
        // The user owns this file: report it instead of silently falling back to defaults.
        LogChannel.GENERAL.logError(
            "Failed to load project hop-lint.yml: " + projectYaml.getAbsolutePath(), e);
        throw new LintConfigurationException(
            "Invalid lint configuration in "
                + projectYaml.getAbsolutePath()
                + ": "
                + e.getMessage(),
            e);
      }
    }

    LinterConfig config = buildLinterConfig(merged);
    config.setEnabled(true);
    return new EffectiveRuleSet(
        new ArrayList<>(merged.values()), config, overlay.getPolicy(), warnings);
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
  private static String closestId(String ruleId, Collection<String> knownIds) {
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
    return resolve(findProjectYaml(context));
  }

  public EffectiveRuleSet resolveForCurrentProject() {
    File projectYaml = null;
    try {
      String projectPath = LinterConfigPlugin.getInstance().getProjectPath();
      if (!Utils.isEmpty(projectPath)) {
        projectYaml = new File(projectPath, "hop-lint.yml");
      }
    } catch (Exception ignored) {
      // CLI mode
    }
    return resolve(
        projectYaml != null && projectYaml.exists() ? projectYaml : findProjectYaml(null));
  }

  /** Locate the project hop-lint.yml governing a file, or null. Used to root relative patterns. */
  public File findProjectYaml(File context) {
    try {
      String projectPath = LinterConfigPlugin.getInstance().getProjectPath();
      if (!Utils.isEmpty(projectPath)) {
        File projectConfig = new File(projectPath, "hop-lint.yml");
        if (projectConfig.exists()) {
          return projectConfig;
        }
      }
    } catch (Exception ignored) {
      // Plugin may not be initialized in CLI mode.
    }

    File directory =
        context != null && context.isDirectory()
            ? context
            : (context != null ? context.getParentFile() : null);
    while (directory != null) {
      File configFile = new File(directory, "hop-lint.yml");
      if (configFile.exists()) {
        return configFile;
      }
      directory = directory.getParentFile();
    }
    return null;
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
