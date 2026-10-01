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
import java.util.LinkedHashMap;
import java.util.Map;
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
    if (projectYaml != null && projectYaml.exists()) {
      try {
        overlay = YamlRulePackParser.parseProjectYaml(projectYaml);
        for (CustomLintRule projectRule : overlay.getProjectRules()) {
          merged.put(projectRule.generateRuleId(), projectRule.copy());
        }
        for (Map.Entry<String, ProjectYamlOverlay.ProjectRuleOverlay> entry :
            overlay.getOverlays().entrySet()) {
          CustomLintRule existing = merged.get(entry.getKey());
          if (existing != null) {
            entry.getValue().applyTo(existing);
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
    return new EffectiveRuleSet(new ArrayList<>(merged.values()), config, overlay.getPolicy());
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
