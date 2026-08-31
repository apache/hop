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
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lint.CustomLintRule;
import org.apache.hop.lint.LintPolicy;
import org.apache.hop.lint.RuleClause;
import org.apache.hop.lint.RuleCombinator;
import org.apache.hop.lint.RuleCondition;
import org.apache.hop.lint.RuleTarget;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/** Parses YAML rule pack and project overlay files. */
public final class YamlRulePackParser {

  private YamlRulePackParser() {}

  /**
   * Read a YAML document as a map.
   *
   * <p>Uses {@link SafeConstructor} rather than a plain {@link Yaml}: a rule pack is ordinary
   * configuration, so YAML tags which instantiate arbitrary Java types have no business in one, and
   * a pack is a file anybody can drop into the plugins folder.
   *
   * @param inputStream the YAML to read, closed by the caller
   * @return the document as a map, empty when the file holds nothing
   * @throws IOException when the document is not a map, so that a malformed file is reported rather
   *     than quietly read as no rules at all
   */
  /**
   * Read an {@code allOf:} or {@code anyOf:} block into the rule's clauses.
   *
   * <p>The first clause is stored in the rule's own targetField / condition / conditionValue, so a
   * composed rule still looks like an ordinary rule to everything that reads one. A rule with
   * neither block keeps exactly the shape it always had.
   */
  private static void applyClauses(
      CustomLintRule customRule, Map<String, Object> ruleData, String ruleName) {
    for (RuleCombinator combinator : RuleCombinator.values()) {
      Object block = ruleData.get(combinator.getYamlKey());
      if (!(block instanceof List<?> entries) || entries.isEmpty()) {
        continue;
      }
      List<RuleClause> clauses = new ArrayList<>();
      for (Object entry : entries) {
        if (!(entry instanceof Map<?, ?> map)) {
          LogChannel.GENERAL.logError(
              "Ignoring a malformed "
                  + combinator.getYamlKey()
                  + " entry in rule '"
                  + ruleName
                  + "': each entry must be a mapping of targetField, condition and conditionValue.");
          continue;
        }
        String field = stringValue(map.get("targetField"), "");
        String conditionName = stringValue(map.get("condition"), null);
        if (Utils.isEmpty(field) || conditionName == null) {
          LogChannel.GENERAL.logError(
              "Ignoring a "
                  + combinator.getYamlKey()
                  + " entry in rule '"
                  + ruleName
                  + "': it must name both a targetField and a condition.");
          continue;
        }
        clauses.add(
            new RuleClause(
                field,
                RuleCondition.valueOf(conditionName),
                stringValue(map.get("conditionValue"), "")));
      }
      if (clauses.isEmpty()) {
        continue;
      }
      customRule.setCombinator(combinator);
      // The first clause becomes the rule's own condition; the rest hang off it.
      RuleClause first = clauses.get(0);
      customRule.setTargetField(first.getTargetField());
      customRule.setCondition(first.getCondition());
      customRule.setConditionValue(first.getConditionValue());
      customRule.setAdditionalClauses(new ArrayList<>(clauses.subList(1, clauses.size())));
      return;
    }
  }

  private static Map<String, Object> readYaml(InputStream inputStream, String source)
      throws IOException {
    Object loaded = new Yaml(new SafeConstructor(new LoaderOptions())).load(inputStream);
    if (loaded == null) {
      return Collections.emptyMap();
    }
    if (!(loaded instanceof Map)) {
      throw new IOException(
          source
              + " is not a YAML mapping. A rule pack is a mapping of pack: and rules: blocks, and"
              + " reading it as empty would silently lint with no rules from it.");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) loaded;
    return map;
  }

  public static List<CustomLintRule> loadFromClasspath(
      String resourcePath, String defaultPackId, RulePackOwner defaultOwner) {
    return loadFromClasspath(resourcePath, defaultPackId, defaultOwner, YamlRulePackParser.class);
  }

  public static List<CustomLintRule> loadFromClasspath(
      String resourcePath, String defaultPackId, RulePackOwner defaultOwner, Class<?> anchor) {
    if (resourcePath == null || resourcePath.isEmpty()) {
      return Collections.emptyList();
    }

    for (ClassLoader loader : classLoadersToTry(anchor)) {
      try (InputStream inputStream = loader.getResourceAsStream(resourcePath)) {
        if (inputStream != null) {
          return loadFromStream(inputStream, defaultPackId, defaultOwner);
        }
      } catch (IOException e) {
        LogChannel.GENERAL.logDetailed(
            "Failed to read rule pack " + resourcePath + " from " + loader + ": " + e.getMessage());
      }
    }

    LogChannel.GENERAL.logError("Rule pack resource not found on classpath: " + resourcePath);
    return Collections.emptyList();
  }

  /**
   * YAML shipped next to the plugin jar (e.g. {@code hop-lint-core.yml} beside the engine jar).
   * Used when the Hop GUI classloader cannot read classpath resources from the plugin jar.
   */
  public static File findPackYamlAdjacentTo(Class<?> anchor, String resourcePath) {
    File pluginDir = PluginDirectoryResolver.locateEnginePluginDirectory(anchor);
    if (pluginDir == null || Utils.isEmpty(resourcePath)) {
      return null;
    }
    File yamlFile = new File(pluginDir, resourcePath);
    return yamlFile.isFile() ? yamlFile : null;
  }

  public static List<CustomLintRule> loadRulesWithAdjacentFallback(
      String resourcePath, String packId, RulePackOwner owner, Class<?> anchor) {
    List<CustomLintRule> rules = loadFromClasspath(resourcePath, packId, owner, anchor);
    if (!rules.isEmpty()) {
      return rules;
    }
    File adjacent = findPackYamlAdjacentTo(anchor, resourcePath);
    if (adjacent == null) {
      return rules;
    }
    try {
      List<CustomLintRule> fromFile = loadFromFile(adjacent, packId, owner);
      if (!fromFile.isEmpty()) {
        LogChannel.GENERAL.logDetailed(
            "Loaded rule pack "
                + packId
                + " from plugin folder file: "
                + adjacent.getAbsolutePath());
      }
      return fromFile;
    } catch (IOException e) {
      LogChannel.GENERAL.logError(
          "Failed to load rule pack "
              + packId
              + " from "
              + adjacent.getAbsolutePath()
              + ": "
              + e.getMessage(),
          e);
      return Collections.emptyList();
    }
  }

  private static ClassLoader[] classLoadersToTry(Class<?> anchor) {
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader anchorLoader = anchor != null ? anchor.getClassLoader() : null;
    ClassLoader parserLoader = YamlRulePackParser.class.getClassLoader();
    return new ClassLoader[] {anchorLoader, parserLoader, contextLoader};
  }

  public static List<CustomLintRule> loadFromFile(
      File file, String defaultPackId, RulePackOwner defaultOwner) throws IOException {
    try (InputStream inputStream = Files.newInputStream(file.toPath())) {
      return loadFromStream(inputStream, defaultPackId, defaultOwner);
    }
  }

  public static List<CustomLintRule> loadFromStream(
      InputStream inputStream, String defaultPackId, RulePackOwner defaultOwner)
      throws IOException {
    Map<String, Object> yamlData = readYaml(inputStream, "The rule pack");
    PackMetadata metadata = parsePackMetadata(yamlData, defaultPackId, defaultOwner, 0);
    return parseRulesSection(yamlData, metadata);
  }

  /**
   * Read just the {@code pack:} metadata block (id, name, owner, priority) from a YAML file so a
   * discovered pack can declare its own identity instead of relying on its file or folder name.
   */
  public static PackMetadata readPackMetadata(
      File file, String defaultPackId, RulePackOwner defaultOwner, int defaultPriority)
      throws IOException {
    Map<String, Object> yamlData;
    try (InputStream inputStream = Files.newInputStream(file.toPath())) {
      yamlData = readYaml(inputStream, file.getPath());
    }
    return parsePackMetadata(yamlData, defaultPackId, defaultOwner, defaultPriority);
  }

  public static ProjectYamlOverlay parseProjectYaml(File projectYaml) throws IOException {
    if (projectYaml == null || !projectYaml.exists()) {
      return ProjectYamlOverlay.empty();
    }
    Map<String, Object> yamlData;
    try (InputStream inputStream = Files.newInputStream(projectYaml.toPath())) {
      yamlData = readYaml(inputStream, projectYaml.getPath());
    }
    LintPolicy policy = parsePolicy(yamlData, projectYaml);

    @SuppressWarnings("unchecked")
    Map<String, Object> rulesSection = (Map<String, Object>) yamlData.get("rules");
    if (rulesSection == null || rulesSection.isEmpty()) {
      return new ProjectYamlOverlay(Collections.emptyList(), Collections.emptyMap(), policy);
    }

    List<CustomLintRule> projectRules = new ArrayList<>();
    Map<String, ProjectYamlOverlay.ProjectRuleOverlay> overlays = new HashMap<>();

    for (Map.Entry<String, Object> entry : rulesSection.entrySet()) {
      String ruleId = entry.getKey();
      @SuppressWarnings("unchecked")
      Map<String, Object> ruleData = (Map<String, Object>) entry.getValue();
      if (isCustomRuleDefinition(ruleData)) {
        CustomLintRule rule =
            parseCustomRule(ruleId, ruleData, RulePackIds.PROJECT, RulePackOwner.PROJECT);
        projectRules.add(rule);
      } else {
        overlays.put(ruleId, ProjectYamlOverlay.ProjectRuleOverlay.fromMap(ruleData));
      }
    }
    return new ProjectYamlOverlay(projectRules, overlays, policy);
  }

  /**
   * Read the {@code exclude:} and {@code suppress:} blocks.
   *
   * <p>A suppression without a rule id would silence everything, which is switching the linter off
   * by another name, and one without a reason is an undocumented decision that nobody can review
   * later. Both are rejected loudly rather than applied.
   */
  private static LintPolicy parsePolicy(Map<String, Object> yamlData, File projectYaml) {
    List<String> excludes = stringListValue(yamlData.get("exclude"));

    List<LintPolicy.Suppression> suppressions = new ArrayList<>();
    Object suppressSection = yamlData.get("suppress");
    if (suppressSection instanceof List) {
      int index = 0;
      for (Object element : (List<?>) suppressSection) {
        index++;
        if (!(element instanceof Map)) {
          LogChannel.GENERAL.logError(
              "Ignoring suppress entry " + index + " in " + projectYaml + ": expected a mapping.");
          continue;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> entry = (Map<String, Object>) element;
        String ruleId = stringValue(entry.get("rule"), null);
        String reason = stringValue(entry.get("reason"), null);

        if (Utils.isEmpty(ruleId)) {
          LogChannel.GENERAL.logError(
              "Ignoring suppress entry "
                  + index
                  + " in "
                  + projectYaml
                  + ": it must name a rule. A suppression without one would silence"
                  + " every rule.");
          continue;
        }
        if (Utils.isEmpty(reason)) {
          LogChannel.GENERAL.logError(
              "Ignoring suppression of "
                  + ruleId
                  + " in "
                  + projectYaml
                  + ": it must give a reason, so the decision can be reviewed later.");
          continue;
        }
        suppressions.add(
            new LintPolicy.Suppression(
                ruleId,
                stringValue(entry.get("path"), null),
                stringValue(entry.get("source"), null),
                reason));
      }
    }

    return new LintPolicy(excludes, suppressions);
  }

  private static PackMetadata parsePackMetadata(
      Map<String, Object> yamlData,
      String defaultPackId,
      RulePackOwner defaultOwner,
      int defaultPriority) {
    @SuppressWarnings("unchecked")
    Map<String, Object> packSection = (Map<String, Object>) yamlData.get("pack");
    if (packSection == null) {
      return new PackMetadata(
          defaultPackId, defaultPackId, defaultOwner, defaultPriority, Collections.emptyList());
    }
    String packId = stringValue(packSection.get("id"), defaultPackId);
    String displayName = stringValue(packSection.get("name"), packId);
    RulePackOwner owner = defaultOwner;
    Object ownerValue = packSection.get("owner");
    if (ownerValue != null) {
      try {
        owner = RulePackOwner.valueOf(ownerValue.toString().trim().toUpperCase());
      } catch (IllegalArgumentException ignored) {
        owner = defaultOwner;
      }
    }
    return new PackMetadata(
        packId,
        displayName,
        owner,
        intValue(packSection.get("priority"), defaultPriority),
        stringListValue(packSection.get("overrides")));
  }

  private static List<CustomLintRule> parseRulesSection(
      Map<String, Object> yamlData, PackMetadata metadata) {
    @SuppressWarnings("unchecked")
    Map<String, Object> rulesSection = (Map<String, Object>) yamlData.get("rules");
    if (rulesSection == null || rulesSection.isEmpty()) {
      return Collections.emptyList();
    }
    List<CustomLintRule> rules = new ArrayList<>();
    for (Map.Entry<String, Object> entry : rulesSection.entrySet()) {
      String ruleId = entry.getKey();
      @SuppressWarnings("unchecked")
      Map<String, Object> ruleData = (Map<String, Object>) entry.getValue();
      if (!isCustomRuleDefinition(ruleData)) {
        continue;
      }
      CustomLintRule rule = parseCustomRule(ruleId, ruleData, metadata.packId(), metadata.owner());
      rules.add(rule);
    }
    return rules;
  }

  public static boolean isCustomRuleDefinition(Map<String, Object> ruleData) {
    if (ruleData == null) {
      return false;
    }
    Object type = ruleData.get("type");
    if ("custom".equals(type)) {
      return true;
    }
    return ruleData.containsKey("target") && ruleData.containsKey("condition");
  }

  public static CustomLintRule parseCustomRule(
      String ruleId, Map<String, Object> ruleData, String packId, RulePackOwner owner) {
    CustomLintRule customRule = new CustomLintRule();
    customRule.setId(ruleId);
    customRule.setPackId(packId);
    customRule.setPackOwner(owner);

    String name = stringValue(ruleData.get("name"), null);
    if (name == null || name.trim().isEmpty()) {
      name = stringValue(ruleData.get("description"), ruleId);
    }
    customRule.setName(name);
    customRule.setDescription(stringValue(ruleData.get("description"), ""));
    customRule.setEnabled(booleanValue(ruleData.get("enabled"), true));
    customRule.setSeverity(stringValue(ruleData.get("severity"), "WARNING"));

    String targetStr = stringValue(ruleData.get("target"), null);
    if (targetStr != null) {
      customRule.setTarget(RuleTarget.valueOf(targetStr));
    }

    String conditionStr = stringValue(ruleData.get("condition"), null);
    if (conditionStr != null) {
      customRule.setCondition(RuleCondition.valueOf(conditionStr));
    }

    customRule.setTargetField(stringValue(ruleData.get("targetField"), ""));
    customRule.setConditionValue(stringValue(ruleData.get("conditionValue"), ""));
    customRule.setAppliesTo(stringListValue(ruleData.get("appliesTo")));

    applyClauses(customRule, ruleData, name);

    @SuppressWarnings("unchecked")
    Map<String, Object> parameters =
        (Map<String, Object>) ruleData.getOrDefault("parameters", new HashMap<>());
    if (parameters != null && !parameters.isEmpty()) {
      customRule.setAdditionalParameters(new HashMap<>(parameters));
    }
    return customRule;
  }

  private static String stringValue(Object value, String defaultValue) {
    return value != null ? value.toString() : defaultValue;
  }

  /**
   * Read a YAML value that may be written as a list or as a single scalar, so both {@code
   * appliesTo: TableOutput} and a bulleted list are accepted.
   */
  private static List<String> stringListValue(Object value) {
    if (value == null) {
      return Collections.emptyList();
    }
    if (value instanceof List) {
      List<String> values = new ArrayList<>();
      for (Object element : (List<?>) value) {
        if (element != null && !element.toString().trim().isEmpty()) {
          values.add(element.toString().trim());
        }
      }
      return values;
    }
    String single = value.toString().trim();
    return single.isEmpty() ? Collections.emptyList() : List.of(single);
  }

  private static boolean booleanValue(Object value, boolean defaultValue) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    return defaultValue;
  }

  private static int intValue(Object value, int defaultValue) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value != null) {
      try {
        return Integer.parseInt(value.toString().trim());
      } catch (NumberFormatException ignored) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  /**
   * A pack's identity, plus the foreign rule ids it declares it intends to replace.
   *
   * <p>Rule ids share one namespace across packs, and the higher-priority pack silently won. A
   * third-party pack could therefore ship its own "DB-001" and quietly replace Apache's
   * hardcoded-password rule with something weaker, with nothing in the UI to show it had happened.
   * Replacing another pack's rule is now something a pack has to ask for by name.
   */
  public record PackMetadata(
      String packId,
      String displayName,
      RulePackOwner owner,
      int priority,
      List<String> overrides) {

    public boolean declaresOverrideOf(String ruleId) {
      return overrides.stream().anyMatch(id -> id.equalsIgnoreCase(ruleId));
    }
  }
}
