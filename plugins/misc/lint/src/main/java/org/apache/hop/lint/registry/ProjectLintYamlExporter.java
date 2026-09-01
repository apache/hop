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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hop.lint.CustomLintRule;
import org.apache.hop.lint.RuleClause;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/** Writes project hop-lint.yml without duplicating entire vendor/Apache rule packs. */
public final class ProjectLintYamlExporter {

  private ProjectLintYamlExporter() {}

  public static String export(List<CustomLintRule> desiredRules) {
    try {
      Map<String, CustomLintRule> packDefaults = new LinkedHashMap<>();
      for (CustomLintRule rule : RuleRegistry.getInstance().resolve(null).getRules()) {
        packDefaults.put(rule.generateRuleId(), rule);
      }

      Map<String, Object> rulesMap = new LinkedHashMap<>();
      for (CustomLintRule desired : desiredRules) {
        String ruleId = desired.generateRuleId();
        CustomLintRule packDefault = packDefaults.get(ruleId);
        if (desired.getPackOwner() == RulePackOwner.PROJECT || packDefault == null) {
          rulesMap.put(ruleId, toFullCustomRuleMap(desired));
        } else if (structurallyDiffersFromPackDefault(desired, packDefault)) {
          // The project has redefined the rule rather than tuned it. Written in full, it replaces
          // the pack rule of that id instead of layering on top of it.
          rulesMap.put(ruleId, toFullCustomRuleMap(desired));
        } else if (differsFromPackDefault(desired, packDefault)) {
          rulesMap.put(ruleId, toOverrideMap(desired, packDefault));
        }
      }

      Map<String, Object> config = new LinkedHashMap<>();
      config.put("rules", rulesMap);

      // Block style, so the file stays something a person edits by hand and reviews in a diff.
      DumperOptions options = new DumperOptions();
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
      return new Yaml(options).dump(config);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to export project hop-lint.yml", e);
    }
  }

  private static boolean differsFromPackDefault(
      CustomLintRule desired, CustomLintRule packDefault) {
    return desired.isEnabled() != packDefault.isEnabled()
        || !Objects.equals(desired.getSeverity(), packDefault.getSeverity())
        || !Objects.equals(desired.getAdditionalParameters(), packDefault.getAdditionalParameters())
        || !Objects.equals(desired.getConditionValue(), packDefault.getConditionValue())
        || structurallyDiffersFromPackDefault(desired, packDefault);
  }

  /**
   * Whether the rule has been changed in a way an override block cannot express.
   *
   * <p>An override tunes a pack rule: switch it off, change its severity, move its threshold.
   * Changing what the rule actually looks at is not a tune, it is a different rule, so it has to be
   * written out in full rather than as a handful of override keys which would silently drop the
   * rest.
   */
  private static boolean structurallyDiffersFromPackDefault(
      CustomLintRule desired, CustomLintRule packDefault) {
    return !Objects.equals(clauseShape(desired), clauseShape(packDefault))
        || desired.getCombinator() != packDefault.getCombinator()
        || !Objects.equals(desired.getTarget(), packDefault.getTarget())
        || !Objects.equals(desired.getTargetField(), packDefault.getTargetField())
        || !Objects.equals(desired.getCondition(), packDefault.getCondition())
        || !Objects.equals(desired.getAppliesTo(), packDefault.getAppliesTo())
        || !Objects.equals(desired.getName(), packDefault.getName())
        || !Objects.equals(desired.getDescription(), packDefault.getDescription());
  }

  /**
   * What a rule looks at, ignoring the values it compares against.
   *
   * <p>A changed threshold is a tune and an override block carries it. A changed field or condition
   * is a redefinition and it cannot, so only those count as structural.
   */
  private static List<String> clauseShape(CustomLintRule rule) {
    List<String> shape = new ArrayList<>();
    for (RuleClause clause : rule.getClauses()) {
      shape.add(clause.getTargetField() + "|" + clause.getCondition());
    }
    return shape;
  }

  private static Map<String, Object> toOverrideMap(
      CustomLintRule desired, CustomLintRule packDefault) {
    Map<String, Object> ruleConfig = new LinkedHashMap<>();
    if (desired.isEnabled() != packDefault.isEnabled()) {
      ruleConfig.put("enabled", desired.isEnabled());
    }
    if (!Objects.equals(desired.getSeverity(), packDefault.getSeverity())) {
      ruleConfig.put("severity", desired.getSeverity());
    }
    if (!Objects.equals(desired.getConditionValue(), packDefault.getConditionValue())
        && desired.getConditionValue() != null) {
      ruleConfig.put("conditionValue", desired.getConditionValue());
    }
    if (!Objects.equals(desired.getAdditionalParameters(), packDefault.getAdditionalParameters())
        && !desired.getAdditionalParameters().isEmpty()) {
      ruleConfig.put("parameters", new HashMap<>(desired.getAdditionalParameters()));
    }
    return ruleConfig;
  }

  private static Map<String, Object> toFullCustomRuleMap(CustomLintRule rule) {
    Map<String, Object> ruleConfig = new LinkedHashMap<>();
    ruleConfig.put("enabled", rule.isEnabled());
    ruleConfig.put("severity", rule.getSeverity());
    ruleConfig.put("type", "custom");
    ruleConfig.put("target", rule.getTarget().name());
    if (rule.isComposed()) {
      // Written back the way it was read, so a composed rule survives a round trip through the
      // rule manager instead of collapsing to its first clause.
      List<Map<String, Object>> clauses = new ArrayList<>();
      for (RuleClause clause : rule.getClauses()) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("targetField", clause.getTargetField());
        entry.put("condition", clause.getCondition().name());
        if (clause.getConditionValue() != null && !clause.getConditionValue().isEmpty()) {
          entry.put("conditionValue", clause.getConditionValue());
        }
        clauses.add(entry);
      }
      ruleConfig.put(rule.getCombinator().getYamlKey(), clauses);
    } else {
      ruleConfig.put("targetField", rule.getTargetField());
      ruleConfig.put("condition", rule.getCondition().name());
      ruleConfig.put("conditionValue", rule.getConditionValue());
    }
    ruleConfig.put("name", rule.getName());
    ruleConfig.put("description", rule.getDescription());
    if (!rule.getAppliesTo().isEmpty()) {
      // Omitted when empty so an unrestricted rule round-trips to the same YAML it came from.
      ruleConfig.put("appliesTo", new ArrayList<>(rule.getAppliesTo()));
    }
    ruleConfig.put("parameters", new HashMap<>(rule.getAdditionalParameters()));
    return ruleConfig;
  }
}
