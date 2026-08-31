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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hop.lint.LintPolicy;
import org.apache.hop.lint.RuleConfig;

/** Parsed project hop-lint.yml: local rule definitions plus per-rule overrides. */
public final class ProjectYamlOverlay {

  private final List<org.apache.hop.lint.CustomLintRule> projectRules;
  private final Map<String, ProjectRuleOverlay> overlays;
  private final LintPolicy policy;

  public ProjectYamlOverlay(
      List<org.apache.hop.lint.CustomLintRule> projectRules,
      Map<String, ProjectRuleOverlay> overlays) {
    this(projectRules, overlays, LintPolicy.empty());
  }

  public ProjectYamlOverlay(
      List<org.apache.hop.lint.CustomLintRule> projectRules,
      Map<String, ProjectRuleOverlay> overlays,
      LintPolicy policy) {
    this.projectRules = projectRules != null ? projectRules : Collections.emptyList();
    this.overlays = overlays != null ? overlays : Collections.emptyMap();
    this.policy = policy != null ? policy : LintPolicy.empty();
  }

  public static ProjectYamlOverlay empty() {
    return new ProjectYamlOverlay(Collections.emptyList(), Collections.emptyMap());
  }

  /** What the project excludes from linting, and which findings it has accepted. */
  public LintPolicy getPolicy() {
    return policy;
  }

  public List<org.apache.hop.lint.CustomLintRule> getProjectRules() {
    return projectRules;
  }

  public Map<String, ProjectRuleOverlay> getOverlays() {
    return overlays;
  }

  public static final class ProjectRuleOverlay {
    private final Boolean enabled;
    private final String severity;
    private final String conditionValue;
    private final Map<String, Object> parameters;

    private ProjectRuleOverlay(
        Boolean enabled, String severity, String conditionValue, Map<String, Object> parameters) {
      this.enabled = enabled;
      this.severity = severity;
      this.conditionValue = conditionValue;
      this.parameters = parameters;
    }

    public static ProjectRuleOverlay fromMap(Map<String, Object> ruleData) {
      Boolean enabled = ruleData.containsKey("enabled") ? (Boolean) ruleData.get("enabled") : null;
      String severity =
          ruleData.containsKey("severity") ? String.valueOf(ruleData.get("severity")) : null;
      // Retuning a threshold is the commonest override there is, and leaving it out here made a
      // project's conditionValue: silently do nothing while the built-in ceiling stayed in force.
      String conditionValue =
          ruleData.containsKey("conditionValue")
              ? String.valueOf(ruleData.get("conditionValue"))
              : null;
      @SuppressWarnings("unchecked")
      Map<String, Object> parameters = (Map<String, Object>) ruleData.get("parameters");
      return new ProjectRuleOverlay(enabled, severity, conditionValue, parameters);
    }

    public void applyTo(org.apache.hop.lint.CustomLintRule rule) {
      if (enabled != null) {
        rule.setEnabled(enabled);
      }
      if (severity != null && !severity.trim().isEmpty()) {
        rule.setSeverity(severity);
      }
      if (conditionValue != null) {
        rule.setConditionValue(conditionValue);
      }
      if (parameters != null && !parameters.isEmpty()) {
        rule.getAdditionalParameters().putAll(parameters);
      }
    }

    public RuleConfig toRuleConfig() {
      RuleConfig config = new RuleConfig();
      if (enabled != null) {
        config.setEnabled(enabled);
      }
      if (severity != null) {
        config.setSeverity(severity);
      }
      if (parameters != null) {
        config.setParameters(parameters);
      }
      return config;
    }
  }
}
