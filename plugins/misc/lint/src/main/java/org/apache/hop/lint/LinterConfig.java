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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

/** Main configuration class for the Hop Linter. Maps to the structure of the hop-lint.yml file. */
public class LinterConfig {

  @JsonProperty("enabled")
  private boolean enabled = false;

  @JsonProperty("parameters")
  private Map<String, Object> parameters = null;

  @JsonProperty("rules")
  private Map<String, RuleConfig> rules = new HashMap<>();

  /** Default constructor */
  public LinterConfig() {}

  /**
   * Constructor with rules map
   *
   * @param rules Map of rule IDs to their configurations
   */
  public LinterConfig(Map<String, RuleConfig> rules) {
    this.rules = rules != null ? rules : new HashMap<>();
  }

  /**
   * Get all rule configurations
   *
   * @return Map of rule IDs to their configurations
   */
  public Map<String, RuleConfig> getRules() {
    return rules;
  }

  /**
   * Set all rule configurations
   *
   * @param rules Map of rule IDs to their configurations
   */
  public void setRules(Map<String, RuleConfig> rules) {
    this.rules = rules != null ? rules : new HashMap<>();
  }

  /**
   * Get configuration for a specific rule
   *
   * @param ruleId The rule ID
   * @return The rule configuration, or null if not found
   */
  public RuleConfig getRuleConfig(String ruleId) {
    return rules.get(ruleId);
  }

  /**
   * Check if a rule is enabled
   *
   * @param ruleId The rule ID
   * @return true if the rule is enabled, false if disabled or not configured
   */
  public boolean isRuleEnabled(String ruleId) {
    RuleConfig config = rules.get(ruleId);
    return config != null && config.isEnabled();
  }

  /**
   * Add or update a rule configuration
   *
   * @param ruleId The rule ID
   * @param config The rule configuration
   */
  public void setRuleConfig(String ruleId, RuleConfig config) {
    rules.put(ruleId, config);
  }

  /**
   * Check if the linter is enabled globally
   *
   * @return true if the linter is enabled, false otherwise
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Set whether the linter is enabled globally
   *
   * @param enabled true to enable the linter, false to disable
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Get global parameters
   *
   * @return Map of global parameter names to values
   */
  public Map<String, Object> getParameters() {
    return parameters;
  }

  /**
   * Set global parameters
   *
   * @param parameters Map of global parameter names to values
   */
  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  /**
   * Get parameters for a specific rule
   *
   * @param ruleId The rule ID
   * @return Map of rule parameters, or null if rule not found
   */
  public Map<String, Object> getRuleParameters(String ruleId) {
    RuleConfig ruleConfig = rules.get(ruleId);
    return ruleConfig != null ? ruleConfig.getParameters() : null;
  }
}
