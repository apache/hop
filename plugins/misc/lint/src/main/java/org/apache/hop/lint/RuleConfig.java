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

/**
 * Configuration class for individual lint rules. Maps to the rule configuration structure in the
 * hop-lint.yml file.
 */
public class RuleConfig {

  @JsonProperty("enabled")
  private boolean enabled = false;

  @JsonProperty("severity")
  private String severity = null; // null = use rule default

  @JsonProperty("parameters")
  private Map<String, Object> parameters = new HashMap<>();

  /** Default constructor */
  public RuleConfig() {}

  /**
   * Constructor with enabled flag and parameters
   *
   * @param enabled Whether the rule is enabled
   * @param parameters Rule-specific parameters
   */
  public RuleConfig(boolean enabled, Map<String, Object> parameters) {
    this.enabled = enabled;
    this.parameters = parameters != null ? parameters : new HashMap<>();
  }

  /**
   * Check if the rule is enabled
   *
   * @return true if the rule is enabled, false otherwise
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Set whether the rule is enabled
   *
   * @param enabled true to enable the rule, false to disable
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Get the rule-specific parameters
   *
   * @return Map of parameter names to values
   */
  public Map<String, Object> getParameters() {
    return parameters;
  }

  /**
   * Set the rule-specific parameters
   *
   * @param parameters Map of parameter names to values
   */
  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  /**
   * Get a specific parameter value
   *
   * @param key The parameter name
   * @return The parameter value, or null if not found
   */
  public Object getParameter(String key) {
    return parameters.get(key);
  }

  /**
   * Get a specific parameter value with a default
   *
   * @param key The parameter name
   * @param defaultValue The default value to return if parameter is not found
   * @return The parameter value, or the default value if not found
   */
  public Object getParameter(String key, Object defaultValue) {
    return parameters.getOrDefault(key, defaultValue);
  }

  /**
   * Get the configured severity for this rule
   *
   * @return The severity (ERROR, WARNING, INFO) or null if not configured
   */
  public String getSeverity() {
    return severity;
  }

  /**
   * Set the severity for this rule
   *
   * @param severity The severity level (ERROR, WARNING, INFO)
   */
  public void setSeverity(String severity) {
    this.severity = severity;
  }

  /**
   * Get the effective severity (config value or default)
   *
   * @param defaultSeverity The default severity to use if not configured
   * @return The effective severity level
   */
  public String getEffectiveSeverity(String defaultSeverity) {
    return severity != null && !severity.trim().isEmpty() ? severity : defaultSeverity;
  }
}
