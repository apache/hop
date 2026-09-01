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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.lint.registry.RulePackIds;
import org.apache.hop.lint.registry.RulePackOwner;

/** Represents a user-defined linting rule */
public class CustomLintRule {

  private String id;
  private String name;
  private String description;
  private boolean enabled;
  private String severity; // ERROR, WARNING
  private RuleTarget target;
  private String targetField;
  private RuleCondition condition;
  private String conditionValue;
  private Map<String, Object> additionalParameters;
  private String packId = RulePackIds.PROJECT;
  private RulePackOwner packOwner = RulePackOwner.PROJECT;

  /**
   * Plugin ids this rule applies to, empty meaning every transform or action.
   *
   * <p>Without this a rule aimed at TRANSFORM runs against every transform in the pipeline, which
   * makes the most-wanted checks unwritable: "Table Output must not truncate" or "REST calls must
   * use HTTPS" only make sense for one kind of transform, and a field like {@code url} means
   * nothing on the others.
   */
  private List<String> appliesTo = new ArrayList<>();

  /**
   * The extra clauses of a composed rule, empty for a rule which checks a single thing.
   *
   * <p>A composed rule keeps its first clause in the plain {@code targetField} / {@code condition}
   * / {@code conditionValue} fields, so that everything which reads a simple rule — the rule
   * manager table, the YAML exporter, an override block — goes on working unchanged.
   */
  private List<RuleClause> additionalClauses = new ArrayList<>();

  /** How the clauses combine. Meaningless, and ignored, for a rule with a single clause. */
  private RuleCombinator combinator = RuleCombinator.ALL_OF;

  public CustomLintRule() {
    this.id = UUID.randomUUID().toString();
    this.enabled = true;
    this.severity = "WARNING";
    this.additionalParameters = new HashMap<>();
  }

  public CustomLintRule(
      String name, RuleTarget target, String targetField, RuleCondition condition) {
    this();
    this.name = name;
    this.target = target;
    this.targetField = targetField;
    this.condition = condition;
  }

  // Getters and setters

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getSeverity() {
    return severity;
  }

  public void setSeverity(String severity) {
    this.severity = severity;
  }

  public RuleTarget getTarget() {
    return target;
  }

  public void setTarget(RuleTarget target) {
    this.target = target;
  }

  public String getTargetField() {
    return targetField;
  }

  public void setTargetField(String targetField) {
    this.targetField = targetField;
  }

  public RuleCondition getCondition() {
    return condition;
  }

  public void setCondition(RuleCondition condition) {
    this.condition = condition;
  }

  public String getConditionValue() {
    return conditionValue;
  }

  public void setConditionValue(String conditionValue) {
    this.conditionValue = conditionValue;
  }

  public Map<String, Object> getAdditionalParameters() {
    return additionalParameters;
  }

  public void setAdditionalParameters(Map<String, Object> additionalParameters) {
    this.additionalParameters =
        additionalParameters != null ? additionalParameters : new HashMap<>();
  }

  public Object getParameter(String key) {
    return additionalParameters.get(key);
  }

  public void setParameter(String key, Object value) {
    additionalParameters.put(key, value);
  }

  public String getPackId() {
    return packId;
  }

  public void setPackId(String packId) {
    this.packId = packId != null ? packId : RulePackIds.PROJECT;
  }

  public List<String> getAppliesTo() {
    return appliesTo;
  }

  public void setAppliesTo(List<String> appliesTo) {
    this.appliesTo = appliesTo != null ? new ArrayList<>(appliesTo) : new ArrayList<>();
  }

  /**
   * True when this rule should run against the given transform or action plugin id.
   *
   * <p>An empty {@code appliesTo} keeps the original behaviour of applying to everything. Matching
   * ignores case, because plugin ids are written by hand in YAML.
   */
  public boolean appliesToPlugin(String pluginId) {
    if (appliesTo.isEmpty()) {
      return true;
    }
    if (pluginId == null) {
      return false;
    }
    return appliesTo.stream().anyMatch(id -> id.equalsIgnoreCase(pluginId.trim()));
  }

  public RulePackOwner getPackOwner() {
    return packOwner;
  }

  public void setPackOwner(RulePackOwner packOwner) {
    this.packOwner = packOwner != null ? packOwner : RulePackOwner.PROJECT;
  }

  /**
   * True when the rule comes from a third-party vendor pack (read-only, not part of Apache Hop).
   */
  public boolean isVendorPack() {
    return packOwner == RulePackOwner.VENDOR;
  }

  /** True when the rule is owned by the project and can be edited or deleted in Rule Manager. */
  public boolean isProjectEditable() {
    return packOwner == RulePackOwner.PROJECT;
  }

  public List<RuleClause> getAdditionalClauses() {
    return additionalClauses;
  }

  public void setAdditionalClauses(List<RuleClause> additionalClauses) {
    this.additionalClauses = additionalClauses == null ? new ArrayList<>() : additionalClauses;
  }

  public RuleCombinator getCombinator() {
    return combinator;
  }

  public void setCombinator(RuleCombinator combinator) {
    this.combinator = combinator == null ? RuleCombinator.ALL_OF : combinator;
  }

  /** Whether this rule checks more than one thing. */
  public boolean isComposed() {
    return !additionalClauses.isEmpty();
  }

  /**
   * Everything this rule checks, the first clause included.
   *
   * @return the clauses, in the order they were written
   */
  public List<RuleClause> getClauses() {
    List<RuleClause> clauses = new ArrayList<>();
    clauses.add(new RuleClause(targetField, condition, conditionValue));
    clauses.addAll(additionalClauses);
    return clauses;
  }

  public CustomLintRule copy() {
    CustomLintRule copy = new CustomLintRule();
    copy.id = this.id;
    copy.name = this.name;
    copy.description = this.description;
    copy.enabled = this.enabled;
    copy.severity = this.severity;
    copy.target = this.target;
    copy.targetField = this.targetField;
    copy.condition = this.condition;
    copy.conditionValue = this.conditionValue;
    copy.packId = this.packId;
    copy.packOwner = this.packOwner;
    copy.additionalParameters = new HashMap<>(this.additionalParameters);
    copy.appliesTo = new ArrayList<>(this.appliesTo);
    copy.combinator = this.combinator;
    copy.additionalClauses = new ArrayList<>();
    for (RuleClause clause : this.additionalClauses) {
      copy.additionalClauses.add(clause.copy());
    }
    return copy;
  }

  /** Generate a rule ID based on target and condition (like TRANS-001, DB-001, etc.) */
  public String generateRuleId() {
    if (id != null && !id.trim().isEmpty()) {
      return id;
    }
    String prefix;
    switch (target) {
      case PIPELINE:
        prefix = "TRANS";
        break;
      case WORKFLOW:
        prefix = "WORK";
        break;
      case DATABASE_CONNECTION:
        prefix = "DB";
        break;
      case TRANSFORM:
        prefix = "STEP";
        break;
      case ACTION:
        prefix = "ACTION";
        break;
      case HOP:
        prefix = "HOP";
        break;
      default:
        prefix = "RULE";
    }

    // Use hash of rule details to create consistent ID
    int hash = Math.abs((target.name() + targetField + condition.name()).hashCode());
    return String.format("%s-%03d", prefix, hash % 1000);
  }

  /** Get a human-readable summary of the rule */
  public String getSummary() {
    StringBuilder summary = new StringBuilder();
    summary.append(target.getDisplayName());
    if (targetField != null && !targetField.isEmpty()) {
      summary.append(".").append(targetField);
    }
    summary.append(" ").append(condition.getDisplayName().toLowerCase());
    if (conditionValue != null && !conditionValue.isEmpty()) {
      summary.append(" '").append(conditionValue).append("'");
    }
    return summary.toString();
  }

  @Override
  public String toString() {
    return name != null ? name : getSummary();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    CustomLintRule that = (CustomLintRule) obj;
    return id != null ? id.equals(that.id) : that.id == null;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
