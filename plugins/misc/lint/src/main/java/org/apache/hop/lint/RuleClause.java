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

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

/**
 * One thing a rule checks: a field, a condition, and the value the condition compares against.
 *
 * <p>A rule which checks a single thing carries exactly one of these. A rule with an {@code allOf:}
 * or {@code anyOf:} block carries several, so that "a Table Input whose SQL selects everything
 * <em>and</em> has no row limit" can be one finding rather than two rules that each fire on their
 * own.
 */
public class RuleClause {

  private static final Class<?> PKG = RuleClause.class; // for i18n purposes

  private String targetField;
  private RuleCondition condition;
  private String conditionValue;

  public RuleClause() {}

  public RuleClause(String targetField, RuleCondition condition, String conditionValue) {
    this.targetField = targetField;
    this.condition = condition;
    this.conditionValue = conditionValue;
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

  /**
   * Why this clause can never report anything, or null when it can be evaluated.
   *
   * <p>A value the condition cannot use, such as {@code abc} for a maximum or {@code [a-} for a
   * pattern, made the rule report nothing at all and log an exception on every run, which reads as
   * a clean result. A missing field, condition or value is not judged here.
   *
   * @param target what the rule checks, for the fields the linter itself provides
   * @param scoped whether the rule is narrowed with {@code appliesTo}; such a rule can read the
   *     plugin's own settings, whose types are not known here, so only the value is judged
   */
  public String valueProblem(RuleTarget target, boolean scoped) {
    if (condition == null || Utils.isEmpty(targetField)) {
      return null;
    }
    if (!scoped
        && RuleTargetFields.getFieldsForTarget(target).contains(targetField)
        && !RuleTargetFields.getCompatibleConditions(targetField).contains(condition)) {
      return BaseMessages.getString(
          PKG, "RuleClause.Problem.ConditionDoesNotFit", condition.name(), targetField);
    }
    if (!condition.requiresValue() || Utils.isEmpty(conditionValue)) {
      return null;
    }
    String value = conditionValue.trim();
    switch (condition) {
      case MAX_VALUE, MIN_VALUE, EXACT_VALUE:
        try {
          Double.parseDouble(value);
        } catch (NumberFormatException e) {
          return BaseMessages.getString(
              PKG, "RuleClause.Problem.NotANumber", condition.name(), conditionValue);
        }
        break;
      case MAX_COLLECTION_SIZE, MIN_COLLECTION_SIZE:
        try {
          Integer.parseInt(value);
        } catch (NumberFormatException e) {
          return BaseMessages.getString(
              PKG, "RuleClause.Problem.NotAWholeNumber", condition.name(), conditionValue);
        }
        break;
      case MATCHES_PATTERN, NOT_MATCHES_PATTERN:
        try {
          Pattern.compile(conditionValue);
        } catch (PatternSyntaxException e) {
          return BaseMessages.getString(
              PKG,
              "RuleClause.Problem.InvalidPattern",
              condition.name(),
              conditionValue,
              e.getDescription());
        }
        break;
      default:
        break;
    }
    return null;
  }

  /** The first problem {@link #valueProblem} finds in any of these clauses, or null. */
  public static String valueProblem(List<RuleClause> clauses, RuleTarget target, boolean scoped) {
    for (RuleClause clause : clauses) {
      String problem = clause.valueProblem(target, scoped);
      if (problem != null) {
        return problem;
      }
    }
    return null;
  }

  public RuleClause copy() {
    return new RuleClause(targetField, condition, conditionValue);
  }

  /** How this clause reads in a finding, e.g. {@code sql NOT_MATCHES_PATTERN "select *"}. */
  public String describe() {
    StringBuilder text = new StringBuilder();
    text.append(targetField).append(' ').append(condition);
    if (conditionValue != null && !conditionValue.isEmpty()) {
      text.append(" '").append(conditionValue).append('\'');
    }
    return text.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RuleClause other)) {
      return false;
    }
    return Objects.equals(targetField, other.targetField)
        && condition == other.condition
        && Objects.equals(conditionValue, other.conditionValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetField, condition, conditionValue);
  }
}
