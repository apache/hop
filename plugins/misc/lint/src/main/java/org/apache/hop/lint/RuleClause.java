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

import java.util.Objects;

/**
 * One thing a rule checks: a field, a condition, and the value the condition compares against.
 *
 * <p>A rule which checks a single thing carries exactly one of these. A rule with an {@code allOf:}
 * or {@code anyOf:} block carries several, so that "a Table Input whose SQL selects everything
 * <em>and</em> has no row limit" can be one finding rather than two rules that each fire on their
 * own.
 */
public class RuleClause {

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
