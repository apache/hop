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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Data class to hold the result of a single linting violation. */
public class LintResult {

  public enum Origin {
    LINT,
    HOP_NATIVE
  }

  private final String ruleId;
  private final String ruleName;
  private final String severity;
  private final String message;
  private final String fileName;
  private final LintSourceRef source;
  private final Origin origin;
  private final String aliasRuleId;

  public LintResult(
      String ruleId, String ruleName, String severity, String message, String fileName) {
    this(ruleId, ruleName, severity, message, fileName, null, Origin.LINT);
  }

  public LintResult(
      String ruleId,
      String ruleName,
      String severity,
      String message,
      String fileName,
      LintSourceRef source,
      Origin origin) {
    this(ruleId, ruleName, severity, message, fileName, source, origin, null);
  }

  /**
   * @param aliasRuleId the other id a native remark answers to: the rule that classified it when it
   *     is reported under its own error code, or that error code when a narrowed rule named it;
   *     null otherwise
   */
  public LintResult(
      String ruleId,
      String ruleName,
      String severity,
      String message,
      String fileName,
      LintSourceRef source,
      Origin origin,
      String aliasRuleId) {
    this.ruleId = ruleId;
    this.ruleName = ruleName;
    this.severity = severity;
    this.message = message;
    this.fileName = fileName;
    this.source = source;
    this.origin = origin != null ? origin : Origin.LINT;
    this.aliasRuleId = aliasRuleId;
  }

  public String getRuleId() {
    return ruleId;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getSeverity() {
    return severity;
  }

  public String getMessage() {
    return message;
  }

  public String getFileName() {
    return fileName;
  }

  public LintSourceRef getSource() {
    return source;
  }

  public Origin getOrigin() {
    return origin;
  }

  /**
   * The other id this finding answers to, or null.
   *
   * <p>A native remark with its own error code is reported under that code, and answers to the rule
   * that classified it too, so what a project wrote against {@code HOP-CHECK} still applies. When a
   * rule naming the check wins instead, the finding answers to the error code as well, so a
   * suppression written against the code keeps working after a project adds such a rule.
   */
  public String getAliasRuleId() {
    return aliasRuleId;
  }

  /** The rule ids this finding answers to: its own, then its alias, if any. */
  public List<String> getRuleIds() {
    if (aliasRuleId == null || aliasRuleId.equalsIgnoreCase(ruleId)) {
      return Collections.singletonList(ruleId);
    }
    return Arrays.asList(ruleId, aliasRuleId);
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder()
            .append("[")
            .append(severity)
            .append("] ")
            .append(ruleId)
            .append(" - ")
            .append(ruleName)
            .append(": ")
            .append(message)
            .append(" (File: ")
            .append(fileName);
    if (source != null && source.hasName()) {
      sb.append(", ").append(source.getKind()).append(": ").append(source.getName());
    }
    sb.append(")");
    return sb.toString();
  }
}
