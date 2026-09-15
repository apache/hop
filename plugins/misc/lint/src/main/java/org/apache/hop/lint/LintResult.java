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
    this.ruleId = ruleId;
    this.ruleName = ruleName;
    this.severity = severity;
    this.message = message;
    this.fileName = fileName;
    this.source = source;
    this.origin = origin != null ? origin : Origin.LINT;
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
