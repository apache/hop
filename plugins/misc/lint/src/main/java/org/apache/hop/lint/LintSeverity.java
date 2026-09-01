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

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.util.Utils;

/** Severity helpers shared by lint results, verify integration, and pre-commit checks. */
public final class LintSeverity {

  public enum FailOn {
    ERROR,
    WARNING,
    NONE
  }

  /** The severities a finding can carry, as opposed to the thresholds a build can fail on. */
  public enum Level {
    ERROR,
    WARNING,
    INFO
  }

  private LintSeverity() {}

  public static FailOn parseFailOn(String value) {
    if (Utils.isEmpty(value)) {
      return FailOn.ERROR;
    }
    try {
      return FailOn.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      return FailOn.ERROR;
    }
  }

  public static boolean meetsFailOnThreshold(String severity, FailOn failOn) {
    if (failOn == null || failOn == FailOn.NONE) {
      return false;
    }
    if (Utils.isEmpty(severity)) {
      return false;
    }
    if ("ERROR".equalsIgnoreCase(severity)) {
      return true;
    }
    return failOn == FailOn.WARNING && "WARNING".equalsIgnoreCase(severity);
  }

  public static int toCheckResultType(String severity) {
    if ("ERROR".equalsIgnoreCase(severity)) {
      return ICheckResult.TYPE_RESULT_ERROR;
    }
    if ("WARNING".equalsIgnoreCase(severity)) {
      return ICheckResult.TYPE_RESULT_WARNING;
    }
    if ("INFO".equalsIgnoreCase(severity)) {
      return ICheckResult.TYPE_RESULT_COMMENT;
    }
    return ICheckResult.TYPE_RESULT_OK;
  }

  public static String fromCheckResultType(int type) {
    if (type == ICheckResult.TYPE_RESULT_ERROR) {
      return "ERROR";
    }
    if (type == ICheckResult.TYPE_RESULT_WARNING) {
      return "WARNING";
    }
    if (type == ICheckResult.TYPE_RESULT_COMMENT) {
      return "INFO";
    }
    return "INFO";
  }
}
