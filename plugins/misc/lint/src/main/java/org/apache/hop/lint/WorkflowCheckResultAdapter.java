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
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.util.Utils;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Converts lint results to Hop workflow verify remarks ({@link ICheckResult}). */
public final class WorkflowCheckResultAdapter {

  private WorkflowCheckResultAdapter() {}

  public static List<ICheckResult> toCheckResults(
      List<LintResult> lintResults, WorkflowMeta workflowMeta) {
    List<ICheckResult> remarks = new ArrayList<>();
    if (lintResults == null) {
      return remarks;
    }
    for (LintResult lintResult : lintResults) {
      ICheckResult checkResult = toCheckResult(lintResult, workflowMeta);
      if (checkResult != null) {
        remarks.add(checkResult);
      }
    }
    return remarks;
  }

  public static ICheckResult toCheckResult(LintResult lintResult, WorkflowMeta workflowMeta) {
    if (lintResult == null) {
      return null;
    }
    ICheckResultSource source = resolveWorkflowSource(lintResult, workflowMeta);
    String text = formatCheckText(lintResult);
    return new CheckResult(
        LintSeverity.toCheckResultType(lintResult.getSeverity()),
        lintResult.getRuleId(),
        text,
        source);
  }

  private static String formatCheckText(LintResult lintResult) {
    StringBuilder text = new StringBuilder();
    text.append("[").append(lintResult.getRuleId()).append("] ");
    if (!Utils.isEmpty(lintResult.getRuleName())) {
      text.append(lintResult.getRuleName()).append(": ");
    }
    text.append(lintResult.getMessage());
    return text.toString();
  }

  private static ICheckResultSource resolveWorkflowSource(
      LintResult lintResult, WorkflowMeta workflowMeta) {
    if (lintResult.getSource() != null && workflowMeta != null) {
      LintSourceRef source = lintResult.getSource();
      if (source.getKind() == LintSourceRef.Kind.ACTION && source.hasName()) {
        ActionMeta action = workflowMeta.findAction(source.getName());
        if (action != null && action.getAction() != null) {
          return action.getAction();
        }
      }
      if (source.getKind() == LintSourceRef.Kind.WORKFLOW) {
        return null;
      }
    }
    return null;
  }
}
