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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Converts between lint results and Hop native verify results ({@link ICheckResult}). */
public final class LintCheckResultAdapter {

  private LintCheckResultAdapter() {}

  public static List<ICheckResult> toCheckResults(
      List<LintResult> lintResults, PipelineMeta pipelineMeta) {
    List<ICheckResult> remarks = new ArrayList<>();
    if (lintResults == null) {
      return remarks;
    }
    for (LintResult lintResult : lintResults) {
      ICheckResult checkResult = toCheckResult(lintResult, pipelineMeta);
      if (checkResult != null) {
        remarks.add(checkResult);
      }
    }
    return remarks;
  }

  public static ICheckResult toCheckResult(LintResult lintResult, PipelineMeta pipelineMeta) {
    if (lintResult == null) {
      return null;
    }
    ICheckResultSource source = resolvePipelineSource(lintResult, pipelineMeta);
    String text = formatCheckText(lintResult);
    CheckResult checkResult =
        new CheckResult(
            LintSeverity.toCheckResultType(lintResult.getSeverity()),
            lintResult.getRuleId(),
            text,
            source);
    return checkResult;
  }

  public static List<LintResult> fromCheckResults(List<ICheckResult> remarks, String fileName) {
    List<LintResult> results = new ArrayList<>();
    if (remarks == null) {
      return results;
    }
    for (ICheckResult remark : remarks) {
      LintResult lintResult = fromCheckResult(remark, fileName);
      if (lintResult != null) {
        results.add(lintResult);
      }
    }
    return results;
  }

  public static LintResult fromCheckResult(ICheckResult remark, String fileName) {
    if (remark == null || remark.getType() == ICheckResult.TYPE_RESULT_OK) {
      return null;
    }

    String ruleId = remark.getErrorCode();
    if (Utils.isEmpty(ruleId)) {
      ruleId = "HOP-CHECK";
    }

    LintSourceRef sourceRef = sourceFromCheckResult(remark.getSourceInfo());
    String ruleName =
        remark.getSourceInfo() != null ? remark.getSourceInfo().getName() : "Hop verify";
    String severity = LintSeverity.fromCheckResultType(remark.getType());

    return new LintResult(
        ruleId,
        ruleName,
        severity,
        remark.getText(),
        fileName,
        sourceRef,
        LintResult.Origin.HOP_NATIVE);
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

  private static ICheckResultSource resolvePipelineSource(
      LintResult lintResult, PipelineMeta pipelineMeta) {
    if (lintResult.getSource() != null && pipelineMeta != null) {
      LintSourceRef source = lintResult.getSource();
      if (source.getKind() == LintSourceRef.Kind.TRANSFORM && source.hasName()) {
        TransformMeta transform = pipelineMeta.findTransform(source.getName());
        if (transform != null) {
          return transform;
        }
      }
      if (source.getKind() == LintSourceRef.Kind.PIPELINE) {
        return null;
      }
    }
    return null;
  }

  private static LintSourceRef sourceFromCheckResult(ICheckResultSource source) {
    if (source instanceof TransformMeta) {
      return LintSourceRef.transform(source.getName());
    }
    if (source instanceof ActionMeta) {
      return LintSourceRef.action(source.getName());
    }
    if (source instanceof PipelineMeta) {
      return LintSourceRef.pipeline(source.getName());
    }
    if (source instanceof WorkflowMeta) {
      return LintSourceRef.workflow(source.getName());
    }
    if (source != null && !Utils.isEmpty(source.getName())) {
      return LintSourceRef.file(source.getName());
    }
    return null;
  }
}
