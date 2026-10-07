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
import org.apache.hop.workflow.action.IAction;

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
    return fromCheckResults(remarks, fileName, null);
  }

  /**
   * Convert Hop's own verify remarks, letting the project's native rules decide how each is
   * reported.
   *
   * @param classifier the native rules in force, or null to keep every remark as the transform
   *     wrote it
   */
  public static List<LintResult> fromCheckResults(
      List<ICheckResult> remarks, String fileName, NativeCheckClassifier classifier) {
    List<LintResult> results = new ArrayList<>();
    if (remarks == null) {
      return results;
    }
    for (ICheckResult remark : remarks) {
      LintResult lintResult = fromCheckResult(remark, fileName, classifier);
      if (lintResult != null) {
        results.add(lintResult);
      }
    }
    return results;
  }

  public static LintResult fromCheckResult(ICheckResult remark, String fileName) {
    return fromCheckResult(remark, fileName, null);
  }

  public static LintResult fromCheckResult(
      ICheckResult remark, String fileName, NativeCheckClassifier classifier) {
    if (remark == null || remark.getType() == ICheckResult.TYPE_RESULT_OK) {
      return null;
    }

    String severity = LintSeverity.fromCheckResultType(remark.getType());
    String ruleId = remark.getErrorCode();
    List<String> aliasRuleIds = new ArrayList<>();

    if (classifier != null && !classifier.isEmpty()) {
      NativeCheckClassifier.Classification classification = classifier.classify(remark);
      if (classification == null) {
        // A rule that names this check and is switched off: the project has said the check is
        // not one it wants to hear about, so there is no finding at all.
        return null;
      }
      severity = classification.severity();
      String classifyingRule = classification.ruleId();
      if (!Utils.isEmpty(classifyingRule)) {
        if (Utils.isEmpty(ruleId)) {
          ruleId = classifyingRule;
        } else if (classification.narrowed()) {
          // A rule naming the plugin or the check is the more specific id. The code stays an
          // alias, so a suppression written against it survives the project adding that rule.
          aliasRuleIds.add(ruleId);
          ruleId = classifyingRule;
        } else {
          // A check with its own error code keeps it, so a project can address that one check.
          // The blanket rule names every remark, and taking its id would collapse them all into
          // one. It is kept alongside, so what a project wrote against it still applies.
          aliasRuleIds.add(classifyingRule);
        }
      }
      // A narrowed rule took the id above, so the rule covering every remark would stop naming
      // this finding. It is kept as the last name, after the check's own code, so a suppression
      // written against either one still applies.
      if (!Utils.isEmpty(classification.blanketRuleId())) {
        aliasRuleIds.add(classification.blanketRuleId());
      }
    }

    if (Utils.isEmpty(ruleId)) {
      ruleId = "HOP-CHECK";
    }

    LintSourceRef sourceRef = sourceFromCheckResult(remark.getSourceInfo());
    String ruleName =
        remark.getSourceInfo() != null ? remark.getSourceInfo().getName() : "Hop verify";

    return new LintResult(
        ruleId,
        ruleName,
        severity,
        remark.getText(),
        fileName,
        sourceRef,
        LintResult.Origin.HOP_NATIVE,
        aliasRuleIds);
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
    // Hop's own workflow verify reports the IAction as the source, not its ActionMeta.
    if (source instanceof ActionMeta || source instanceof IAction) {
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
