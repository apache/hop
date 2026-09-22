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
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.CheckTransformsExtension;
import org.apache.hop.pipeline.PipelineMeta;

/** Adds YAML/custom lint findings to the native pipeline verify Problems tab. */
@ExtensionPoint(
    id = "PipelineVerifyLintExtension",
    extensionPointId = "AfterCheckTransforms",
    description = "Adds lint rule results to pipeline verify output")
public class PipelineVerifyLintExtension implements IExtensionPoint<CheckTransformsExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, CheckTransformsExtension extension)
      throws HopException {
    try {
      if (LintVerifyReentrancy.isDrivenByLinter()) {
        // The linter started this check and is already collecting policy findings; adding
        // them here would report every one of them twice.
        return;
      }

      LinterConfigPlugin config = LinterConfigPlugin.getInstance();
      if (!config.isLinterEnabled() || !config.isIncludeLintInPipelineVerify()) {
        return;
      }

      PipelineMeta pipelineMeta = extension.getPipelineMeta();
      if (pipelineMeta == null) {
        return;
      }

      String fileName = LintPathUtils.normalizePath(pipelineMeta.getFilename());
      HopLinter linter = new HopLinter();
      linter.loadConfigurationForContext(new java.io.File(fileName));

      if (linter.isExcluded(fileName)) {
        // The project keeps this file out of linting. Hop's own verify output is left alone —
        // the user asked for it — but nothing lint-related is added to it or reported from it.
        return;
      }

      // Hop collected its own remarks before this point, so they have passed no suppression yet.
      // The policy findings added below have: applyPolicy suppresses them as it builds them.
      linter.removeSuppressed(extension.getRemarks(), fileName);

      // Read while the list still holds Hop's own remarks alone. The blanket native rule names no
      // plugin and no message, so it matches anything put in front of it - including a policy
      // finding turned into a remark, whose own rule id it would overwrite.
      List<LintResult> results =
          new ArrayList<>(linter.fromNativeRemarks(extension.getRemarks(), fileName));

      List<LintResult> policyResults =
          linter.applyPolicy(linter.runPolicyRules(pipelineMeta, fileName), fileName);
      List<ICheckResult> policyRemarks =
          LintCheckResultAdapter.toCheckResults(policyResults, pipelineMeta);
      extension.getRemarks().addAll(policyRemarks);

      // Through the same conversion as before. This view has always reported a policy finding as
      // Hop's own verify output renders it, and reporting it differently here would leave the
      // Problems bar disagreeing with the background lint about the same file.
      results.addAll(LintCheckResultAdapter.fromCheckResults(policyRemarks, fileName));
      List<LintResult> verifyViewResults = LintResultDeduplicator.deduplicate(results);
      LintResultsManager.getInstance().updateResultsForFile(fileName, verifyViewResults);
      LintProblemsBarManager.getInstance().updateProblemsBar(fileName);

      log.logDetailed(
          "Added "
              + policyResults.size()
              + " lint result(s) to pipeline verify output for "
              + fileName);
    } catch (Exception e) {
      log.logError("Error adding lint results to pipeline verify: " + e.getMessage(), e);
    }
  }
}
