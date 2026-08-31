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

      List<LintResult> policyResults = linter.runPolicyRules(pipelineMeta, fileName);
      extension
          .getRemarks()
          .addAll(LintCheckResultAdapter.toCheckResults(policyResults, pipelineMeta));

      List<LintResult> verifyViewResults =
          LintResultDeduplicator.deduplicate(
              LintCheckResultAdapter.fromCheckResults(extension.getRemarks(), fileName));
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
