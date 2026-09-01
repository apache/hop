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
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;

/**
 * Runs Hop's built-in pipeline/workflow verify checks and converts results to {@link LintResult}.
 */
public final class HopNativeCheckRunner {

  private HopNativeCheckRunner() {}

  public static List<LintResult> runNativeChecks(
      Object hopObject,
      String fileName,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    List<ICheckResult> remarks = new ArrayList<>();
    IProgressMonitor monitor = new NullProgressMonitor();

    if (!(hopObject instanceof PipelineMeta) && !(hopObject instanceof WorkflowMeta)) {
      return List.of();
    }

    // Suppressed: the caller already collected the policy findings, and checkTransforms fires
    // the verify extension point, which would add them a second time.
    LintVerifyReentrancy.runDrivenByLinter(
        () -> {
          if (hopObject instanceof PipelineMeta) {
            ((PipelineMeta) hopObject)
                .checkTransforms(remarks, false, monitor, variables, metadataProvider);
          } else {
            ((WorkflowMeta) hopObject)
                .checkActions(remarks, false, monitor, variables, metadataProvider);
          }
        });

    return LintCheckResultAdapter.fromCheckResults(remarks, fileName);
  }

  private static final class NullProgressMonitor implements IProgressMonitor {
    @Override
    public void beginTask(String task, int work) {}

    @Override
    public void done() {}

    @Override
    public void setTaskName(String name) {}

    @Override
    public void subTask(String name) {}

    @Override
    public boolean isCanceled() {
      return false;
    }

    @Override
    public void worked(int work) {}
  }
}
