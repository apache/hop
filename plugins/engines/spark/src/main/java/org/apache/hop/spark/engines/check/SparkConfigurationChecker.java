/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.engines.check;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;

/**
 * Runs the checks of a native Spark run configuration that apply to its cluster type and deploy
 * mode, in order. A check whose prerequisite did not pass is skipped rather than failing for the
 * same reason; a check that throws is a failure with the reason.
 */
public class SparkConfigurationChecker {

  private static final Class<?> PKG = SparkConfigurationChecker.class;

  private final List<ISparkConfigurationCheck> checks;

  public SparkConfigurationChecker(List<ISparkConfigurationCheck> checks) {
    this.checks = checks;
  }

  /** The checks of every cluster type, then the ones of each cluster type. */
  public static SparkConfigurationChecker standard() {
    return new SparkConfigurationChecker(
        List.of(
            new SettingsCheck(),
            new ClusterTypeCheck(),
            new VersionsCheck(),
            new FatJarCheck(),
            new StagingFolderCheck(),
            new StagingUrlCheck(),
            new StandaloneMasterCheck(),
            new StandaloneStatusCheck(),
            new StandaloneRestCheck(),
            new YarnConfigurationCheck(),
            new YarnKerberosCheck(),
            new YarnResourceManagerCheck(),
            new YarnQueueCheck(),
            new YarnContainerSizeCheck(),
            new KubernetesApiCheck(),
            new KubernetesPermissionsCheck(),
            new KubernetesServiceAccountCheck(),
            new KubernetesImageCheck(),
            new KubernetesClientModeCheck(),
            new DatabricksCheck(),
            new TestJobCheck()));
  }

  public List<ICheckResult> check(
      SparkPipelineRunConfiguration configuration, IVariables variables, IProgressMonitor monitor) {
    return check(configuration, variables, null, monitor);
  }

  /**
   * @param metadataProvider the metadata to run the test job with, null to leave the test job out
   */
  public List<ICheckResult> check(
      SparkPipelineRunConfiguration configuration,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      IProgressMonitor monitor) {
    return check(configuration, variables, metadataProvider, monitor, metadataProvider != null);
  }

  /**
   * @param metadataProvider the project metadata, for the connections the checks use and the test
   *     job
   * @param testJob whether the test job runs after the checks
   */
  public List<ICheckResult> check(
      SparkPipelineRunConfiguration configuration,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      IProgressMonitor monitor,
      boolean testJob) {
    try (SparkCheckContext context =
        new SparkCheckContext(configuration, variables, metadataProvider, monitor, testJob)) {
      return check(context, monitor);
    }
  }

  List<ICheckResult> check(SparkCheckContext context, IProgressMonitor monitor) {
    List<ISparkConfigurationCheck> applicable = new ArrayList<>();
    for (ISparkConfigurationCheck check : checks) {
      if (check.appliesTo(context)) {
        applicable.add(check);
      }
    }
    if (monitor != null) {
      monitor.beginTask(BaseMessages.getString(PKG, "SparkCheck.Progress"), applicable.size());
    }
    List<ICheckResult> results = new ArrayList<>();
    for (ISparkConfigurationCheck check : applicable) {
      if (monitor != null && monitor.isCanceled()) {
        break;
      }
      if (monitor != null) {
        monitor.subTask(check.getName());
      }
      ICheckResult result = run(check, context);
      context.putResult(check.getId(), result);
      results.add(result);
      if (monitor != null) {
        monitor.worked(1);
      }
    }
    if (monitor != null) {
      monitor.done();
    }
    return results;
  }

  static ICheckResult run(ISparkConfigurationCheck check, SparkCheckContext context) {
    for (String required : check.getRequiredChecks()) {
      ICheckResult before = context.getResult(required);
      if (before != null && before.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        return new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "SparkCheck.Skipped", before.getSourceInfo().getName()),
            check);
      }
    }
    try {
      return check.run(context);
    } catch (Exception | LinkageError e) {
      // A class conflict is a failure of this check, not of the whole test
      return new CheckResult(
          ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(PKG, "SparkCheck.Failed", oneLine(e)),
          check);
    }
  }

  /** A reason on one line: Hop exception messages start with a line break, and nest causes. */
  static String oneLine(Throwable e) {
    String message = Const.NVL(e.getMessage(), e.toString()).trim();
    return message.replaceAll("\\s*\\R\\s*", ": ");
  }
}
