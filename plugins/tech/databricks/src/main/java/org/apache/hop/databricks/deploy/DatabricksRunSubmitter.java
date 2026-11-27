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

package org.apache.hop.databricks.deploy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.cluster.ISparkClusterSubmitter;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.DatabricksRunStatus;
import org.apache.hop.databricks.deploy.DatabricksJobSpecFactory.ClusterTarget;
import org.apache.hop.databricks.deploy.DatabricksJobSpecFactory.ComputeMode;

/**
 * Runs a native Spark pipeline on Databricks for the native Spark engine's Databricks cluster type:
 * one one-time job run ({@code runs/submit}) of {@code MainSpark}, with the staged fat jar as its
 * library. The run id is the driver id: the engine follows it with {@code runs/get} and Stop
 * cancels it.
 */
public class DatabricksRunSubmitter implements ISparkClusterSubmitter {

  static final String ARG_PROJECT_PACKAGE = "--HopProjectPackage=";
  static final String ARG_PIPELINE_PATH = "--HopPipelinePath=";
  static final String ARG_RUN_CONFIGURATION = "--HopRunConfigurationName=";

  /** The file name of a fat jar that Hop staged: {@code hop-native-<12 hex>.jar}. */
  static final Pattern HOP_FAT_JAR = Pattern.compile("hop-native-[0-9a-f]{12}\\.jar");

  private final ILogChannel log;

  private final DatabricksJobsClient client;
  private final ClusterTarget clusterTarget;
  private final String description;

  /**
   * @param description where runs go, for the log: the workspace and the compute
   */
  public DatabricksRunSubmitter(
      ILogChannel log,
      DatabricksJobsClient client,
      ClusterTarget clusterTarget,
      String description) {
    this.log = log;
    this.client = client;
    this.clusterTarget = clusterTarget;
    this.description = description;
  }

  @Override
  public String submit(
      String appResource,
      String mainClass,
      List<String> appArgs,
      Map<String, String> sparkProperties,
      Map<String, String> environmentVariables)
      throws HopException {
    if (!DatabricksJobSpecFactory.MAIN_CLASS.equals(mainClass)) {
      throw new HopException(
          "A Databricks run starts " + DatabricksJobSpecFactory.MAIN_CLASS + ", not " + mainClass);
    }
    MainSparkLaunchSpec launch =
        new MainSparkLaunchSpec(
            argument(appArgs, ARG_PIPELINE_PATH),
            null,
            argument(appArgs, ARG_RUN_CONFIGURATION),
            argument(appArgs, ARG_PROJECT_PACKAGE),
            null);
    if (clusterTarget.mode() == ComputeMode.EXISTING_CLUSTER) {
      removeOlderFatJars(clusterTarget.existingClusterId(), appResource);
    }
    String runName = sparkProperties == null ? null : sparkProperties.get("spark.app.name");
    String body =
        DatabricksJobSpecFactory.buildSubmitRunJson(runName, clusterTarget, appResource, launch);
    return String.valueOf(client.submitRun(body));
  }

  @Override
  public SparkDriverStatus status(String id) throws HopException {
    DatabricksRunStatus run = client.getRun(runId(id));
    return new SparkDriverStatus(
        run.toStatusVariable(),
        run.getRunPageUrl(),
        run.getStateMessage(),
        run.getLifeCycleState().isTerminal(),
        run.isSuccess());
  }

  @Override
  public String kill(String id) throws HopException {
    client.cancelRun(runId(id));
    return "Databricks run " + id + " is being cancelled";
  }

  @Override
  public String describe() {
    return description;
  }

  @Override
  public void close() {
    client.close();
  }

  /**
   * An all-purpose cluster keeps the libraries of earlier runs until it restarts, and their classes
   * can win over the fat jar of this run: after a Hop upgrade or a plugin change the driver then
   * runs old code. Ask Databricks to remove the other fat jars Hop staged at the next restart, and
   * say so. A token that may not read or change the cluster's libraries only loses this help.
   */
  void removeOlderFatJars(String clusterId, String appResource) {
    String current = fileName(appResource);
    try {
      List<String> older = new ArrayList<>();
      for (String jar : client.getClusterJarLibraries(clusterId)) {
        String name = fileName(jar);
        if (HOP_FAT_JAR.matcher(name).matches() && !name.equals(current)) {
          older.add(jar);
        }
      }
      if (older.isEmpty()) {
        return;
      }
      client.uninstallClusterJarLibraries(clusterId, older);
      log.logBasic(
          "WARNING: cluster "
              + clusterId
              + " still has "
              + older.size()
              + " fat jar(s) of earlier Hop runs installed ("
              + String.join(", ", older.stream().map(DatabricksRunSubmitter::fileName).toList())
              + "). Their classes can take precedence over "
              + current
              + ", so the run may use out-of-date Hop code. Hop asked Databricks to remove them;"
              + " that happens when the cluster restarts. Restart the cluster if this run fails"
              + " with classes or settings that look out of date.");
    } catch (HopException e) {
      log.logBasic(
          "Note: Hop can't check whether cluster "
              + clusterId
              + " still has fat jars of earlier runs installed ("
              + StringUtils.normalizeSpace(e.getMessage())
              + "). Give the token the 'libraries' scope, or restart the cluster after you"
              + " upgrade Hop or change its plugins.");
    }
  }

  private static String fileName(String path) {
    String p = StringUtils.defaultString(path);
    return p.substring(p.lastIndexOf('/') + 1);
  }

  /** The value of a {@code --Hop...=} argument of MainSpark. */
  static String argument(List<String> appArgs, String prefix) throws HopException {
    if (appArgs != null) {
      for (String arg : appArgs) {
        if (arg != null && arg.startsWith(prefix)) {
          return arg.substring(prefix.length());
        }
      }
    }
    throw new HopException("A Databricks run needs the MainSpark argument " + prefix + "...");
  }

  private static long runId(String id) throws HopException {
    if (StringUtils.isBlank(id)) {
      throw new HopException("No Databricks run id");
    }
    try {
      return Long.parseLong(id.trim());
    } catch (NumberFormatException e) {
      throw new HopException("'" + id + "' is not a Databricks run id", e);
    }
  }
}
