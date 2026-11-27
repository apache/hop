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

package org.apache.hop.spark.engines.check;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.cluster.SparkClusterTarget;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.submit.SparkClusterRun;

/**
 * Databricks: the connection, compute and volume folder are set, and the Databricks plugin reaches
 * the workspace and reads the volume with the connection. Nothing runs on the cluster.
 */
public class DatabricksCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "Databricks";

  public DatabricksCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.DATABRICKS;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(ClusterTypeCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    SparkPipelineRunConfiguration config = context.getConfiguration();
    List<String> missing = new ArrayList<>();
    if (context.resolve(config.getDatabricksConnection()).isEmpty()) {
      missing.add("the Databricks connection");
    }
    String compute = context.resolve(config.getDatabricksCompute());
    if (compute.isEmpty()) {
      missing.add("the compute (a cluster id or new_cluster)");
    }
    String folder = context.resolve(config.getStagingFolder());
    if (folder.isEmpty()) {
      missing.add(
          "the staging folder (a volume folder such as /Volumes/catalog/schema/volume/hop)");
    }
    if (!missing.isEmpty()) {
      return error(
          "Not set: " + String.join(", ", missing) + ".",
          "Fill them in on the Cluster tab: Hop submits the run with the connection, runs it on"
              + " the compute and uploads the fat jar and the project package to the folder.");
    }
    if ("serverless".equalsIgnoreCase(compute)) {
      return error(
          "The compute is serverless, which runs Spark Connect: the native Spark engine needs a"
              + " SparkContext.",
          "Use an all-purpose cluster in Dedicated access mode, or new_cluster.");
    }
    if (!folder.startsWith("/Volumes/") && !folder.startsWith("dbfs:/")) {
      return warning(
          "The staging folder '" + folder + "' is not a Unity Catalog volume path.",
          "Use a volume folder such as /Volumes/catalog/schema/volume/hop: newer workspaces"
              + " disable the DBFS root.");
    }
    if (context.getMetadataProvider() == null) {
      return info(
          "The settings are complete. The connection was not tried: there is no project"
              + " metadata to read it from.");
    }
    try {
      SparkClusterTarget target =
          SparkClusterRun.platformTarget(
              LogChannel.GENERAL, context.getVariables(), context.getMetadataProvider(), config);
      int files = target.getStagingStore().list().size();
      target.getSubmitter().close();
      return ok(
          "The connection reaches the workspace and reads "
              + folder
              + " ("
              + files
              + " staged files). The run goes to "
              + target.getSubmitter().describe()
              + ".");
    } catch (Exception e) {
      return error(
          "The volume folder can't be read with the connection: " + oneLine(e) + ".",
          "Check the workspace URL and token of the connection (Test in the connection editor),"
              + " that the volume exists, and that the token's user may read and write it"
              + " (USE CATALOG, USE SCHEMA, READ VOLUME, WRITE VOLUME).");
    }
  }

  private static String oneLine(Exception e) {
    String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    return message.strip().replaceAll("\\s+", " ");
  }
}
