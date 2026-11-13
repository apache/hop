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

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.cluster.SparkClusterTarget;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.RestDatabricksJobsClient;
import org.apache.hop.databricks.deploy.DatabricksJobSpecFactory.ClusterTarget;
import org.apache.hop.databricks.metadata.DatabricksConnection;

/**
 * The Databricks cluster type of the native Spark engine: a pipeline run from Hop runs as a
 * one-time job run on a Databricks cluster, with the fat jar and the project package staged on a
 * Unity Catalog volume. The Spark engine plugin asks for it through the {@code SparkClusterTarget}
 * extension point, so neither plugin depends on the other.
 */
@ExtensionPoint(
    id = "DatabricksSparkClusterTarget",
    extensionPointId = "SparkClusterTarget",
    description = "Runs native Spark pipelines on a Databricks cluster")
public class DatabricksSparkClusterTargetExtensionPoint
    implements IExtensionPoint<SparkClusterTarget> {

  /** The code of the native Spark engine's Databricks cluster type. */
  public static final String CLUSTER_TYPE = "DATABRICKS";

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, SparkClusterTarget target)
      throws HopException {
    if (!CLUSTER_TYPE.equals(target.getClusterType())) {
      return;
    }
    String connectionName = target.getSetting(SparkClusterTarget.SETTING_CONNECTION);
    if (StringUtils.isBlank(connectionName)) {
      throw new HopException(
          "A Databricks run needs a Databricks connection: set it in the Spark run"
              + " configuration.");
    }
    if (target.getMetadataProvider() == null) {
      throw new HopException(
          "A Databricks run reads Databricks connection '"
              + connectionName
              + "' from the project metadata, and there is none.");
    }
    DatabricksConnection connection =
        target.getMetadataProvider().getSerializer(DatabricksConnection.class).load(connectionName);
    if (connection == null) {
      throw new HopException("Databricks connection '" + connectionName + "' does not exist.");
    }
    String compute = target.getSetting(SparkClusterTarget.SETTING_COMPUTE).trim();
    if (DatabricksJobSpecFactory.isServerlessToken(compute)) {
      throw new HopException(
          "Serverless compute runs Spark Connect, and the native Spark engine needs a"
              + " SparkContext: use an all-purpose cluster in Dedicated access mode, or"
              + " new_cluster.");
    }
    ClusterTarget clusterTarget =
        DatabricksJobSpecFactory.resolveClusterTarget(
            compute,
            orDefault(
                target.getSetting(SparkClusterTarget.SETTING_RUNTIME),
                DatabricksJobSpecFactory.DEFAULT_SPARK_VERSION),
            orDefault(
                target.getSetting(SparkClusterTarget.SETTING_NODE_TYPE),
                DatabricksJobSpecFactory.DEFAULT_NODE_TYPE_ID),
            orDefault(
                target.getSetting(SparkClusterTarget.SETTING_WORKERS),
                String.valueOf(DatabricksJobSpecFactory.DEFAULT_NUM_WORKERS)),
            null,
            DatabricksJobSpecFactory.DEFAULT_ENVIRONMENT_KEY,
            DatabricksJobSpecFactory.DEFAULT_ENVIRONMENT_CLIENT);
    String base =
        HopSparkDeployHelper.normalizeBase(
            target.getSetting(SparkClusterTarget.SETTING_STAGING_FOLDER));

    DatabricksJobsClient client = RestDatabricksJobsClient.create(connection, variables);
    target.setStagingStore(new DatabricksVolumeStagingStore(client, base));
    target.setSubmitter(
        new DatabricksRunSubmitter(
            log,
            client,
            clusterTarget,
            "Databricks "
                + variables.resolve(connection.getHost())
                + " ("
                + (clusterTarget.mode() == DatabricksJobSpecFactory.ComputeMode.NEW_CLUSTER
                    ? "a new job cluster"
                    : "cluster " + compute)
                + ")"));
  }

  private static String orDefault(String value, String defaultValue) {
    return StringUtils.isBlank(value) ? defaultValue : value.trim();
  }
}
