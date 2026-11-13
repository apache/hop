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

package org.apache.hop.spark.engines;

import org.apache.hop.core.fatjar.IHasStagingFolder;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;

public interface ISparkPipelineEngineRunConfiguration
    extends IPipelineEngineRunConfiguration, IHasStagingFolder {

  String getSparkMaster();

  String getSparkAppName();

  String getFatJar();

  /**
   * Optional folder with Hadoop client configuration ({@code core-site.xml}, {@code yarn-site.xml},
   * ...) for sessions Hop starts itself. Empty: {@code HADOOP_CONF_DIR} / {@code YARN_CONF_DIR} for
   * YARN masters.
   */
  String getHadoopConfDir();

  String getSparkConfigs();

  String getDriverMemory();

  String getExecutorMemory();

  String getExecutorCores();

  /**
   * Optional multi-line path scheme map for Spark Dataset / Lake PATH I/O ({@code from=to} per
   * line, e.g. {@code s3=s3a}). Rewrites only the URI scheme before {@code load}/{@code save};
   * classic Hop VFS paths are unaffected.
   */
  String getPathSchemeMap();

  /**
   * Default execution mode for generic (mapPartitions) Hop transforms: {@code DISTRIBUTED} or
   * {@code DRIVER_ONLY}. Per-transform overrides can force either mode via transform attributes.
   */
  String getGenericTransformRunMode();

  /** Where the driver runs; null means {@link SparkDeployMode#CLIENT}. */
  SparkDeployMode getDeployMode();

  /** Hop VFS folder that cluster mode uploads the fat jar and project package to. */
  String getStagingFolder();

  /** The staging folder as the cluster reads it; empty when the cluster uses the same URL. */
  String getStagingClusterUrl();

  /** Submission endpoint; empty to derive it from the master (standalone: port 6066). */
  String getSubmitUrl();

  /** Where the pipeline runs: the chosen cluster type, or the one the Spark master points to. */
  SparkClusterType getEffectiveClusterType();

  /** Databricks: the name of the Databricks connection. */
  String getDatabricksConnection();

  /** Databricks: an all-purpose cluster id, or {@code new_cluster}. */
  String getDatabricksCompute();

  /** Databricks {@code new_cluster}: the runtime of the job cluster. */
  String getDatabricksRuntime();

  /** Databricks {@code new_cluster}: the node type of the job cluster. */
  String getDatabricksNodeType();

  /** Databricks {@code new_cluster}: the number of workers of the job cluster. */
  String getDatabricksWorkers();
}
