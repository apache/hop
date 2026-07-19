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

/**
 * Paths and names passed as {@code spark_jar_task.parameters} to {@code
 * org.apache.hop.spark.run.MainSpark}.
 *
 * <p>When {@link #projectPackagePath} or {@link #configFilePath} is set, the job uses named {@code
 * --Hop*} arguments (required for project package). Otherwise three positional arguments match the
 * classic deploy layout.
 *
 * @param pipelinePath entry pipeline path (Volume/DBFS URI, or package-relative path when using a
 *     project package)
 * @param metadataPath metadata JSON path; may be null when a project package embeds metadata
 * @param runConfigName Native Spark pipeline run configuration name
 * @param projectPackagePath optional Spark project package zip on the workspace
 * @param configFilePath optional environment / described-variables JSON for {@code --HopConfigFile}
 */
public record MainSparkLaunchSpec(
    String pipelinePath,
    String metadataPath,
    String runConfigName,
    String projectPackagePath,
    String configFilePath) {

  public MainSparkLaunchSpec {
    if (StringUtils.isBlank(pipelinePath)) {
      throw new IllegalArgumentException("pipelinePath is required");
    }
    if (StringUtils.isBlank(runConfigName)) {
      throw new IllegalArgumentException("runConfigName is required");
    }
  }

  /** Classic three-argument deploy (pipeline + metadata + run config). */
  public static MainSparkLaunchSpec positional(
      String pipelinePath, String metadataPath, String runConfigName) {
    return new MainSparkLaunchSpec(pipelinePath, metadataPath, runConfigName, null, null);
  }

  /** True when named {@code --Hop*} parameters must be used. */
  public boolean useNamedParameters() {
    return StringUtils.isNotBlank(projectPackagePath) || StringUtils.isNotBlank(configFilePath);
  }

  public boolean hasProjectPackage() {
    return StringUtils.isNotBlank(projectPackagePath);
  }

  public boolean hasConfigFile() {
    return StringUtils.isNotBlank(configFilePath);
  }
}
