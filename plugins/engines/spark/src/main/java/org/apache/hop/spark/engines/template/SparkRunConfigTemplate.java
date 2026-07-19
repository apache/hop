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

package org.apache.hop.spark.engines.template;

import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;

/**
 * Named presets for {@link SparkPipelineRunConfiguration} fields. Pure data — no SWT. Apply via
 * {@link #applyTo(SparkPipelineRunConfiguration)}.
 */
public enum SparkRunConfigTemplate {
  LOCAL_EXECUTION(
      "Local execution",
      "Embedded Spark in the Hop process using all local cores (local[*]).",
      "local[*]",
      "Apache Hop",
      "",
      "spark.ui.enabled=false",
      "",
      "",
      ""),
  LOCAL_CAPPED(
      "Local (capped cores)",
      "Embedded Spark limited to 4 local cores (local[4]).",
      "local[4]",
      "Apache Hop",
      "",
      "spark.ui.enabled=false",
      "",
      "",
      ""),
  SPARK_STANDALONE(
      "Spark standalone server",
      "Connect Hop as a Spark client to a standalone master (replace host).",
      "spark://host:7077",
      "Apache Hop",
      "",
      "spark.ui.enabled=false",
      "2g",
      "2g",
      "2"),
  SPARK_SUBMIT(
      "spark-submit (cluster)",
      "Leave master empty so spark-submit --master wins; use a native-provided fat jar.",
      "",
      "Apache Hop",
      "",
      "spark.ui.enabled=false",
      "",
      "",
      ""),
  DATABRICKS(
      "Databricks",
      "Cluster / submit oriented placeholders. Master empty for submit-style runs; set paths for your workspace.",
      "",
      "Apache Hop",
      "",
      """
spark.ui.enabled=false
spark.executor.cores=3
spark.driver.cores=1
spark.default.parallelism=12
spark.sql.shuffle.partitions=12
      """,
      "",
      "",
      ""),
  YARN_CLIENT(
      "YARN client",
      "Submit to YARN in client deploy mode (cluster provides Spark).",
      "yarn",
      "Apache Hop",
      "",
      "spark.ui.enabled=false\nspark.submit.deployMode=client",
      "2g",
      "2g",
      "2"),
  LAKEHOUSE_LOCAL(
      "Lakehouse local",
      "Local[*] for Delta/Iceberg pipelines (connectors on plugin classpath; session plan applies extensions).",
      "local[*]",
      "Apache Hop",
      "",
      "spark.ui.enabled=false",
      "",
      "",
      "");

  private final String displayName;
  private final String description;
  private final String sparkMaster;
  private final String sparkAppName;
  private final String fatJar;
  private final String sparkConfigs;
  private final String driverMemory;
  private final String executorMemory;
  private final String executorCores;

  SparkRunConfigTemplate(
      String displayName,
      String description,
      String sparkMaster,
      String sparkAppName,
      String fatJar,
      String sparkConfigs,
      String driverMemory,
      String executorMemory,
      String executorCores) {
    this.displayName = displayName;
    this.description = description;
    this.sparkMaster = sparkMaster;
    this.sparkAppName = sparkAppName;
    this.fatJar = fatJar;
    this.sparkConfigs = sparkConfigs;
    this.driverMemory = driverMemory;
    this.executorMemory = executorMemory;
    this.executorCores = executorCores;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDescription() {
    return description;
  }

  /** Labels for selection dialogs (same order as {@link #values()}). */
  public static String[] displayNames() {
    return Arrays.stream(values())
        .map(SparkRunConfigTemplate::getDisplayName)
        .toArray(String[]::new);
  }

  public static SparkRunConfigTemplate fromDisplayName(String name) {
    if (name == null) {
      return null;
    }
    for (SparkRunConfigTemplate t : values()) {
      if (t.displayName.equals(name)) {
        return t;
      }
    }
    return null;
  }

  /**
   * Apply this template's field values onto {@code config}. Does not change temp location or
   * plugins to stage (keeps operator environment defaults).
   */
  public void applyTo(SparkPipelineRunConfiguration config) {
    Objects.requireNonNull(config, "config");
    config.setSparkMaster(sparkMaster);
    config.setSparkAppName(sparkAppName);
    config.setFatJar(fatJar);
    config.setSparkConfigs(sparkConfigs);
    config.setDriverMemory(driverMemory);
    config.setExecutorMemory(executorMemory);
    config.setExecutorCores(executorCores);
  }

  /**
   * True if the configuration looks customized relative to a fresh {@link
   * SparkPipelineRunConfiguration} default (used for overwrite confirmation).
   */
  public static boolean looksCustomized(SparkPipelineRunConfiguration config) {
    if (config == null) {
      return false;
    }
    SparkPipelineRunConfiguration def = new SparkPipelineRunConfiguration();
    if (!Objects.equals(nz(config.getSparkMaster()), nz(def.getSparkMaster()))) {
      return true;
    }
    if (!Objects.equals(nz(config.getSparkAppName()), nz(def.getSparkAppName()))) {
      return true;
    }
    if (StringUtils.isNotEmpty(config.getFatJar())) {
      return true;
    }
    if (StringUtils.isNotEmpty(config.getSparkConfigs())) {
      return true;
    }
    if (StringUtils.isNotEmpty(config.getDriverMemory())
        || StringUtils.isNotEmpty(config.getExecutorMemory())
        || StringUtils.isNotEmpty(config.getExecutorCores())) {
      return true;
    }
    if (StringUtils.isNotEmpty(config.getPathSchemeMap())) {
      return true;
    }
    return StringUtils.isNotEmpty(config.getPluginsToStage());
  }

  private static String nz(String s) {
    return s == null ? "" : s;
  }
}
