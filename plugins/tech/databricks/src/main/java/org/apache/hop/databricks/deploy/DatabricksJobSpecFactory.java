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
import org.apache.hop.core.exception.HopException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/** Builds Jobs API JSON for Hop Native Spark (MainSpark JAR task) on Databricks. */
public final class DatabricksJobSpecFactory {

  /** Driver entry on the native Spark fat jar (see engines/spark MainSpark). */
  public static final String MAIN_CLASS = "org.apache.hop.spark.run.MainSpark";

  public static final String DEFAULT_TASK_KEY = "hop_native_spark";

  private DatabricksJobSpecFactory() {}

  /**
   * Create-job body: name + single spark_jar_task on an existing cluster with jar library.
   *
   * @param jobName Databricks job name
   * @param existingClusterId required existing cluster id
   * @param jarDbfsPath dbfs path to fat jar (with or without dbfs: prefix)
   * @param pipelineDbfsPath pipeline .hpl on DBFS
   * @param metadataDbfsPath metadata JSON on DBFS
   * @param runConfigName pipeline run configuration name inside the metadata export
   */
  public static String buildCreateJobJson(
      String jobName,
      String existingClusterId,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    JSONObject root = new JSONObject();
    root.put("name", require(jobName, "job name"));
    JSONArray tasks = new JSONArray();
    tasks.add(
        buildJarTask(
            existingClusterId, jarDbfsPath, pipelineDbfsPath, metadataDbfsPath, runConfigName));
    root.put("tasks", tasks);
    return root.toJSONString();
  }

  /**
   * jobs/reset body: job_id + new_settings (same shape as create, without wrapping name-only
   * quirks).
   */
  public static String buildResetJobJson(
      long jobId,
      String jobName,
      String existingClusterId,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    JSONObject root = new JSONObject();
    root.put("job_id", jobId);
    JSONObject settings = new JSONObject();
    settings.put("name", require(jobName, "job name"));
    JSONArray tasks = new JSONArray();
    tasks.add(
        buildJarTask(
            existingClusterId, jarDbfsPath, pipelineDbfsPath, metadataDbfsPath, runConfigName));
    settings.put("tasks", tasks);
    root.put("new_settings", settings);
    return root.toJSONString();
  }

  /** One-time runs/submit body with the same JAR task. */
  public static String buildSubmitRunJson(
      String runName,
      String existingClusterId,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    JSONObject root = new JSONObject();
    if (StringUtils.isNotBlank(runName)) {
      root.put("run_name", runName);
    }
    JSONArray tasks = new JSONArray();
    tasks.add(
        buildJarTask(
            existingClusterId, jarDbfsPath, pipelineDbfsPath, metadataDbfsPath, runConfigName));
    root.put("tasks", tasks);
    return root.toJSONString();
  }

  @SuppressWarnings("unchecked")
  static JSONObject buildJarTask(
      String existingClusterId,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    JSONObject task = new JSONObject();
    task.put("task_key", DEFAULT_TASK_KEY);
    task.put("existing_cluster_id", require(existingClusterId, "existing cluster id"));

    JSONObject jarTask = new JSONObject();
    jarTask.put("main_class_name", MAIN_CLASS);
    JSONArray params = new JSONArray();
    params.add(toDbfsUri(pipelineDbfsPath));
    params.add(toDbfsUri(metadataDbfsPath));
    params.add(require(runConfigName, "run configuration name"));
    jarTask.put("parameters", params);
    task.put("spark_jar_task", jarTask);

    JSONArray libraries = new JSONArray();
    JSONObject lib = new JSONObject();
    lib.put("jar", toDbfsUri(jarDbfsPath));
    libraries.add(lib);
    task.put("libraries", libraries);
    return task;
  }

  /** Ensure dbfs:/ prefix for library and parameter paths. */
  public static String toDbfsUri(String path) throws HopException {
    String p = require(path, "dbfs path");
    if (p.startsWith("dbfs:")) {
      return p;
    }
    if (!p.startsWith("/")) {
      p = "/" + p;
    }
    return "dbfs:" + p;
  }

  private static String require(String value, String label) throws HopException {
    if (StringUtils.isBlank(value)) {
      throw new HopException("Missing " + label);
    }
    return value.trim();
  }
}
