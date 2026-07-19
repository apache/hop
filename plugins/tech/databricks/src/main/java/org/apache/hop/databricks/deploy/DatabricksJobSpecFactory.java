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
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/** Builds Jobs API JSON for Hop Native Spark (MainSpark JAR task) on Databricks. */
public final class DatabricksJobSpecFactory {

  /** Driver entry on the native Spark fat jar (see engines/spark MainSpark). */
  public static final String MAIN_CLASS = "org.apache.hop.spark.run.MainSpark";

  public static final String DEFAULT_TASK_KEY = "hop_native_spark";

  /**
   * Sentinel: emit a Jobs API {@code new_cluster} object (classic ephemeral job cluster) instead of
   * {@code existing_cluster_id}.
   */
  public static final String NEW_CLUSTER_TOKEN = "new_cluster";

  /**
   * Sentinel: serverless workspace compute — job-level {@code environments} + task {@code
   * environment_key} (no cluster fields).
   */
  public static final String SERVERLESS_TOKEN = "serverless";

  public static final String DEFAULT_SPARK_VERSION = "18.2.x-scala2.13";

  /** AWS default: local NVMe (i3) avoids required EBS on m5.* job clusters. */
  public static final String DEFAULT_NODE_TYPE_ID = "i3.xlarge";

  public static final int DEFAULT_NUM_WORKERS = 1;
  public static final String DEFAULT_ENVIRONMENT_KEY = "default";

  /** Serverless environment client version (e.g. {@code 4} for Spark 4.x runtime). */
  public static final String DEFAULT_ENVIRONMENT_CLIENT = "4";

  public enum ComputeMode {
    EXISTING_CLUSTER,
    NEW_CLUSTER,
    SERVERLESS
  }

  /**
   * Resolved compute target for job create/reset.
   *
   * @param mode existing cluster, classic job cluster, or serverless environment
   * @param existingClusterId set when {@link ComputeMode#EXISTING_CLUSTER}
   * @param newCluster set when {@link ComputeMode#NEW_CLUSTER}
   * @param environmentKey set when {@link ComputeMode#SERVERLESS}
   * @param environmentClient set when {@link ComputeMode#SERVERLESS} (spec.client)
   */
  public record ClusterTarget(
      ComputeMode mode,
      String existingClusterId,
      JSONObject newCluster,
      String environmentKey,
      String environmentClient) {}

  private DatabricksJobSpecFactory() {}

  /**
   * Create-job body: name + single spark_jar_task with jar library.
   *
   * @param jobName Databricks job display name
   * @param clusterTarget resolved compute target (existing cluster, new cluster, or serverless)
   * @param jarDbfsPath workspace or volume path of the fat jar
   * @param launch MainSpark parameters (pipeline / metadata / package / env)
   */
  public static String buildCreateJobJson(
      String jobName, ClusterTarget clusterTarget, String jarDbfsPath, MainSparkLaunchSpec launch)
      throws HopException {
    JSONObject root = new JSONObject();
    root.put("name", require(jobName, "job name"));
    String jarUri = toDbfsUri(jarDbfsPath);
    applyEnvironments(root, clusterTarget, jarUri);
    JSONArray tasks = new JSONArray();
    tasks.add(buildJarTask(clusterTarget, jarUri, launch));
    root.put("tasks", tasks);
    return root.toJSONString();
  }

  /**
   * Create-job body: name + single spark_jar_task with jar library.
   *
   * @param jobName Databricks job display name
   * @param clusterTarget resolved compute target (existing cluster, new cluster, or serverless)
   * @param jarDbfsPath workspace or volume path of the fat jar
   * @param pipelineDbfsPath path of the uploaded pipeline file
   * @param metadataDbfsPath path of the uploaded metadata JSON
   * @param runConfigName optional pipeline run configuration name
   */
  public static String buildCreateJobJson(
      String jobName,
      ClusterTarget clusterTarget,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    return buildCreateJobJson(
        jobName,
        clusterTarget,
        jarDbfsPath,
        MainSparkLaunchSpec.positional(pipelineDbfsPath, metadataDbfsPath, runConfigName));
  }

  /**
   * Create-job with cluster field + optional new_cluster object (legacy callers). Prefer {@link
   * #buildCreateJobJson(String, ClusterTarget, String, String, String, String)}.
   */
  public static String buildCreateJobJson(
      String jobName,
      String clusterField,
      JSONObject newCluster,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    ClusterTarget target = clusterTargetFromFieldAndObject(clusterField, newCluster);
    return buildCreateJobJson(
        jobName, target, jarDbfsPath, pipelineDbfsPath, metadataDbfsPath, runConfigName);
  }

  /** Backward-compatible create: existing cluster only. */
  public static String buildCreateJobJson(
      String jobName,
      String existingClusterId,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    return buildCreateJobJson(
        jobName,
        existingClusterId,
        null,
        jarDbfsPath,
        pipelineDbfsPath,
        metadataDbfsPath,
        runConfigName);
  }

  /** jobs/reset body: job_id + new_settings. */
  public static String buildResetJobJson(
      long jobId,
      String jobName,
      ClusterTarget clusterTarget,
      String jarDbfsPath,
      MainSparkLaunchSpec launch)
      throws HopException {
    JSONObject root = new JSONObject();
    root.put("job_id", jobId);
    JSONObject settings = new JSONObject();
    settings.put("name", require(jobName, "job name"));
    String jarUri = toDbfsUri(jarDbfsPath);
    applyEnvironments(settings, clusterTarget, jarUri);
    JSONArray tasks = new JSONArray();
    tasks.add(buildJarTask(clusterTarget, jarUri, launch));
    settings.put("tasks", tasks);
    root.put("new_settings", settings);
    return root.toJSONString();
  }

  /** jobs/reset body: job_id + new_settings. */
  public static String buildResetJobJson(
      long jobId,
      String jobName,
      ClusterTarget clusterTarget,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    return buildResetJobJson(
        jobId,
        jobName,
        clusterTarget,
        jarDbfsPath,
        MainSparkLaunchSpec.positional(pipelineDbfsPath, metadataDbfsPath, runConfigName));
  }

  public static String buildResetJobJson(
      long jobId,
      String jobName,
      String clusterField,
      JSONObject newCluster,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    ClusterTarget target = clusterTargetFromFieldAndObject(clusterField, newCluster);
    return buildResetJobJson(
        jobId, jobName, target, jarDbfsPath, pipelineDbfsPath, metadataDbfsPath, runConfigName);
  }

  /** Backward-compatible reset: existing cluster only. */
  public static String buildResetJobJson(
      long jobId,
      String jobName,
      String existingClusterId,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    return buildResetJobJson(
        jobId,
        jobName,
        existingClusterId,
        null,
        jarDbfsPath,
        pipelineDbfsPath,
        metadataDbfsPath,
        runConfigName);
  }

  /** One-time runs/submit body with the same JAR task. */
  public static String buildSubmitRunJson(
      String runName, ClusterTarget clusterTarget, String jarDbfsPath, MainSparkLaunchSpec launch)
      throws HopException {
    JSONObject root = new JSONObject();
    if (StringUtils.isNotBlank(runName)) {
      root.put("run_name", runName);
    }
    String jarUri = toDbfsUri(jarDbfsPath);
    applyEnvironments(root, clusterTarget, jarUri);
    JSONArray tasks = new JSONArray();
    tasks.add(buildJarTask(clusterTarget, jarUri, launch));
    root.put("tasks", tasks);
    return root.toJSONString();
  }

  /** One-time runs/submit body with the same JAR task. */
  public static String buildSubmitRunJson(
      String runName,
      ClusterTarget clusterTarget,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    return buildSubmitRunJson(
        runName,
        clusterTarget,
        jarDbfsPath,
        MainSparkLaunchSpec.positional(pipelineDbfsPath, metadataDbfsPath, runConfigName));
  }

  public static String buildSubmitRunJson(
      String runName,
      String clusterField,
      JSONObject newCluster,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    ClusterTarget target = clusterTargetFromFieldAndObject(clusterField, newCluster);
    return buildSubmitRunJson(
        runName, target, jarDbfsPath, pipelineDbfsPath, metadataDbfsPath, runConfigName);
  }

  public static boolean isNewClusterToken(String clusterField) {
    return StringUtils.isNotBlank(clusterField)
        && NEW_CLUSTER_TOKEN.equalsIgnoreCase(clusterField.trim());
  }

  public static boolean isServerlessToken(String clusterField) {
    return StringUtils.isNotBlank(clusterField)
        && SERVERLESS_TOKEN.equalsIgnoreCase(clusterField.trim());
  }

  /**
   * Build the Jobs API {@code new_cluster} object from structured fields and/or a full JSON
   * override.
   */
  @SuppressWarnings("unchecked")
  public static JSONObject buildNewClusterObject(
      String sparkVersion, String nodeTypeId, String numWorkers, String newClusterJsonOptional)
      throws HopException {
    if (StringUtils.isNotBlank(newClusterJsonOptional)) {
      try {
        Object parsed = new JSONParser().parse(newClusterJsonOptional.trim());
        if (!(parsed instanceof JSONObject jsonObject)) {
          throw new HopException("new_cluster JSON must be a JSON object");
        }
        return jsonObject;
      } catch (HopException e) {
        throw e;
      } catch (Exception e) {
        throw new HopException("Unable to parse new_cluster JSON", e);
      }
    }

    String version =
        StringUtils.isNotBlank(sparkVersion) ? sparkVersion.trim() : DEFAULT_SPARK_VERSION;
    String node = StringUtils.isNotBlank(nodeTypeId) ? nodeTypeId.trim() : DEFAULT_NODE_TYPE_ID;
    int workers = Const.toInt(numWorkers, DEFAULT_NUM_WORKERS);
    if (workers < 0) {
      throw new HopException("new_cluster num_workers must be >= 0");
    }

    JSONObject cluster = new JSONObject();
    cluster.put("spark_version", version);
    cluster.put("node_type_id", node);
    cluster.put("num_workers", workers);
    return cluster;
  }

  /**
   * Resolve cluster field into existing id, classic {@code new_cluster}, or serverless environment.
   *
   * @param environmentKey serverless environment_key (default {@code default})
   * @param environmentClient serverless {@code spec.client} (default {@code 4})
   */
  public static ClusterTarget resolveClusterTarget(
      String clusterField,
      String sparkVersion,
      String nodeTypeId,
      String numWorkers,
      String newClusterJsonOptional,
      String environmentKey,
      String environmentClient)
      throws HopException {
    if (StringUtils.isBlank(clusterField)) {
      throw new HopException(
          "Missing cluster field: existing cluster id, new_cluster, or serverless");
    }
    String field = clusterField.trim();
    if (isServerlessToken(field)) {
      String envKey =
          StringUtils.isNotBlank(environmentKey) ? environmentKey.trim() : DEFAULT_ENVIRONMENT_KEY;
      String client =
          StringUtils.isNotBlank(environmentClient)
              ? environmentClient.trim()
              : DEFAULT_ENVIRONMENT_CLIENT;
      return new ClusterTarget(ComputeMode.SERVERLESS, null, null, envKey, client);
    }
    if (isNewClusterToken(field)) {
      return new ClusterTarget(
          ComputeMode.NEW_CLUSTER,
          null,
          buildNewClusterObject(sparkVersion, nodeTypeId, numWorkers, newClusterJsonOptional),
          null,
          null);
    }
    return new ClusterTarget(ComputeMode.EXISTING_CLUSTER, field, null, null, null);
  }

  /**
   * @deprecated use {@link #resolveClusterTarget(String, String, String, String, String, String,
   *     String)}
   */
  @Deprecated(since = "2.19")
  public static ClusterTarget resolveClusterTarget(
      String clusterField,
      String sparkVersion,
      String nodeTypeId,
      String numWorkers,
      String newClusterJsonOptional)
      throws HopException {
    return resolveClusterTarget(
        clusterField,
        sparkVersion,
        nodeTypeId,
        numWorkers,
        newClusterJsonOptional,
        DEFAULT_ENVIRONMENT_KEY,
        DEFAULT_ENVIRONMENT_CLIENT);
  }

  private static ClusterTarget clusterTargetFromFieldAndObject(
      String clusterField, JSONObject newCluster) throws HopException {
    if (StringUtils.isBlank(clusterField)) {
      throw new HopException(
          "Missing cluster field: existing cluster id, new_cluster, or serverless");
    }
    String field = clusterField.trim();
    if (isServerlessToken(field)) {
      return new ClusterTarget(
          ComputeMode.SERVERLESS, null, null, DEFAULT_ENVIRONMENT_KEY, DEFAULT_ENVIRONMENT_CLIENT);
    }
    if (isNewClusterToken(field)) {
      if (newCluster == null || newCluster.isEmpty()) {
        throw new HopException(
            "new_cluster object is required when cluster field is '" + NEW_CLUSTER_TOKEN + "'");
      }
      return new ClusterTarget(ComputeMode.NEW_CLUSTER, null, newCluster, null, null);
    }
    return new ClusterTarget(ComputeMode.EXISTING_CLUSTER, field, null, null, null);
  }

  /**
   * Serverless jobs declare libraries on the environment ({@code spec.dependencies}), not on the
   * task. Classic modes leave environments unset.
   */
  @SuppressWarnings("unchecked")
  private static void applyEnvironments(
      JSONObject jobOrSettings, ClusterTarget clusterTarget, String jarUri) throws HopException {
    if (clusterTarget.mode() != ComputeMode.SERVERLESS) {
      return;
    }
    JSONArray environments = new JSONArray();
    JSONObject env = new JSONObject();
    env.put("environment_key", require(clusterTarget.environmentKey(), "environment_key"));
    JSONObject spec = new JSONObject();
    spec.put("client", require(clusterTarget.environmentClient(), "environment client"));
    // Serverless JAR tasks require java_dependencies on the environment (not task libraries[])
    JSONArray javaDependencies = new JSONArray();
    javaDependencies.add(require(jarUri, "jar path"));
    spec.put("java_dependencies", javaDependencies);
    env.put("spec", spec);
    environments.add(env);
    jobOrSettings.put("environments", environments);
  }

  static JSONObject buildJarTask(
      ClusterTarget clusterTarget,
      String jarUri,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    return buildJarTask(
        clusterTarget,
        jarUri,
        MainSparkLaunchSpec.positional(pipelineDbfsPath, metadataDbfsPath, runConfigName));
  }

  @SuppressWarnings("unchecked")
  static JSONObject buildJarTask(
      ClusterTarget clusterTarget, String jarUri, MainSparkLaunchSpec launch) throws HopException {
    JSONObject task = new JSONObject();
    task.put("task_key", DEFAULT_TASK_KEY);

    switch (clusterTarget.mode()) {
      case SERVERLESS ->
          task.put("environment_key", require(clusterTarget.environmentKey(), "environment_key"));
      case NEW_CLUSTER -> {
        if (clusterTarget.newCluster() == null || clusterTarget.newCluster().isEmpty()) {
          throw new HopException(
              "new_cluster object is required when cluster field is '" + NEW_CLUSTER_TOKEN + "'");
        }
        task.put("new_cluster", clusterTarget.newCluster());
      }
      case EXISTING_CLUSTER ->
          task.put(
              "existing_cluster_id",
              require(clusterTarget.existingClusterId(), "existing cluster id"));
      default -> throw new HopException("Unsupported compute mode: " + clusterTarget.mode());
    }

    JSONObject jarTask = new JSONObject();
    jarTask.put("main_class_name", MAIN_CLASS);
    jarTask.put("parameters", buildMainSparkParameters(launch));
    task.put("spark_jar_task", jarTask);

    if (clusterTarget.mode() != ComputeMode.SERVERLESS) {
      JSONArray libraries = new JSONArray();
      JSONObject lib = new JSONObject();
      lib.put(
          "jar", jarUri.startsWith("/") || jarUri.startsWith("dbfs:") ? jarUri : toDbfsUri(jarUri));
      libraries.add(lib);
      task.put("libraries", libraries);
    }
    return task;
  }

  /**
   * Build {@code spark_jar_task.parameters} for MainSpark. Uses named {@code --Hop*} args when a
   * project package or env config is present; otherwise classic positional pipeline/metadata/rc.
   */
  static JSONArray buildMainSparkParameters(MainSparkLaunchSpec launch) throws HopException {
    if (launch == null) {
      throw new HopException("MainSpark launch specification is required");
    }
    String runConfig = require(launch.runConfigName(), "run configuration name");
    JSONArray params = new JSONArray();
    if (!launch.useNamedParameters()) {
      params.add(parameterPath(launch.pipelinePath()));
      params.add(parameterPath(require(launch.metadataPath(), "metadata path")));
      params.add(runConfig);
      return params;
    }

    if (launch.hasProjectPackage()) {
      params.add("--HopProjectPackage=" + parameterPath(launch.projectPackagePath()));
    }
    params.add("--HopPipelinePath=" + parameterPath(launch.pipelinePath()));
    if (StringUtils.isNotBlank(launch.metadataPath())) {
      params.add("--HopMetadataPath=" + parameterPath(launch.metadataPath()));
    } else if (!launch.hasProjectPackage()) {
      throw new HopException(
          "Metadata path is required unless a Spark project package is uploaded");
    }
    params.add("--HopRunConfigurationName=" + runConfig);
    if (launch.hasConfigFile()) {
      params.add("--HopConfigFile=" + parameterPath(launch.configFilePath()));
    }
    return params;
  }

  /**
   * Paths that are package-relative (no leading slash / scheme) stay as-is; workspace paths go
   * through {@link #toDbfsUri(String)}.
   */
  static String parameterPath(String path) throws HopException {
    String p = require(path, "path");
    // Relative pipeline path inside a project package (e.g. pipelines/hello.hpl)
    if (!p.startsWith("/")
        && !p.startsWith("dbfs:")
        && !p.contains("://")
        && !p.matches("^[A-Za-z]:[\\\\/].*")) {
      return p;
    }
    return toDbfsUri(p);
  }

  /**
   * @deprecated internal bridge for older call sites that pass field + object
   */
  @Deprecated(since = "2.19")
  static JSONObject buildJarTask(
      String clusterField,
      JSONObject newCluster,
      String jarDbfsPath,
      String pipelineDbfsPath,
      String metadataDbfsPath,
      String runConfigName)
      throws HopException {
    return buildJarTask(
        clusterTargetFromFieldAndObject(clusterField, newCluster),
        toDbfsUri(jarDbfsPath),
        MainSparkLaunchSpec.positional(pipelineDbfsPath, metadataDbfsPath, runConfigName));
  }

  /**
   * Normalize library / MainSpark parameter paths.
   *
   * <ul>
   *   <li>UC Volumes and Workspace files stay as absolute {@code /Volumes/…} or {@code
   *       /Workspace/…} (no {@code dbfs:} scheme).
   *   <li>Classic DBFS paths get a {@code dbfs:} scheme when missing.
   * </ul>
   */
  public static String toDbfsUri(String path) throws HopException {
    String p = require(path, "workspace path");
    if (p.startsWith("dbfs:")) {
      String stripped = p.substring("dbfs:".length());
      if (!stripped.startsWith("/")) {
        stripped = "/" + stripped;
      }
      if (isVolumeOrWorkspacePath(stripped)) {
        return stripped;
      }
      return "dbfs:" + stripped;
    }
    if (!p.startsWith("/")) {
      p = "/" + p;
    }
    if (isVolumeOrWorkspacePath(p)) {
      return p;
    }
    return "dbfs:" + p;
  }

  static boolean isVolumeOrWorkspacePath(String absolutePath) {
    return absolutePath.startsWith("/Volumes/")
        || absolutePath.equals("/Volumes")
        || absolutePath.startsWith("/Workspace/")
        || absolutePath.equals("/Workspace");
  }

  private static String require(String value, String label) throws HopException {
    if (StringUtils.isBlank(value)) {
      throw new HopException("Missing " + label);
    }
    return value.trim();
  }
}
