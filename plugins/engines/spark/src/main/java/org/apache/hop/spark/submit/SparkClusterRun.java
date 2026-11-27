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

package org.apache.hop.spark.submit;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.NameScope;
import org.apache.hop.core.cluster.ISparkClusterSubmitter;
import org.apache.hop.core.cluster.SparkClusterTarget;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.fatjar.ContentStaging;
import org.apache.hop.core.fatjar.FatJarCache;
import org.apache.hop.core.fatjar.NativeSparkFatJar;
import org.apache.hop.core.fatjar.VfsStagingStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkSetting;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import org.apache.hop.spark.run.MainSpark;
import org.apache.hop.spark.util.SparkHadoopConfiguration;

/**
 * Runs a pipeline with the driver on the cluster: build (or reuse) the fat jar, export the project
 * with the client's variables, stage both, submit {@link MainSpark} and follow the driver until it
 * ends. The remote driver loads the same run configuration from the package and, running under a
 * submit, executes the pipeline instead of submitting it again.
 */
public class SparkClusterRun {

  /** Milliseconds between two driver status calls. */
  static final long POLL_MILLIS = 2000L;

  /** Status calls that may fail in a row before following the driver fails: a minute. */
  static final int UNREACHABLE_POLLS = 30;

  /** Status calls that may say the state is unknown in a row: five minutes. */
  static final int UNKNOWN_POLLS = 150;

  /** Where an unsaved pipeline goes in the project tree of the package. */
  static final String UNSAVED_PIPELINE_FOLDER = ".hop-spark-unsaved";

  static final String SPARK_JARS = "spark.jars";

  private static final Pattern URL_SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]+:");

  /** Variables of the client JVM that mean nothing on the driver, or that it sets itself. */
  static final List<String> NOT_SHIPPED_PREFIXES =
      List.of(
          "java.",
          "javax.",
          "jdk.",
          "sun.",
          "os.",
          "user.",
          "file.",
          "line.",
          "path.",
          "awt.",
          "native.",
          "stdout.",
          "stderr.",
          "Internal.",
          "spark.");

  static final List<String> NOT_SHIPPED_NAMES =
      List.of(
          "PROJECT_HOME",
          SparkProjectPackage.VAR_PACKAGE_URI,
          SparkProjectPackage.VAR_PACKAGE_SPARK_FILE,
          SparkProjectPackage.VAR_PACKAGE_FALLBACK,
          "HOP_PLUGIN_BASE_FOLDERS",
          "HOP_CONFIG_FOLDER",
          "HOP_AUDIT_FOLDER",
          "HOP_SHARED_JDBC_FOLDERS");

  private final ILogChannel log;
  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final PipelineMeta pipelineMeta;
  private final String runConfigurationName;
  private final ISparkPipelineEngineRunConfiguration runConfiguration;

  private volatile ISparkClusterSubmitter submitter;
  private ContentStaging staging;
  private volatile String submissionId;

  /** Set by {@link #kill()}: the run was stopped. Guarded by {@link #submitLock} with the id. */
  private volatile boolean cancelled;

  private final Object submitLock = new Object();

  long pollMillis = POLL_MILLIS;

  /** The last state the cluster reported: no kill once it says the driver ended. */
  private volatile SparkDriverStatus lastStatus;

  public SparkClusterRun(
      ILogChannel log,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration) {
    this.log = log;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.pipelineMeta = pipelineMeta;
    this.runConfigurationName = runConfigurationName;
    this.runConfiguration = runConfiguration;
  }

  /**
   * Build, stage and submit.
   *
   * @return false when the run was stopped before the driver was submitted
   */
  public boolean submit() throws HopException {
    String projectHome = variables.getVariable("PROJECT_HOME");
    if (StringUtils.isEmpty(projectHome)) {
      throw new HopException(
          "Cluster mode runs a pipeline of a project: the driver on the cluster loads it from the"
              + " project package. Run the pipeline in a project, or use client mode.");
    }
    String pipelinePath;
    Map<String, byte[]> unsavedPipeline = Map.of();
    if (StringUtils.isEmpty(pipelineMeta.getFilename())) {
      // A pipeline that has no file, such as the test job of the configuration test, travels in
      // the package next to the project
      byte[] xml = pipelineMeta.getXml(variables).getBytes(StandardCharsets.UTF_8);
      pipelinePath = UNSAVED_PIPELINE_FOLDER + "/" + sha256(xml).substring(0, 12) + ".hpl";
      unsavedPipeline = Map.of(pipelinePath, xml);
    } else {
      pipelinePath = relativePipelinePath(projectHome);
    }

    String master = variables.resolve(runConfiguration.getSparkMaster());
    boolean yarn = SparkHadoopConfiguration.isYarnMaster(master);
    SparkClusterTarget platform = null;
    if (runConfiguration.getEffectiveClusterType() == SparkClusterType.DATABRICKS) {
      // Another plugin submits the run and stages its files: Databricks has no Spark master
      platform = platformTarget(log, variables, metadataProvider, runConfiguration);
      submitter = platform.getSubmitter();
      master = "";
    } else {
      Map<String, String> hadoopProperties = yarn ? hadoopProperties() : Map.of();
      submitter = createSubmitter(master, hadoopProperties);
    }

    String configuredJar = variables.resolve(runConfiguration.getFatJar());
    Path fatJar;
    if (StringUtils.isEmpty(configuredJar)) {
      fatJar = NativeSparkFatJar.getOrBuild(log, variables, true);
    } else {
      // A URL (hdfs://, s3a://, local:/...) is a jar the cluster reads where it is
      fatJar = localFile(configuredJar);
      if (fatJar != null && !Files.isRegularFile(fatJar)) {
        throw new HopException(
            "The fat jar '" + configuredJar + "' of the run configuration does not exist.");
      }
    }
    // Building the fat jar can take minutes: Stop may have come meanwhile
    if (cancelled) {
      return false;
    }
    Path projectPackage =
        Paths.get(
            SparkProjectPackage.exportForCluster(
                projectHome,
                metadataProvider,
                variables,
                log,
                packagedVariables(projectHome),
                unsavedPipeline));

    staging =
        new ContentStaging(
            log,
            platform != null
                ? platform.getStagingStore()
                : new VfsStagingStore(
                    variables,
                    runConfiguration.getStagingFolder(),
                    runConfiguration.getStagingClusterUrl(),
                    "Cluster mode needs a staging folder in the Spark run configuration: a"
                        + " location the Spark cluster can read, where Hop uploads the fat jar and"
                        + " the project package."));
    String jarUrl;
    if (fatJar != null) {
      jarUrl = staging.stageFatJar(fatJar);
    } else {
      jarUrl = configuredJar;
      log.logBasic("The cluster reads the fat jar from " + jarUrl);
    }
    String packageUrl = staging.stage(projectPackage, projectPackage.getFileName().toString());
    Map<String, String> properties = sparkProperties(master, jarUrl);
    if (yarn) {
      // On YARN the application jar is on the class path of the driver and the executors already;
      // other jars of the Spark settings stay
      String jars = SparkSetting.without(SPARK_JARS, properties.get(SPARK_JARS), jarUrl);
      if (jars == null) {
        properties.remove(SPARK_JARS);
      } else {
        properties.put(SPARK_JARS, jars);
      }
      // A failed pipeline must not silently run a second time and write its output twice
      properties.putIfAbsent("spark.yarn.maxAppAttempts", "1");
      if (!properties.containsKey("spark.yarn.jars")
          && !properties.containsKey("spark.yarn.archive")) {
        properties.put("spark.yarn.archive", stageSparkRuntime(staging));
      }
    }

    boolean submitted =
        submitDriver(
            jarUrl,
            List.of(
                "--HopProjectPackage=" + packageUrl,
                "--HopPipelinePath=" + pipelinePath,
                "--HopRunConfigurationName=" + runConfigurationName),
            properties);
    if (!submitted) {
      return false;
    }
    log.logBasic(
        "Submitted driver "
            + submissionId
            + " to "
            + submitter.describe()
            + " ("
            + (StringUtils.isEmpty(master) ? "" : "master " + master + ", ")
            + "pipeline "
            + pipelinePath
            + ")");
    // Files of other runs, versions and installations that nobody used for a while
    staging.cleanUpIfDue(variables);
    return true;
  }

  /**
   * Submit the driver, unless the run was stopped. A stop that comes while the submit runs cancels
   * it, or kills the driver as soon as the cluster gave it an id: {@link #kill()} has no id to kill
   * before that.
   *
   * @return false when the run was stopped before the driver was submitted
   */
  boolean submitDriver(String jarUrl, List<String> appArgs, Map<String, String> properties)
      throws HopException {
    if (cancelled) {
      return false;
    }
    String id = submitter.submit(jarUrl, MainSpark.class.getName(), appArgs, properties, Map.of());
    boolean stoppedMeanwhile;
    synchronized (submitLock) {
      submissionId = id;
      stoppedMeanwhile = cancelled;
    }
    if (stoppedMeanwhile) {
      log.logBasic("The run was stopped while driver " + id + " was submitted");
      killSubmitted(id);
    }
    return true;
  }

  /**
   * Follow the driver until it ends.
   *
   * @param stopped true once the run is stopped from Hop, which stops following
   * @return the last status
   */
  public SparkDriverStatus waitForDriver(BooleanSupplier stopped) throws HopException {
    String lastState = null;
    int unreachable = 0;
    int unknown = 0;
    while (true) {
      SparkDriverStatus status;
      try {
        status = submitter.status(submissionId);
        lastStatus = status;
        unreachable = 0;
      } catch (HopException e) {
        // A master restart or a network hiccup: give it a minute before giving up
        if (++unreachable > UNREACHABLE_POLLS) {
          throw e;
        }
        log.logDetailed("Driver status unavailable, retrying: " + e.getMessage());
        status = null;
      }
      if (status != null) {
        // The cluster lost track of the driver (a worker or node that does not come back)
        unknown = isUnknown(status) ? unknown + 1 : 0;
        if (unknown > UNKNOWN_POLLS) {
          throw new HopException(
              "The cluster has not known the state of driver "
                  + submissionId
                  + " for "
                  + UNKNOWN_POLLS * pollMillis / 1000
                  + " s: it lost track of it. Look at the cluster for what happened to it.");
        }
        if (!StringUtils.equals(lastState, status.state())) {
          log.logBasic(
              "Driver "
                  + submissionId
                  + " is "
                  + status.state()
                  + (StringUtils.isNotEmpty(status.location()) ? " at " + status.location() : ""));
          lastState = status.state();
        }
        if (status.isFinal()) {
          return status;
        }
      }
      if (stopped.getAsBoolean()) {
        return status;
      }
      // Executors that start later still download the staged files: keep them marked as used
      if (staging != null) {
        staging.keepAlive();
      }
      try {
        Thread.sleep(pollMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HopException("Interrupted while waiting for driver " + submissionId, e);
      }
    }
  }

  /**
   * The submitter and staging store of a cluster type that another plugin provides, through {@link
   * HopExtensionPoint#SparkClusterTarget}. Databricks is the one there is: the Databricks plugin
   * submits a one-time job run and stages on a Unity Catalog volume.
   *
   * @throws HopException when no installed plugin provides the cluster type, or its settings are
   *     incomplete
   */
  public static SparkClusterTarget platformTarget(
      ILogChannel log,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ISparkPipelineEngineRunConfiguration runConfiguration)
      throws HopException {
    SparkClusterType type = runConfiguration.getEffectiveClusterType();
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(
        SparkClusterTarget.SETTING_CONNECTION,
        variables.resolve(runConfiguration.getDatabricksConnection()));
    settings.put(
        SparkClusterTarget.SETTING_COMPUTE,
        variables.resolve(runConfiguration.getDatabricksCompute()));
    settings.put(
        SparkClusterTarget.SETTING_RUNTIME,
        variables.resolve(runConfiguration.getDatabricksRuntime()));
    settings.put(
        SparkClusterTarget.SETTING_NODE_TYPE,
        variables.resolve(runConfiguration.getDatabricksNodeType()));
    settings.put(
        SparkClusterTarget.SETTING_WORKERS,
        variables.resolve(runConfiguration.getDatabricksWorkers()));
    settings.put(
        SparkClusterTarget.SETTING_STAGING_FOLDER,
        variables.resolve(runConfiguration.getStagingFolder()));
    SparkClusterTarget target =
        new SparkClusterTarget(log, variables, metadataProvider, type.getCode(), settings);
    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.SparkClusterTarget.id, target);
    if (target.getSubmitter() == null || target.getStagingStore() == null) {
      throw new HopException(
          "The cluster type '"
              + type.getDescription()
              + "' needs the Databricks plugin (hop-tech-databricks): install it from the"
              + " marketplace, or choose another cluster type.");
    }
    return target;
  }

  /** The submitter for the master: the REST submission server of a standalone master, or YARN. */
  ISparkClusterSubmitter createSubmitter(String master, Map<String, String> hadoopProperties)
      throws HopException {
    if (SparkHadoopConfiguration.isYarnMaster(master)) {
      return new SparkYarnSubmitter(log, hadoopProperties);
    }
    if (master != null && master.trim().startsWith("k8s://")) {
      return new SparkKubernetesSubmitter(log, master.trim());
    }
    String submitUrl = variables.resolve(runConfiguration.getSubmitUrl());
    if (StringUtils.isBlank(submitUrl)) {
      submitUrl = SparkStandaloneRestSubmitter.deriveSubmitUrl(master);
    }
    return new SparkStandaloneRestSubmitter(
        submitUrl, org.apache.spark.package$.MODULE$.SPARK_VERSION());
  }

  /**
   * The Hadoop client configuration of the run configuration, as spark.hadoop.* properties: the
   * Hadoop configuration folder, with the spark.hadoop.* rows of the Spark settings on top. The
   * launcher gets those rows too, so the YARN client that follows and stops the application talks
   * to the ResourceManager the application went to.
   */
  Map<String, String> hadoopProperties() throws HopException {
    List<String> folders =
        SparkHadoopConfiguration.resolveFolders(
            variables.resolve(runConfiguration.getHadoopConfDir()), true, System::getenv);
    if (folders.isEmpty()) {
      throw new HopException(
          "Cluster mode on YARN needs the Hadoop client configuration: set the Hadoop configuration"
              + " folder of the run configuration, or HADOOP_CONF_DIR / YARN_CONF_DIR.");
    }
    Map<String, String> properties =
        new LinkedHashMap<>(SparkHadoopConfiguration.load(folders, log));
    sparkSettings()
        .forEach(
            (key, value) -> {
              if (key.startsWith(SparkHadoopConfiguration.SPARK_HADOOP_PREFIX)) {
                properties.put(key, value);
              }
            });
    return properties;
  }

  /**
   * Stage the Spark runtime of this engine for the YARN containers as one archive, and return the
   * value of {@code spark.yarn.archive}. The archive is built once per installation and named after
   * its content, so after the first run nothing is uploaded; without it YARN uploads a few hundred
   * MB for every run, and a NodeManager localizes one archive much faster than 150 loose jars.
   */
  private String stageSparkRuntime(ContentStaging staging) throws HopException {
    Path archive =
        SparkHadoopConfiguration.yarnRuntimeArchive(log, variables, SparkClusterRun.class);
    String sha = FatJarCache.sha256Of(archive);
    return staging.stage(archive, "spark-runtime-" + sha.substring(0, 12) + ".zip");
  }

  /**
   * Stop the run: kill the driver when it was submitted. Before that the run doesn't submit it, and
   * a submit in progress is cancelled, or its driver killed once it has an id.
   */
  public void kill() {
    String id;
    synchronized (submitLock) {
      cancelled = true;
      id = submissionId;
    }
    if (id == null) {
      log.logBasic("Stopped before the driver was submitted: it won't be");
      ISparkClusterSubmitter current = submitter;
      if (current != null) {
        current.cancelSubmission();
      }
      return;
    }
    killSubmitted(id);
  }

  private void killSubmitted(String id) {
    if (lastStatus != null && lastStatus.isFinal()) {
      log.logDetailed("Not killing driver " + id + ": it already ended");
      return;
    }
    try {
      log.logBasic("Killing driver " + id + ": " + submitter.kill(id));
    } catch (HopException e) {
      log.logError("Unable to kill driver " + id, e);
    }
  }

  /** Whether the run was stopped. */
  public boolean isCancelled() {
    return cancelled;
  }

  /** For tests: the submitter that {@link #submit()} would create. */
  void setSubmitter(ISparkClusterSubmitter submitter) {
    this.submitter = submitter;
  }

  /** For tests: the submission to follow. */
  void setSubmissionId(String submissionId) {
    this.submissionId = submissionId;
  }

  static boolean isUnknown(SparkDriverStatus status) {
    return status.state() == null || "UNKNOWN".equalsIgnoreCase(status.state());
  }

  /**
   * The local file of a fat jar location: a plain path or a {@code file:} URL.
   *
   * @return the file, or null for a URL that the cluster reads itself ({@code hdfs://}, {@code
   *     s3a://}, {@code local:/}, ...)
   */
  public static Path localFile(String location) {
    String value = location.trim();
    if (value.regionMatches(true, 0, "file:", 0, 5)) {
      try {
        return Paths.get(URI.create(value));
      } catch (Exception e) {
        return Paths.get(value.substring(5));
      }
    }
    // A scheme of two or more characters: a drive letter (C:\...) is a path
    if (URL_SCHEME.matcher(value).find()) {
      return null;
    }
    try {
      return Paths.get(value);
    } catch (InvalidPathException e) {
      return null;
    }
  }

  /** Let go of the submitter's connections. */
  public void close() {
    if (submitter != null) {
      submitter.close();
    }
  }

  public String getSubmissionId() {
    return submissionId;
  }

  Map<String, String> sparkProperties(String master, String jarUrl) {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("spark.master", master);
    properties.put("spark.submit.deployMode", "cluster");
    String appName = variables.resolve(runConfiguration.getSparkAppName());
    properties.put(
        "spark.app.name",
        StringUtils.isNotEmpty(appName) ? appName : "Apache Hop - " + pipelineMeta.getName());
    // The driver gets the application jar on its class path; executors need it in spark.jars
    properties.put(SPARK_JARS, jarUrl);
    properties.put("spark.driver.supervise", "false");
    putIfSet(properties, "spark.driver.memory", runConfiguration.getDriverMemory());
    putIfSet(properties, "spark.executor.memory", runConfiguration.getExecutorMemory());
    putIfSet(properties, "spark.executor.cores", runConfiguration.getExecutorCores());
    // A row replaces what Hop set, except a list such as spark.jars: that gets both
    sparkSettings()
        .forEach(
            (key, value) ->
                properties.put(key, SparkSetting.merge(key, properties.get(key), value)));
    return properties;
  }

  /** The rows of the Spark settings, resolved. */
  Map<String, String> sparkSettings() {
    Map<String, String> settings = new LinkedHashMap<>();
    String extra = runConfiguration.getSparkConfigs();
    if (StringUtils.isNotEmpty(extra)) {
      for (String line : extra.split("\\r?\\n")) {
        String trimmed = line.trim();
        int eq = trimmed.indexOf('=');
        if (!trimmed.isEmpty() && !trimmed.startsWith("#") && eq > 0) {
          settings.put(
              variables.resolve(trimmed.substring(0, eq).trim()),
              variables.resolve(trimmed.substring(eq + 1).trim()));
        }
      }
    }
    return settings;
  }

  private static String sha256(byte[] bytes) throws HopException {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (Exception e) {
      throw new HopException("Unable to hash the pipeline", e);
    }
  }

  private void putIfSet(Map<String, String> properties, String key, String value) {
    String resolved = variables.resolve(value);
    if (StringUtils.isNotEmpty(resolved)) {
      properties.put(key, resolved);
    }
  }

  /** The pipeline file relative to the project home, as the driver finds it in the package. */
  String relativePipelinePath(String projectHome) throws HopException {
    String filename = pipelineMeta.getFilename();
    if (StringUtils.isEmpty(filename)) {
      throw new HopException(
          "Cluster mode runs a saved pipeline file from the project package. Save the pipeline"
              + " in the project first.");
    }
    try {
      FileObject home = HopVfs.getFileObject(variables.resolve(projectHome));
      FileObject pipeline = HopVfs.getFileObject(variables.resolve(filename));
      if (!home.getName().isDescendent(pipeline.getName(), NameScope.DESCENDENT)) {
        throw new HopException(
            "Pipeline "
                + pipeline.getName().getURI()
                + " is not in the project home "
                + home.getName().getURI()
                + ": the driver on the cluster only sees the project package.");
      }
      return home.getName().getRelativeName(pipeline.getName());
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to locate " + filename + " in project " + projectHome, e);
    }
  }

  private Map<String, String> packagedVariables(String projectHome) {
    Map<String, String> shipped = variablesToShip(variables);
    shipped.put(SparkProjectPackage.VAR_CLIENT_PROJECT_HOME, variables.resolve(projectHome));
    return shipped;
  }

  /**
   * The variables of this run for the driver: project, environment and parameter values, without
   * the JVM and Hop installation settings of this client.
   */
  static Map<String, String> variablesToShip(IVariables variables) {
    Map<String, String> shipped = new LinkedHashMap<>();
    for (String name : variables.getVariableNames()) {
      String value = variables.getVariable(name);
      if (value == null
          || NOT_SHIPPED_NAMES.contains(name)
          || NOT_SHIPPED_PREFIXES.stream().anyMatch(name::startsWith)) {
        continue;
      }
      shipped.put(name, value);
    }
    return shipped;
  }
}
