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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hop.core.cluster.ISparkClusterSubmitter;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.spark.util.SparkHadoopConfiguration;
import org.apache.spark.launcher.InProcessLauncher;
import org.apache.spark.launcher.SparkAppHandle;

/**
 * Submits a driver to YARN in cluster deploy mode, and follows and kills the YARN application.
 *
 * <p>Submission runs Spark's own YARN client in this JVM ({@link InProcessLauncher}): no Spark
 * installation and no {@code SPARK_HOME} are needed. It returns once YARN accepted the application
 * ({@code spark.yarn.submit.waitAppCompletion=false}); the status and Stop go through the YARN
 * client API, so they do not depend on the launcher thread.
 */
public class SparkYarnSubmitter implements ISparkClusterSubmitter {

  /** How long YARN may take to accept an application. */
  static final long SUBMIT_TIMEOUT_MILLIS = 10 * 60 * 1000L;

  private final ILogChannel log;

  /** The Hadoop client configuration as {@code spark.hadoop.*} properties. */
  private final Map<String, String> hadoopProperties;

  private YarnClient yarnClient;

  /** Set by {@link #cancelSubmission()}: Stop came before YARN accepted the application. */
  private volatile boolean cancelled;

  public SparkYarnSubmitter(ILogChannel log, Map<String, String> hadoopProperties) {
    this.log = log;
    this.hadoopProperties = hadoopProperties;
  }

  @Override
  public String submit(
      String appResource,
      String mainClass,
      List<String> appArgs,
      Map<String, String> sparkProperties,
      Map<String, String> environmentVariables)
      throws HopException {
    InProcessLauncher launcher =
        new InProcessLauncher()
            // Without a properties file the launcher looks for SPARK_HOME/conf/spark-defaults.conf
            // and fails when there is no Spark installation. All settings come from the run
            // configuration, and the YARN containers get Spark from spark.yarn.jars.
            .setPropertiesFile(emptyPropertiesFile())
            .setMaster("yarn")
            .setDeployMode("cluster")
            .setAppResource(appResource)
            .setMainClass(mainClass)
            .addAppArgs(appArgs.toArray(new String[0]));
    for (Map.Entry<String, String> property : hadoopProperties.entrySet()) {
      launcher.setConf(property.getKey(), property.getValue());
    }
    for (Map.Entry<String, String> property : sparkProperties.entrySet()) {
      if (!"spark.master".equals(property.getKey())
          && !"spark.submit.deployMode".equals(property.getKey())) {
        launcher.setConf(property.getKey(), property.getValue());
      }
    }
    // The driver runs in the YARN application master and gets the environment through it
    for (Map.Entry<String, String> variable : environmentVariables.entrySet()) {
      launcher.setConf("spark.yarn.appMasterEnv." + variable.getKey(), variable.getValue());
    }
    // Hand back the application id once YARN accepted it; following happens through YarnClient
    launcher.setConf("spark.yarn.submit.waitAppCompletion", "false");

    SparkAppHandle handle;
    try {
      handle = launcher.startApplication();
    } catch (Exception e) {
      throw new HopException("Unable to start the YARN submission", e);
    }
    long deadline = System.currentTimeMillis() + SUBMIT_TIMEOUT_MILLIS;
    while (handle.getAppId() == null) {
      if (cancelled) {
        handle.kill();
        throw new HopException("The YARN submission was cancelled");
      }
      if (handle.getState().isFinal()) {
        Throwable error = handle.getError().orElse(null);
        throw new HopException(
            "YARN did not accept the application (launcher state "
                + handle.getState()
                + ")"
                + (error != null ? ": " + error.getMessage() : ""),
            error);
      }
      if (System.currentTimeMillis() > deadline) {
        handle.kill();
        throw new HopException(
            "YARN did not accept the application within "
                + SUBMIT_TIMEOUT_MILLIS / 60000
                + " minutes (launcher state "
                + handle.getState()
                + ")");
      }
      sleep();
    }
    return handle.getAppId();
  }

  @Override
  public void cancelSubmission() {
    cancelled = true;
  }

  @Override
  public SparkDriverStatus status(String id) throws HopException {
    ClassLoader previous = usePluginClassLoader();
    try {
      ApplicationReport report = yarn().getApplicationReport(ApplicationId.fromString(id));
      return toStatus(
          report.getYarnApplicationState(),
          report.getFinalApplicationStatus(),
          report.getTrackingUrl(),
          report.getDiagnostics());
    } catch (Exception e) {
      throw new HopException("Unable to read the state of YARN application " + id, e);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  @Override
  public String kill(String id) throws HopException {
    ClassLoader previous = usePluginClassLoader();
    try {
      yarn().killApplication(ApplicationId.fromString(id));
      return "kill requested from the ResourceManager";
    } catch (Exception e) {
      throw new HopException("Unable to kill YARN application " + id, e);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  /**
   * Hadoop resolves its client classes through the thread's context class loader. Stop runs on a
   * Hop thread whose loader also sees the Hadoop jars of other plugins, and the same class from two
   * loaders fails as "class X not Y": always use the loader of this plugin.
   */
  private static ClassLoader usePluginClassLoader() {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(SparkYarnSubmitter.class.getClassLoader());
    return previous;
  }

  @Override
  public String describe() {
    String resourceManager =
        hadoopProperties.get(
            SparkHadoopConfiguration.SPARK_HADOOP_PREFIX + YarnConfiguration.RM_ADDRESS);
    return "YARN" + (resourceManager != null ? " (" + resourceManager + ")" : "");
  }

  /**
   * The driver of a YARN cluster-mode run is the application master: a FINISHED application ended
   * successfully only when its final status says so.
   */
  static SparkDriverStatus toStatus(
      YarnApplicationState state,
      FinalApplicationStatus finalStatus,
      String trackingUrl,
      String diagnostics) {
    boolean isFinal =
        state == YarnApplicationState.FINISHED
            || state == YarnApplicationState.FAILED
            || state == YarnApplicationState.KILLED;
    boolean success =
        state == YarnApplicationState.FINISHED && finalStatus == FinalApplicationStatus.SUCCEEDED;
    String name =
        state == YarnApplicationState.FINISHED && !success
            ? "FINISHED/" + finalStatus
            : state.name();
    return new SparkDriverStatus(
        name,
        StringUtils.isNotEmpty(trackingUrl) ? trackingUrl : null,
        StringUtils.isNotEmpty(diagnostics) ? diagnostics : null,
        isFinal,
        success);
  }

  @Override
  public synchronized void close() {
    if (yarnClient != null) {
      try {
        yarnClient.stop();
      } catch (Exception e) {
        log.logDetailed("Error closing the YARN client: " + e.getMessage());
      }
      yarnClient = null;
    }
  }

  private synchronized YarnClient yarn() {
    if (yarnClient == null) {
      Configuration configuration = new YarnConfiguration();
      configuration.setClassLoader(SparkYarnSubmitter.class.getClassLoader());
      for (Map.Entry<String, String> property : hadoopProperties.entrySet()) {
        if (property.getKey().startsWith(SparkHadoopConfiguration.SPARK_HADOOP_PREFIX)) {
          configuration.set(
              property.getKey().substring(SparkHadoopConfiguration.SPARK_HADOOP_PREFIX.length()),
              property.getValue());
        }
      }
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(configuration);
      yarnClient.start();
      log.logDetailed("Connected to the YARN ResourceManager " + describe());
    }
    return yarnClient;
  }

  static String emptyPropertiesFile() throws HopException {
    try {
      java.nio.file.Path file = java.nio.file.Files.createTempFile("hop-spark-submit", ".conf");
      file.toFile().deleteOnExit();
      return file.toString();
    } catch (java.io.IOException e) {
      throw new HopException("Unable to create a properties file for the YARN submission", e);
    }
  }

  private static void sleep() throws HopException {
    try {
      Thread.sleep(500L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted while submitting to YARN", e);
    }
  }
}
