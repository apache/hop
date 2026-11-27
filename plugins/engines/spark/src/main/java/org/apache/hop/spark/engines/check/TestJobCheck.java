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

package org.apache.hop.spark.engines.check;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.IHopLoggingEventListener;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogMessage;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkPipelineEngine;

/**
 * Runs a small pipeline on the cluster with this configuration: 1000 generated rows through a Hop
 * transform on the executors, which load the fat jar. It passes when the run finishes without
 * errors and, in client mode, the row counts of the last transform come back to Hop. In cluster
 * mode it runs on the cluster the way a pipeline does: submitted with the project package, where
 * the tested run configuration and the test pipeline go too. Later checks of what comes back from a
 * run (metrics, sampling, logging) belong here.
 */
public class TestJobCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "TestJob";

  static final String PIPELINE = "hop-spark-test-job.hpl";
  static final String LAST_TRANSFORM = "check";
  static final long ROWS = 1000;
  static final long TIMEOUT_MINUTES = 15;

  /**
   * The name of the tested run configuration. In cluster mode the driver loads it by this name from
   * the project package.
   */
  static final String RUN_CONFIGURATION = "hop-spark-configuration-test";

  public TestJobCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    // The test job is an unsaved pipeline: a Databricks run submits a pipeline file of the
    // project, and with spark-submit it is spark-submit that starts the pipeline, not Hop
    SparkClusterType type = context.getClusterType();
    return context.isRunningTestJob()
        && type != SparkClusterType.DATABRICKS
        && type != SparkClusterType.SUBMITTED;
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    if (context.hasFailures()) {
      return info(
          "Skipped: the test job would fail on what the checks above found. Fix those first.");
    }
    // In cluster mode the driver runs the test job from a package of the project
    boolean onTheCluster = context.isClusterMode();
    if (onTheCluster && StringUtils.isEmpty(context.getVariables().getVariable("PROJECT_HOME"))) {
      return warning(
          "Not run: in cluster mode the test job runs from a package of the project, and no"
              + " project is open.",
          "Open a project and test again, or test the configuration in client mode.");
    }
    PipelineMeta pipelineMeta;
    try (InputStream xml = TestJobCheck.class.getResourceAsStream(PIPELINE)) {
      pipelineMeta = new PipelineMeta(xml, context.getMetadataProvider(), context.getVariables());
    }
    PipelineRunConfiguration runConfiguration = new PipelineRunConfiguration();
    runConfiguration.setName(RUN_CONFIGURATION);
    runConfiguration.setEngineRunConfiguration(context.getConfiguration().clone());

    SparkPipelineEngine engine = new SparkPipelineEngine();
    engine.initializeFrom(context.getVariables());
    engine.setPipelineMeta(pipelineMeta);
    engine.setMetadataProvider(
        onTheCluster
            ? withRunConfiguration(
                context.getMetadataProvider(), runConfiguration, context.getVariables())
            : context.getMetadataProvider());
    engine.setPipelineRunConfiguration(runConfiguration);

    // What the engine says it does (building the fat jar, staging, submitting), for the progress
    AtomicReference<String> latest = new AtomicReference<>("starting Spark");
    IHopLoggingEventListener listener =
        event -> {
          if (event.getMessage() instanceof LogMessage message
              && message.getLevel() != null
              && message.getLevel().getLevel() <= LogLevel.BASIC.getLevel()
              && isOf(engine, message.getLogChannelId())) {
            latest.set(message.getMessage());
          }
        };
    HopLogStore.getAppender().addLoggingEventListener(listener);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread starter =
        new Thread(
            () -> {
              try {
                engine.execute();
              } catch (Throwable e) {
                failure.set(e);
              }
            },
            "hop-spark-test-job");
    long start = System.currentTimeMillis();
    long deadline = start + TimeUnit.MINUTES.toMillis(TIMEOUT_MINUTES);
    try {
      starter.start();
      while (starter.isAlive() || !engine.isFinished() && !engine.isStopped()) {
        if (failure.get() != null) {
          break;
        }
        if (context.isCanceled() || System.currentTimeMillis() > deadline) {
          engine.stopAll();
          return warning(
              context.isCanceled()
                  ? "The test job was cancelled."
                  : "The test job did not finish within " + TIMEOUT_MINUTES + " minutes.",
              "Look at the Spark UI of the cluster: the application may wait for executors.");
        }
        context.progress(progress(engine, latest.get(), System.currentTimeMillis() - start));
        Thread.sleep(250);
      }
    } finally {
      HopLogStore.getAppender().removeLoggingEventListener(listener);
    }
    if (failure.get() != null) {
      throw failure.get() instanceof Exception e ? e : new IllegalStateException(failure.get());
    }
    engine.waitUntilFinished();
    long seconds = (System.currentTimeMillis() - start) / 1000;
    if (onTheCluster) {
      if (engine.getErrors() > 0) {
        return error(
            "The test job failed on the cluster after " + seconds + " s.",
            "The Hop log has the state the driver ended in and where its log is; the checks above"
                + " passed, so look at what they can't see: the image or Java version of the driver"
                + " and executors, or a class conflict in the fat jar.");
      }
      return ok(
          "The test job ran on the cluster in "
              + seconds
              + " s: the driver sent "
              + ROWS
              + " rows through a Hop transform on the executors. Row counts of a cluster-mode run"
              + " don't come back to Hop yet.");
    }
    if (engine.getErrors() > 0) {
      return error(
          "The test job failed after " + seconds + " s.",
          "The Hop log has the reason; the checks above passed, so look at what they can't see:"
              + " the image or Java version of the executors, or a class conflict in the fat jar.");
    }
    long rows = 0;
    for (IEngineComponent component : engine.getComponentCopies(LAST_TRANSFORM)) {
      rows += component.getLinesRead();
    }
    if (rows != ROWS) {
      return warning(
          "The test job finished in "
              + seconds
              + " s, but "
              + (rows == 0 ? "no row counts came back" : rows + " rows were counted, not " + ROWS)
              + ".",
          "The pipeline runs; its metrics don't reach Hop. Check that the driver can reach this"
              + " machine, or ignore it when you don't follow runs in Hop.");
    }
    return ok(
        "The test job sent "
            + ROWS
            + " rows through a Hop transform on the executors in "
            + seconds
            + " s, and their counts came back.");
  }

  /**
   * The metadata of the run with the tested run configuration added: the driver of a cluster-mode
   * run loads it by name from the package, and the configuration being edited may not be saved.
   */
  static IHopMetadataProvider withRunConfiguration(
      IHopMetadataProvider metadataProvider,
      PipelineRunConfiguration runConfiguration,
      IVariables variables)
      throws HopException {
    MemoryMetadataProvider tested = new MemoryMetadataProvider();
    tested.getSerializer(PipelineRunConfiguration.class).save(runConfiguration);
    if (metadataProvider == null) {
      return tested;
    }
    // The last provider wins: the tested configuration over one of the same name
    return new MultiMetadataProvider(variables, metadataProvider, tested);
  }

  /**
   * "Test job, 42 s: Fat jar: 240 of 480 jar files added (50%)", or the rows done while it runs.
   */
  static String progress(SparkPipelineEngine engine, String latest, long millis) {
    long rows = 0;
    for (IEngineComponent component : engine.getComponentCopies(LAST_TRANSFORM)) {
      rows += component.getLinesRead();
    }
    String what = rows > 0 ? rows + " of " + ROWS + " rows done" : shorten(latest);
    return "Test job, " + millis / 1000 + " s: " + what;
  }

  static String shorten(String message) {
    String line = message == null ? "" : message.trim().replaceAll("\\s*\\R\\s*", " ");
    return line.length() <= 100 ? line : line.substring(0, 97) + "...";
  }

  /** Whether a log channel is the engine's or one of its children (transforms, the fat jar). */
  private static boolean isOf(SparkPipelineEngine engine, String logChannelId) {
    String engineId = engine.getLogChannelId();
    if (engineId == null || logChannelId == null) {
      return false;
    }
    return engineId.equals(logChannelId)
        || LoggingRegistry.getInstance().getLogChannelChildren(engineId).contains(logChannelId);
  }
}
