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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.cluster.ISparkClusterSubmitter;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkClusterRunTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopLogStore.init();
    HopClientEnvironment.init();
  }

  @Test
  void shipsRunVariablesNotClientSettings() {
    IVariables variables = new Variables();
    variables.setVariable("HOP_DATA", "/data");
    variables.setVariable("INPUT_DATE", "2026-11-13");
    variables.setVariable("PROJECT_HOME", "/project");
    variables.setVariable("java.home", "/opt/java");
    variables.setVariable("Internal.Entry.Current.Folder", "/project");
    variables.setVariable("HOP_PLUGIN_BASE_FOLDERS", "plugins");

    Map<String, String> shipped = SparkClusterRun.variablesToShip(variables);
    assertEquals("/data", shipped.get("HOP_DATA"));
    assertEquals("2026-11-13", shipped.get("INPUT_DATE"));
    assertFalse(shipped.containsKey("PROJECT_HOME"));
    assertFalse(shipped.containsKey("java.home"));
    assertFalse(shipped.containsKey("Internal.Entry.Current.Folder"));
    assertFalse(shipped.containsKey("HOP_PLUGIN_BASE_FOLDERS"));
  }

  /** Stop while the fat jar was built or the files were staged: no driver is submitted. */
  @Test
  void aStopBeforeTheSubmitSubmitsNothing() throws Exception {
    FakeSubmitter submitter = new FakeSubmitter();
    SparkClusterRun run = run(new SparkPipelineRunConfiguration());
    run.setSubmitter(submitter);

    run.kill();
    assertFalse(run.submitDriver("hop.jar", List.of(), Map.of()));
    assertEquals(0, submitter.submits.get());
    assertEquals(1, submitter.cancels.get(), "a submit in progress is cancelled");
    assertTrue(submitter.killed.isEmpty());
  }

  /**
   * Stop while the submit runs: there is no id to kill yet, so the driver is killed as soon as the
   * cluster gives it one. Before, it ran to the end while Hop showed the run as stopped.
   */
  @Test
  void aStopDuringTheSubmitKillsTheDriverOnceItHasAnId() throws Exception {
    SparkClusterRun run = run(new SparkPipelineRunConfiguration());
    FakeSubmitter submitter =
        new FakeSubmitter() {
          @Override
          public String submit(
              String appResource,
              String mainClass,
              List<String> appArgs,
              Map<String, String> sparkProperties,
              Map<String, String> environmentVariables) {
            submits.incrementAndGet();
            run.kill();
            return "driver-1";
          }
        };
    run.setSubmitter(submitter);

    assertTrue(run.submitDriver("hop.jar", List.of(), Map.of()));
    assertEquals(List.of("driver-1"), submitter.killed);
    assertEquals("driver-1", run.getSubmissionId());
  }

  @Test
  void aStopAfterTheSubmitKillsTheDriver() throws Exception {
    FakeSubmitter submitter = new FakeSubmitter();
    SparkClusterRun run = run(new SparkPipelineRunConfiguration());
    run.setSubmitter(submitter);

    assertTrue(run.submitDriver("hop.jar", List.of(), Map.of()));
    assertTrue(submitter.killed.isEmpty());
    run.kill();
    assertEquals(List.of("driver-1"), submitter.killed);
    assertEquals(0, submitter.cancels.get());
  }

  /** A cluster that keeps saying it does not know the state lost track of the driver. */
  @Test
  void aDriverTheClusterLostTrackOfFailsTheRun() {
    FakeSubmitter submitter = new FakeSubmitter();
    submitter.state = "UNKNOWN";
    SparkClusterRun run = run(new SparkPipelineRunConfiguration());
    run.setSubmitter(submitter);
    run.setSubmissionId("driver-1");
    run.pollMillis = 1L;

    HopException e = assertThrows(HopException.class, () -> run.waitForDriver(() -> false));
    assertTrue(e.getMessage().contains("lost track of it"), e.getMessage());
    assertEquals(SparkClusterRun.UNKNOWN_POLLS + 1, submitter.statusCalls.get());
  }

  @Test
  void aStatusThatKeepsFailingFailsTheRun() {
    FakeSubmitter submitter = new FakeSubmitter();
    submitter.state = null;
    SparkClusterRun run = run(new SparkPipelineRunConfiguration());
    run.setSubmitter(submitter);
    run.setSubmissionId("driver-1");
    run.pollMillis = 1L;

    assertThrows(HopException.class, () -> run.waitForDriver(() -> false));
    assertEquals(SparkClusterRun.UNREACHABLE_POLLS + 1, submitter.statusCalls.get());
  }

  /**
   * A spark.jars row adds a jar the executors need too, such as a JDBC driver: the Hop fat jar
   * stays. Other rows still replace what Hop sets.
   */
  @Test
  void aSparkJarsRowAddsToTheFatJar() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setDriverMemory("1g");
    config.setSparkConfigs("spark.jars=s3a://libs/postgresql.jar\nspark.driver.memory=4g");
    Map<String, String> properties =
        run(config).sparkProperties("spark://spark:7077", "s3a://staging/hop-native-abc.jar");
    assertEquals(
        "s3a://staging/hop-native-abc.jar,s3a://libs/postgresql.jar", properties.get("spark.jars"));
    assertEquals("4g", properties.get("spark.driver.memory"));
  }

  /**
   * The YARN client that follows and stops the application gets the spark.hadoop.* rows of the
   * Spark settings too, as the launcher does: both talk to the same ResourceManager.
   */
  @Test
  void theYarnClientTakesTheSparkHadoopRows() throws Exception {
    Path conf = Files.createDirectories(tempDir.resolve("hadoop-conf"));
    Files.writeString(
        conf.resolve("yarn-site.xml"),
        "<configuration><property><name>yarn.resourcemanager.address</name>"
            + "<value>rm-old:8032</value></property></configuration>",
        StandardCharsets.UTF_8);
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setSparkMaster("yarn");
    config.setHadoopConfDir(conf.toString());
    config.setSparkConfigs(
        "spark.hadoop.yarn.resourcemanager.address=rm-new:8032\nspark.executor.instances=2");

    Map<String, String> hadoop = run(config).hadoopProperties();
    assertEquals("rm-new:8032", hadoop.get("spark.hadoop.yarn.resourcemanager.address"));
    assertNull(hadoop.get("spark.executor.instances"), "only Hadoop settings");
  }

  /** A fat jar given as a URL is one the cluster reads itself: no local file to stage. */
  @Test
  void aFatJarUrlIsNoLocalFile() {
    assertEquals(
        Paths.get("/opt/hop/hop-native.jar"), SparkClusterRun.localFile("/opt/hop/hop-native.jar"));
    assertEquals(
        Paths.get("/opt/hop/hop-native.jar"),
        SparkClusterRun.localFile("file:/opt/hop/hop-native.jar"));
    assertEquals(
        Paths.get("/opt/hop/hop-native.jar"),
        SparkClusterRun.localFile("file:///opt/hop/hop-native.jar"));
    assertNull(SparkClusterRun.localFile("hdfs://nn:8020/hop/hop-native.jar"));
    assertNull(SparkClusterRun.localFile("s3a://bucket/hop-native.jar"));
    assertNull(SparkClusterRun.localFile("local:/opt/hop/hop-native.jar"));
    assertNotNull(SparkClusterRun.localFile("C:\\hop\\hop-native.jar"), "a drive letter");
  }

  private static SparkClusterRun run(SparkPipelineRunConfiguration config) {
    return new SparkClusterRun(
        LogChannel.GENERAL, new Variables(), null, new PipelineMeta(), "spark", config);
  }

  /** A cluster that takes every driver, as driver-1, and reports a fixed state. */
  private static class FakeSubmitter implements ISparkClusterSubmitter {
    final AtomicInteger submits = new AtomicInteger();
    final AtomicInteger cancels = new AtomicInteger();
    final AtomicInteger statusCalls = new AtomicInteger();
    final List<String> killed = new CopyOnWriteArrayList<>();

    /** The state status reports; null makes status fail. */
    volatile String state = "RUNNING";

    @Override
    public String submit(
        String appResource,
        String mainClass,
        List<String> appArgs,
        Map<String, String> sparkProperties,
        Map<String, String> environmentVariables) {
      submits.incrementAndGet();
      return "driver-1";
    }

    @Override
    public SparkDriverStatus status(String id) throws HopException {
      statusCalls.incrementAndGet();
      if (state == null) {
        throw new HopException("unreachable");
      }
      return new SparkDriverStatus(state, null, null, false, false);
    }

    @Override
    public String kill(String id) {
      killed.add(id);
      return "killed";
    }

    @Override
    public void cancelSubmission() {
      cancels.incrementAndGet();
    }

    @Override
    public String describe() {
      return "fake";
    }
  }
}
