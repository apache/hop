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

package org.apache.hop.beam.engines.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FlinkJobConfigurationTest {

  @TempDir Path tempDir;

  @Test
  void fixedJobIdIsUsedOnTheJobGraph() throws Exception {
    String jobId = new JobID().toHexString();
    Path confDir = FlinkJobConfiguration.createFromSource(null, jobId);
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertEquals(jobId, loaded.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));

      StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
      env.fromElements(1).print();
      StreamGraph streamGraph = env.getStreamGraph();
      streamGraph.setJobName("hop-flink-job-id");
      JobGraph jobGraph =
          PipelineExecutorUtils.getJobGraph(
              streamGraph, loaded, Thread.currentThread().getContextClassLoader());
      assertEquals(jobId, jobGraph.getJobID().toHexString());
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }

  @Test
  void existingStandardConfigIsPreserved() throws Exception {
    Path source = tempDir.resolve("standard");
    Files.createDirectory(source);
    Files.writeString(
        source.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
        "parallelism.default: 7\n\""
            + PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key()
            + "\": \"00000000000000000000000000000000\"\n",
        StandardCharsets.UTF_8);

    String jobId = new JobID().toHexString();
    Path confDir = FlinkJobConfiguration.createFromSource(source, jobId);
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertEquals(jobId, loaded.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
      assertEquals(7, loaded.get(CoreOptions.DEFAULT_PARALLELISM));
      assertTrue(Files.exists(confDir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME)));
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }

  @Test
  void existingLegacyConfigIsPreserved() throws Exception {
    Path source = tempDir.resolve("legacy");
    Files.createDirectory(source);
    Files.writeString(
        source.resolve(GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME),
        "parallelism.default: 3\n",
        StandardCharsets.UTF_8);

    String jobId = new JobID().toHexString();
    Path confDir = FlinkJobConfiguration.createFromSource(source, jobId);
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertEquals(jobId, loaded.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
      assertEquals(3, loaded.get(CoreOptions.DEFAULT_PARALLELISM));
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }

  @Test
  void missingConfigurationDirectoryThrows() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                FlinkJobConfiguration.createFromSource(
                    tempDir.resolve("missing"), new JobID().toHexString()));
    assertTrue(exception.getMessage().contains("does not exist"));
  }

  private static final List<String> PLUGIN_CLASSPATH =
      List.of(
          "file:/opt/hop/plugins/engines/beam/hop-engines-beam.jar", "file:/opt/my%20hop/x.jar");

  /** Embedded Flink: the plugin class path reaches the job graph, and resolves parent-first. */
  @Test
  void embeddedClasspathReachesTheJobGraph() throws Exception {
    Path confDir =
        FlinkJobConfiguration.createFromSource(null, new JobID().toHexString(), PLUGIN_CLASSPATH);
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertEquals(PLUGIN_CLASSPATH, loaded.get(PipelineOptions.CLASSPATHS));
      assertEquals("parent-first", loaded.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER));

      StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
      env.fromElements(1).print();
      StreamGraph streamGraph = env.getStreamGraph();
      JobGraph jobGraph =
          PipelineExecutorUtils.getJobGraph(
              streamGraph, loaded, Thread.currentThread().getContextClassLoader());
      assertEquals(PLUGIN_CLASSPATH, jobGraph.getClasspaths().stream().map(URL::toString).toList());
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }

  /** Any other master: no class path, no resolve order, as before. */
  @Test
  void withoutAClasspathNothingIsAdded() throws Exception {
    Path confDir = FlinkJobConfiguration.createFromSource(null, new JobID().toHexString());
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertFalse(loaded.contains(PipelineOptions.CLASSPATHS));
      assertFalse(loaded.contains(CoreOptions.CLASSLOADER_RESOLVE_ORDER));
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }

  @Test
  void theUsersLegacyClasspathIsKeptAndExtended() throws Exception {
    Path source = tempDir.resolve("legacy-classpath");
    Files.createDirectory(source);
    Files.writeString(
        source.resolve(GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME),
        "parallelism.default: 3\npipeline.classpaths: file:/user/own.jar\n",
        StandardCharsets.UTF_8);

    Path confDir =
        FlinkJobConfiguration.createFromSource(source, new JobID().toHexString(), PLUGIN_CLASSPATH);
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertEquals(
          List.of("file:/user/own.jar", PLUGIN_CLASSPATH.get(0), PLUGIN_CLASSPATH.get(1)),
          loaded.get(PipelineOptions.CLASSPATHS));
      assertEquals(3, loaded.get(CoreOptions.DEFAULT_PARALLELISM));
      assertEquals("parent-first", loaded.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER));
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }

  @Test
  void theUsersNestedStandardClasspathIsKeptAndExtended() throws Exception {
    Path source = tempDir.resolve("standard-classpath");
    Files.createDirectory(source);
    Files.writeString(
        source.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
        "pipeline:\n  classpaths: [\"file:/user/own.jar\"]\n",
        StandardCharsets.UTF_8);

    Path confDir =
        FlinkJobConfiguration.createFromSource(source, new JobID().toHexString(), PLUGIN_CLASSPATH);
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertEquals(
          List.of("file:/user/own.jar", PLUGIN_CLASSPATH.get(0), PLUGIN_CLASSPATH.get(1)),
          loaded.get(PipelineOptions.CLASSPATHS));
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }

  /** A resolve order the user chose stays: the engine warns about it instead. */
  @Test
  void theUsersResolveOrderIsKept() throws Exception {
    Path source = tempDir.resolve("child-first");
    Files.createDirectory(source);
    Files.writeString(
        source.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
        "classloader.resolve-order: child-first\n",
        StandardCharsets.UTF_8);

    Path confDir =
        FlinkJobConfiguration.createFromSource(source, new JobID().toHexString(), PLUGIN_CLASSPATH);
    try {
      Configuration loaded = GlobalConfiguration.loadConfiguration(confDir.toString());
      assertEquals("child-first", loaded.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER));
      assertEquals(PLUGIN_CLASSPATH, loaded.get(PipelineOptions.CLASSPATHS));
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }
}
