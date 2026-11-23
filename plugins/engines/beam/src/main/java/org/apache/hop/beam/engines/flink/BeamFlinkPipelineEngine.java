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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.Getter;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hop.beam.engines.BeamPipelineEngine;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.HopURLClassLoader;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

@PipelineEnginePlugin(
    id = "BeamFlinkPipelineEngine",
    name = "Beam Flink pipeline engine",
    description = "This is a Flink pipeline engine provided by the Apache Beam community")
public class BeamFlinkPipelineEngine extends BeamPipelineEngine
    implements IPipelineEngine<PipelineMeta> {

  private static final Class<?> PKG = BeamFlinkPipelineEngine.class;

  /** Execution-information detail for the Flink job id submitted for this pipeline. */
  public static final String DETAIL_FLINK_JOB_ID = "flink.job.id";

  private static final String COLLECTION_MASTER = "[collection]";

  private static final String AUTO_MASTER = "[auto]";

  /** Embedded Flink: a mini-cluster in this JVM. */
  static final String LOCAL_MASTER = "[local]";

  /**
   * Job id Flink will submit. Null for the collection master, which does not start a job, and for
   * the auto master, where Flink ignores the configuration directory that carries the id.
   */
  @Getter private String flinkJobId;

  private Path flinkJobConfDir;

  @Override
  public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    BeamFlinkPipelineRunConfiguration runConfiguration = new BeamFlinkPipelineRunConfiguration();
    runConfiguration.setUserAgent("Hop");
    return runConfiguration;
  }

  @Override
  public void validatePipelineRunConfigurationClass(
      IPipelineEngineRunConfiguration engineRunConfiguration) throws HopException {
    if (!(engineRunConfiguration instanceof BeamFlinkPipelineRunConfiguration)) {
      throw new HopException(
          "A Beam Direct pipeline engine needs a direct run configuration, not of class "
              + engineRunConfiguration.getClass().getName());
    }
  }

  @Override
  public void prepareExecution() throws HopException {
    super.prepareExecution();
    assignFlinkJobId();
  }

  @Override
  public void startThreads() throws HopException {
    try {
      super.startThreads();
    } finally {
      deleteFlinkJobConfiguration();
    }
  }

  @Override
  protected ExecutionState capturePipelineExecutionState() {
    ExecutionState executionState = super.capturePipelineExecutionState();
    if (StringUtils.isNotEmpty(flinkJobId)) {
      if (executionState.getDetails() == null) {
        executionState.setDetails(new HashMap<>());
      }
      executionState.getDetails().put(DETAIL_FLINK_JOB_ID, flinkJobId);
    }
    return executionState;
  }

  /**
   * Fix the id Flink submits. {@code FlinkRunnerResult} keeps no job id, including after a failed
   * attached run.
   */
  private void assignFlinkJobId() throws HopException {
    deleteFlinkJobConfiguration();
    flinkJobId = null;
    if (getBeamPipeline() == null || getBeamPipeline().getOptions() == null) {
      return;
    }
    FlinkPipelineOptions options = getBeamPipeline().getOptions().as(FlinkPipelineOptions.class);
    if (!acceptsFixedJobId(options.getFlinkMaster())) {
      return;
    }
    String jobId = new JobID().toHexString();
    List<String> classpath =
        runsEmbedded(options.getFlinkMaster())
            ? embeddedClasspath(getClass().getClassLoader())
            : List.of();
    flinkJobConfDir = FlinkJobConfiguration.create(options.getFlinkConfDir(), jobId, classpath);
    options.setFlinkConfDir(flinkJobConfDir.toAbsolutePath().toString());
    if (!classpath.isEmpty()) {
      logChannel.logDetailed(
          BaseMessages.getString(
              PKG, "BeamEnginesFlink.EmbeddedClasspath.Log", String.valueOf(classpath.size())));
      String resolveOrder =
          GlobalConfiguration.loadConfiguration(flinkJobConfDir.toString())
              .get(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
      if (!FlinkJobConfiguration.PARENT_FIRST.equalsIgnoreCase(resolveOrder)) {
        logChannel.logBasic(
            BaseMessages.getString(
                PKG,
                "BeamEnginesFlink.EmbeddedResolveOrder.Warning",
                CoreOptions.CLASSLOADER_RESOLVE_ORDER.key(),
                resolveOrder));
      }
    }
    flinkJobId = jobId;
    logChannel.logBasic(BaseMessages.getString(PKG, "BeamEnginesFlink.JobId.Log", jobId));
  }

  /**
   * Only a local or host:port master builds its Flink environment from the configuration directory
   * Hop hands over. The auto master (also Beam's default for an empty master) takes the environment
   * of {@code flink run} or the Kubernetes operator instead, so a fixed job id would never reach
   * Flink and the reported id would be wrong.
   */
  static boolean acceptsFixedJobId(String flinkMaster) {
    return StringUtils.isNotBlank(flinkMaster)
        && !COLLECTION_MASTER.equals(flinkMaster.trim())
        && !AUTO_MASTER.equals(flinkMaster.trim());
  }

  /**
   * Embedded Flink ({@code [local]}) runs the job in this JVM, in a class loader for user code
   * whose parent is the class loader that loaded Flink. It does not use the thread's context class
   * loader, and Beam stages no files for it, not even a fat jar: the class path of the Beam plugin
   * has to be in the Flink configuration. Any other master runs on a cluster that cannot reach
   * these files.
   */
  static boolean runsEmbedded(String flinkMaster) {
    return LOCAL_MASTER.equals(StringUtils.trim(flinkMaster));
  }

  /**
   * The jars of the Beam plugin's class loader, wherever the plugin is installed: its jar, its lib
   * folder and the plugin folders its dependencies.xml names. Empty when the Beam classes are on a
   * flat class path (a fat jar, tests), where Flink sees them already.
   *
   * <p>Some of these URLs are decoded (a path with a space keeps the space), others are encoded:
   * every entry is written as an encoded file URI. A ';' is encoded too, as Flink splits the class
   * path on it.
   */
  static List<String> embeddedClasspath(ClassLoader classLoader) {
    if (!(classLoader instanceof HopURLClassLoader pluginClassLoader)) {
      return List.of();
    }
    List<String> classpath = new ArrayList<>();
    for (URL url : pluginClassLoader.getURLs()) {
      classpath.add(encode(url));
    }
    return classpath;
  }

  static String encode(URL url) {
    if (!"file".equals(url.getProtocol())) {
      return url.toString();
    }
    try {
      return new URI("file", null, decodedPath(url), null).toString().replace(";", "%3B");
    } catch (URISyntaxException e) {
      return url.toString();
    }
  }

  /** The path of an encoded URL ({@code my%20hop}) or of a decoded one ({@code my hop}). */
  private static String decodedPath(URL url) {
    try {
      return url.toURI().getPath();
    } catch (URISyntaxException e) {
      // A decoded URL: its path holds characters a URI does not allow, as they are
      return url.getPath();
    }
  }

  private void deleteFlinkJobConfiguration() {
    Path dir = flinkJobConfDir;
    flinkJobConfDir = null;
    if (dir == null) {
      return;
    }
    try {
      FlinkJobConfiguration.delete(dir);
    } catch (IOException e) {
      logChannel.logDebug("Could not remove temporary Flink configuration directory " + dir, e);
    }
  }
}
