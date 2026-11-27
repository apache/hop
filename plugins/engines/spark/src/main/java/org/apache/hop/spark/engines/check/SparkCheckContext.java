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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;

/** What the checks of a native Spark run configuration work from, and what they found so far. */
public class SparkCheckContext implements AutoCloseable {

  private final SparkPipelineRunConfiguration configuration;
  private final IVariables variables;
  private final Map<String, ICheckResult> results = new LinkedHashMap<>();
  private final Map<String, Object> found = new HashMap<>();
  private final List<AutoCloseable> closeables = new ArrayList<>();

  /** The metadata the test job runs with; null when there is no test job. */
  private final IHopMetadataProvider metadataProvider;

  /** Cancels a test job that runs. */
  private final IProgressMonitor monitor;

  /** Whether the test job runs after the checks. */
  private final boolean testJob;

  public SparkCheckContext(SparkPipelineRunConfiguration configuration, IVariables variables) {
    this(configuration, variables, null, null);
  }

  /** With a metadata provider the test job runs after the checks. */
  public SparkCheckContext(
      SparkPipelineRunConfiguration configuration,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      IProgressMonitor monitor) {
    this(configuration, variables, metadataProvider, monitor, metadataProvider != null);
  }

  /**
   * @param metadataProvider the project metadata: the connections the checks use, and the metadata
   *     of the test job
   * @param testJob whether the test job runs after the checks; it needs the metadata provider
   */
  public SparkCheckContext(
      SparkPipelineRunConfiguration configuration,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      IProgressMonitor monitor,
      boolean testJob) {
    this.configuration = configuration;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.monitor = monitor;
    this.testJob = testJob && metadataProvider != null;
  }

  /** Whether the test runs the test job as well: it has the metadata to run it with. */
  public boolean isRunningTestJob() {
    return testJob;
  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /** Says what a check that takes a while is doing, under the check's name. */
  public void progress(String text) {
    if (monitor != null) {
      monitor.subTask(text);
    }
  }

  public boolean isCanceled() {
    return monitor != null && monitor.isCanceled();
  }

  /** Whether a check that ran so far failed. */
  public boolean hasFailures() {
    return results.values().stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR);
  }

  public SparkPipelineRunConfiguration getConfiguration() {
    return configuration;
  }

  public IVariables getVariables() {
    return variables;
  }

  /** A setting with its variables resolved, never null. */
  public String resolve(String value) {
    String resolved = value == null ? null : variables.resolve(value);
    return resolved == null ? "" : resolved.trim();
  }

  public String getMaster() {
    return resolve(configuration.getSparkMaster());
  }

  /** The cluster type chosen, or the one the resolved Spark master points to. */
  public SparkClusterType getClusterType() {
    return configuration.getClusterType() != null
        ? configuration.getClusterType()
        : SparkClusterType.fromMaster(getMaster());
  }

  /** Whether the driver is submitted to the cluster: then the jar and pipeline are staged. */
  public boolean isClusterMode() {
    SparkClusterType type = getClusterType();
    return type != null
        && type.hasClusterDeployMode()
        && SparkClusterType.submitsTheDriver(type, configuration.getDeployMode());
  }

  /** Whether the pipeline runs on executors of a cluster rather than in this process. */
  public boolean isOnACluster() {
    SparkClusterType type = getClusterType();
    return type != null && type != SparkClusterType.LOCAL;
  }

  void putResult(String checkId, ICheckResult result) {
    results.put(checkId, result);
  }

  /** The result of a check that ran before, null when it did not run. */
  public ICheckResult getResult(String checkId) {
    return results.get(checkId);
  }

  /** Keeps something a check found (a configuration, a client) for the checks after it. */
  public void put(String key, Object value) {
    found.put(key, value);
    if (value instanceof AutoCloseable closeable) {
      closeables.add(closeable);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key) {
    return (T) found.get(key);
  }

  /**
   * The {@code key=value} lines of the Spark config, resolved: {@code spark.yarn.queue}, {@code
   * spark.kubernetes.namespace} ...
   */
  public Map<String, String> getSparkConfigs() {
    Map<String, String> configs = new LinkedHashMap<>();
    String lines = configuration.getSparkConfigs();
    if (lines == null) {
      return configs;
    }
    for (String line : lines.split("\\r?\\n")) {
      String trimmed = line.trim();
      int eq = trimmed.indexOf('=');
      if (!trimmed.startsWith("#") && eq > 0) {
        configs.put(resolve(trimmed.substring(0, eq)), resolve(trimmed.substring(eq + 1)));
      }
    }
    return configs;
  }

  /** Closes what the checks opened (a YARN or Kubernetes client). */
  @Override
  public void close() {
    for (AutoCloseable closeable : closeables) {
      try {
        closeable.close();
      } catch (Exception e) {
        // Done with it either way
      }
    }
    closeables.clear();
  }
}
