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

package org.apache.hop.core.cluster;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.fatjar.IStagingStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * The payload of {@link HopExtensionPoint#SparkClusterTarget}: the native Spark engine asks for the
 * submitter and staging store of a cluster type that another plugin provides, such as Databricks.
 * The plugin that knows the cluster type sets both; the others leave them alone.
 */
@Getter
public class SparkClusterTarget {

  /** Setting: the name of the connection to the platform, for example a Databricks connection. */
  public static final String SETTING_CONNECTION = "connection";

  /** Setting: the compute to run on, for example a cluster id or {@code new_cluster}. */
  public static final String SETTING_COMPUTE = "compute";

  /** Setting: the runtime version of a cluster that is created for the run. */
  public static final String SETTING_RUNTIME = "runtime";

  /** Setting: the node type of a cluster that is created for the run. */
  public static final String SETTING_NODE_TYPE = "nodeType";

  /** Setting: the number of workers of a cluster that is created for the run. */
  public static final String SETTING_WORKERS = "workers";

  /** Setting: the folder the fat jar and the project package are staged in. */
  public static final String SETTING_STAGING_FOLDER = "stagingFolder";

  private final ILogChannel log;
  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;

  /** The code of the cluster type, for example {@code DATABRICKS}. */
  private final String clusterType;

  /** The settings of the run configuration for this cluster type, resolved. */
  private final Map<String, String> settings;

  @Setter private ISparkClusterSubmitter submitter;
  @Setter private IStagingStore stagingStore;

  public SparkClusterTarget(
      ILogChannel log,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String clusterType,
      Map<String, String> settings) {
    this.log = log;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.clusterType = clusterType;
    this.settings = settings;
  }

  /** A setting, or an empty string when it is not set. */
  public String getSetting(String name) {
    String value = settings == null ? null : settings.get(name);
    return value == null ? "" : value;
  }
}
