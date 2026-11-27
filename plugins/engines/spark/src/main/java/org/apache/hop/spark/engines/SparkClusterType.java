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

package org.apache.hop.spark.engines;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Where a native Spark pipeline runs. It decides which options of the run configuration apply and
 * which checks the configuration test runs; the Spark master stays what the engine connects to.
 */
public enum SparkClusterType implements IEnumHasCodeAndDescription {
  /** Spark in the Hop process: {@code local[*]}. */
  LOCAL("local[*]"),
  /** A Spark standalone master: {@code spark://host:7077}. */
  STANDALONE("spark://host:7077"),
  /** Hadoop YARN: {@code yarn}. */
  YARN("yarn"),
  /** Kubernetes: {@code k8s://https://host:6443}. */
  KUBERNETES("k8s://https://host:6443"),
  /**
   * The pipeline is started with spark-submit, which sets the master: it stays empty here. For
   * Databricks, also for the jobs of the Databricks job run action, use {@link #DATABRICKS}.
   */
  SUBMITTED(""),
  /**
   * A Databricks cluster. Run from Hop, the pipeline goes to the cluster as a one-time job run,
   * with the fat jar and the project package staged on a volume through the Databricks plugin. In a
   * Databricks job, such as one the Databricks job run action deploys, it runs on the job's
   * cluster. Databricks sets the master.
   */
  DATABRICKS("");

  private final String masterTemplate;

  SparkClusterType(String masterTemplate) {
    this.masterTemplate = masterTemplate;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(SparkClusterType.class, "SparkClusterType." + name());
  }

  /** A Spark master of this type, to fill in when the current one is of another type. */
  public String getMasterTemplate() {
    return masterTemplate;
  }

  /**
   * The cluster type a Spark master points to: the type of a run configuration saved before it had
   * one.
   *
   * @return the type, or null when the master is a variable expression or not a Spark master
   */
  public static SparkClusterType fromMaster(String master) {
    String value = StringUtils.trimToEmpty(master).toLowerCase();
    if (value.isEmpty()) {
      return SUBMITTED;
    }
    if (value.startsWith("local")) {
      return LOCAL;
    }
    if (value.startsWith("spark://")) {
      return STANDALONE;
    }
    if (value.equals("yarn") || value.startsWith("yarn-")) {
      return YARN;
    }
    if (value.startsWith("k8s://")) {
      return KUBERNETES;
    }
    return null;
  }

  /** Whether a driver can be submitted to this type in cluster deploy mode. */
  public boolean hasClusterDeployMode() {
    return this == STANDALONE || this == YARN || this == KUBERNETES;
  }

  /**
   * Whether Hop submits the driver to the cluster instead of running it in this process. Databricks
   * always runs the driver on its cluster. Local and spark-submit have no deploy mode: the editor
   * hides it for them but keeps the value, so it is ignored there.
   *
   * @param type the cluster type, or null when the master doesn't tell: then the deploy mode
   *     decides
   */
  public static boolean submitsTheDriver(SparkClusterType type, SparkDeployMode deployMode) {
    if (type == DATABRICKS) {
      return true;
    }
    return (type == null || type.hasClusterDeployMode())
        && SparkDeployMode.orDefault(deployMode) == SparkDeployMode.CLUSTER;
  }

  /** Whether {@code master} is a master of this type. */
  public boolean matchesMaster(String master) {
    if (this == DATABRICKS) {
      // Databricks sets the master of the cluster: the run configuration has none
      return StringUtils.trimToEmpty(master).isEmpty();
    }
    return fromMaster(master) == this;
  }
}
