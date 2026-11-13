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

import java.util.ArrayList;
import java.util.List;

/** The Spark settings the Spark settings table suggests, per cluster type. */
public final class SparkSettingKeys {

  /** Settings every cluster type uses. */
  static final List<String> COMMON =
      List.of(
          "spark.sql.shuffle.partitions",
          "spark.default.parallelism",
          "spark.sql.adaptive.enabled",
          "spark.executor.instances",
          "spark.dynamicAllocation.enabled",
          "spark.dynamicAllocation.minExecutors",
          "spark.dynamicAllocation.maxExecutors",
          "spark.executor.memoryOverhead",
          "spark.driver.cores",
          "spark.driver.host",
          "spark.driver.port",
          "spark.blockManager.port",
          "spark.network.timeout",
          "spark.ui.enabled",
          "spark.hadoop.fs.s3a.endpoint",
          "spark.hadoop.fs.s3a.path.style.access",
          "spark.hadoop.fs.s3a.access.key",
          "spark.hadoop.fs.s3a.secret.key");

  static final List<String> STANDALONE = List.of("spark.cores.max");

  static final List<String> YARN =
      List.of(
          "spark.yarn.queue",
          "spark.yarn.maxAppAttempts",
          "spark.yarn.archive",
          "spark.kerberos.principal",
          "spark.kerberos.keytab");

  static final List<String> KUBERNETES =
      List.of(
          "spark.kubernetes.namespace",
          "spark.kubernetes.container.image",
          "spark.kubernetes.container.image.pullPolicy",
          "spark.kubernetes.container.image.pullSecrets",
          "spark.kubernetes.authenticate.driver.serviceAccountName",
          "spark.kubernetes.driver.request.cores",
          "spark.kubernetes.executor.request.cores");

  private SparkSettingKeys() {}

  /** The keys of {@code type} first, then the ones every cluster type uses. */
  public static List<String> suggestions(SparkClusterType type) {
    List<String> keys = new ArrayList<>();
    if (type == SparkClusterType.STANDALONE) {
      keys.addAll(STANDALONE);
    } else if (type == SparkClusterType.YARN) {
      keys.addAll(YARN);
    } else if (type == SparkClusterType.KUBERNETES) {
      keys.addAll(KUBERNETES);
    }
    keys.addAll(COMMON);
    return keys;
  }
}
