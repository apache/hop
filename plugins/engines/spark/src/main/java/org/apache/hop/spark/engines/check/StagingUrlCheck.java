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

import java.net.URI;
import java.util.Set;
import org.apache.hop.core.ICheckResult;

/** The cluster can read the staging folder: under its own URL, or under the Hop location. */
public class StagingUrlCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "StagingUrl";

  /** Schemes Spark and Hadoop read without Hop. */
  static final Set<String> CLUSTER_SCHEMES =
      Set.of(
          "hdfs", "viewfs", "s3a", "s3", "abfs", "abfss", "wasb", "wasbs", "gs", "http", "https",
          "dbfs", "file");

  public StagingUrlCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.isClusterMode()
        && !context.resolve(context.getConfiguration().getStagingFolder()).isEmpty();
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    String url = context.resolve(context.getConfiguration().getStagingClusterUrl());
    boolean own = !url.isEmpty();
    String read = own ? url : context.resolve(context.getConfiguration().getStagingFolder());
    String scheme = scheme(read);
    if (scheme == null || scheme.equals("file")) {
      return warning(
          "The cluster reads the staging folder as the local path '"
              + read
              + "', which only works when every cluster node mounts that same path.",
          "Set the staging folder URL for the cluster, for example 's3a://bucket/staging' or"
              + " 'hdfs://namenode/staging'.");
    }
    if (!CLUSTER_SCHEMES.contains(scheme)) {
      return error(
          "The cluster reads the staging folder as '"
              + read
              + "', but '"
              + scheme
              + "' is a Hop connection that Spark can't read.",
          "Set the staging folder URL for the cluster: how the cluster reaches the same folder,"
              + " for example 's3a://bucket/staging' or 'http://minio:9000/bucket/staging'.");
    }
    return ok("The cluster reads the staging folder as '" + read + "'.");
  }

  static String scheme(String location) {
    try {
      String scheme = URI.create(location.replace(" ", "%20")).getScheme();
      return scheme == null || scheme.length() == 1 ? null : scheme.toLowerCase();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
