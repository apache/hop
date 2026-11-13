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

import org.apache.hop.core.ICheckResult;

/** Cluster mode uploads the fat jar and the project package to the staging folder. */
public class StagingFolderCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "StagingFolder";

  public StagingFolderCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.isClusterMode();
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    String folder = context.resolve(context.getConfiguration().getStagingFolder());
    if (folder.isEmpty()) {
      return error(
          "No staging folder is set: cluster mode uploads the fat jar and the pipeline to it.",
          "Set the staging folder on the Cluster tab: an S3, Azure, Google, MinIO or HDFS location,"
              + " or a file system path the cluster shares.");
    }
    try {
      probeWrite(folder, context.getVariables());
      return ok("'" + folder + "' can be written.");
    } catch (Exception e) {
      return error(
          "'" + folder + "' can't be written: " + e.getMessage() + ".",
          "Check that the folder (bucket, container) exists and that the credentials of its"
              + " connection may write to it.");
    }
  }
}
