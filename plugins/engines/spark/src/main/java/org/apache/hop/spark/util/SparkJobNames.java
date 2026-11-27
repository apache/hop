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

package org.apache.hop.spark.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Names the Spark jobs, stages and SQL queries of a pipeline after the pipeline and the transform
 * that starts them, in the Spark UI and on Databricks. Without it Spark names them after the line
 * of code that started them, such as {@code count at SparkPipelineEngine.java:411} or a lambda.
 *
 * <p>Spark keeps these as local properties of the thread that runs the actions, so they are set on
 * the thread that builds and runs the pipeline. A stage runs all transforms between two shuffles
 * and is named after the transform whose output started it.
 */
public final class SparkJobNames {

  private SparkJobNames() {}

  /** The job group of a run: every job of the run is in it. */
  public static String groupId(String logChannelId) {
    return "hop-" + StringUtils.defaultString(logChannelId);
  }

  /** {@code Hop pipeline 'name'}: the description of the job group. */
  public static String pipeline(String pipelineName) {
    return "Hop pipeline '" + StringUtils.defaultString(pipelineName) + "'";
  }

  /** {@code Hop pipeline 'name', transform 'name' (plugin id)}: the description of one job. */
  public static String transform(String pipelineName, String transformName, String pluginId) {
    return pipeline(pipelineName)
        + ", transform '"
        + StringUtils.defaultString(transformName)
        + "'"
        + (StringUtils.isEmpty(pluginId) ? "" : " (" + pluginId + ")");
  }

  /** {@code Hop name → transform}: the short name the jobs and stages get in the Spark UI. */
  public static String callSite(String pipelineName, String transformName) {
    return "Hop "
        + StringUtils.defaultString(pipelineName)
        + " → "
        + StringUtils.defaultString(transformName);
  }

  /** Put the jobs that this thread starts in the job group of the run. */
  public static void forPipeline(SparkSession spark, String groupId, String pipelineName) {
    SparkContext context = context(spark);
    if (context != null) {
      context.setJobGroup(groupId, pipeline(pipelineName), false);
    }
  }

  /** Name the jobs that this thread starts next after a transform. */
  public static void forTransform(
      SparkSession spark, String pipelineName, String transformName, String pluginId) {
    SparkContext context = context(spark);
    if (context != null) {
      context.setJobDescription(transform(pipelineName, transformName, pluginId));
      context.setCallSite(callSite(pipelineName, transformName));
    }
  }

  /** Forget the names: later jobs of this thread get Spark's own names again. */
  public static void clear(SparkSession spark) {
    SparkContext context = context(spark);
    if (context != null) {
      context.clearCallSite();
      context.clearJobGroup();
    }
  }

  private static SparkContext context(SparkSession spark) {
    try {
      return spark == null ? null : spark.sparkContext();
    } catch (RuntimeException e) {
      // No SparkContext (Spark Connect): Spark keeps its own names
      return null;
    }
  }
}
