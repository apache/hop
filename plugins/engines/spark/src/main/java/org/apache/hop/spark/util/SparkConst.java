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

public final class SparkConst {

  public static final String PLUGIN_ID = "SparkPipelineEngine";
  public static final String PLUGIN_NAME = "Native Spark pipeline engine";

  public static final String INJECTOR_TRANSFORM_NAME = "_INJECTOR_";

  public static final String MEMORY_GROUP_BY_PLUGIN_ID = "MemoryGroupBy";
  public static final String MERGE_JOIN_PLUGIN_ID = "MergeJoin";
  public static final String UNIQUE_ROWS_PLUGIN_ID = "Unique";
  public static final String SORT_ROWS_PLUGIN_ID = "SortRows";
  public static final String GROUP_BY_PLUGIN_ID = "GroupBy";

  public static final String SPARK_FILE_INPUT_PLUGIN_ID = "SparkFileInput";
  public static final String SPARK_FILE_OUTPUT_PLUGIN_ID = "SparkFileOutput";

  private SparkConst() {}
}
