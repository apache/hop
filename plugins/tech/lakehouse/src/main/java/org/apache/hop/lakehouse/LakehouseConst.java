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

package org.apache.hop.lakehouse;

/**
 * Plugin IDs of the lake table transforms. They keep their original values so that existing
 * pipelines open unchanged.
 */
public final class LakehouseConst {

  public static final String LAKE_TABLE_INPUT_PLUGIN_ID = "SparkLakeTableInput";
  public static final String LAKE_TABLE_OUTPUT_PLUGIN_ID = "SparkLakeTableOutput";
  public static final String LAKE_TABLE_MERGE_PLUGIN_ID = "SparkLakeTableMerge";
  public static final String LAKE_TABLE_MAINTENANCE_PLUGIN_ID = "SparkLakeTableMaintenance";

  /** ID of the native Spark pipeline engine plugin. */
  public static final String SPARK_ENGINE_ID = "SparkPipelineEngine";

  private LakehouseConst() {}
}
