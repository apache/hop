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

package org.apache.hop.core.logging;

import org.apache.hop.core.metrics.MetricsSnapshotType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class Metrics implements IMetrics {

  // Database
  //
  public static Metrics METRIC_DATABASE_CONNECT_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_CONNECT", "Connect to database" );
  public static Metrics METRIC_DATABASE_CONNECT_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_CONNECT", "Connect to database" );
  public static Metrics METRIC_DATABASE_PREPARE_SQL_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_PREPARE_SQL", "Prepare SQL statement" );
  public static Metrics METRIC_DATABASE_PREPARE_SQL_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_PREPARE_SQL", "Prepare SQL statement" );
  public static Metrics METRIC_DATABASE_CREATE_SQL_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_CREATE_SQL", "Create SQL statement" );
  public static Metrics METRIC_DATABASE_CREATE_SQL_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_CREATE_SQL", "Create SQL statement" );
  public static Metrics METRIC_DATABASE_SQL_VALUES_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_SQL_VALUES", "Set values on perpared statement" );
  public static Metrics METRIC_DATABASE_SQL_VALUES_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_SQL_VALUES", "Set values on perpared statement" );
  public static Metrics METRIC_DATABASE_EXECUTE_SQL_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_EXECUTE_SQL", "Execute SQL statement" );
  public static Metrics METRIC_DATABASE_EXECUTE_SQL_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_EXECUTE_SQL", "Execute SQL statement" );
  public static Metrics METRIC_DATABASE_OPEN_QUERY_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_OPEN_QUERY", "Open SQL query" );
  public static Metrics METRIC_DATABASE_OPEN_QUERY_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_OPEN_QUERY", "Open SQL query" );
  public static Metrics METRIC_DATABASE_GET_ROW_META_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_GET_ROW_META", "Get row metadata" );
  public static Metrics METRIC_DATABASE_GET_ROW_META_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_GET_ROW_META", "Get row metadata" );
  public static Metrics METRIC_DATABASE_SET_LOOKUP_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_SET_LOOKUP", "Set lookup values" );
  public static Metrics METRIC_DATABASE_SET_LOOKUP_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_SET_LOOKUP", "Set lookup values" );
  public static Metrics METRIC_DATABASE_PREPARE_UPDATE_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_PREPARE_UPDATE", "Prepare update" );
  public static Metrics METRIC_DATABASE_PREPARE_UPDATE_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_PREPARE_UPDATE", "Prepare update" );
  public static Metrics METRIC_DATABASE_PREPARE_DELETE_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_PREPARE_DELETE", "Prepare delete" );
  public static Metrics METRIC_DATABASE_PREPARE_DELETE_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_PREPARE_DELETE", "Prepare delete" );
  public static Metrics METRIC_DATABASE_PREPARE_DBPROC_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_PREPARE_DBPROC", "Prepare DB procedure" );
  public static Metrics METRIC_DATABASE_PREPARE_DBPROC_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_PREPARE_DBPROC", "Prepare DB procedure" );
  public static Metrics METRIC_DATABASE_GET_LOOKUP_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_GET_LOOKUP", "Get lookup" );
  public static Metrics METRIC_DATABASE_GET_LOOKUP_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_GET_LOOKUP", "Get lookup" );
  public static Metrics METRIC_DATABASE_GET_DBMETA_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_DATABASE_GET_DBMETA", "Get DB metadata" );
  public static Metrics METRIC_DATABASE_GET_DBMETA_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_DATABASE_GET_DBMETA", "Get DB metadata" );

  public static Metrics METRIC_DATABASE_GET_ROW_COUNT = new Metrics(
    MetricsSnapshotType.COUNT, "METRIC_DATABASE_GET_ROW_COUNT", "Get row from DB (count)" );
  public static Metrics METRIC_DATABASE_GET_ROW_SUM_TIME = new Metrics(
    MetricsSnapshotType.SUM, "METRIC_DATABASE_GET_ROW_SUM_TIME", "Get row from DB (total time)" );
  public static Metrics METRIC_DATABASE_GET_ROW_MIN_TIME = new Metrics(
    MetricsSnapshotType.MIN, "METRIC_DATABASE_GET_ROW_MIN_TIME", "Get row from DB (min time)" );
  public static Metrics METRIC_DATABASE_GET_ROW_MAX_TIME = new Metrics(
    MetricsSnapshotType.MAX, "METRIC_DATABASE_GET_ROW_MAX_TIME", "Get row from DB (max time)" );

  // Pipeline
  //
  public static Metrics METRIC_PIPELINE_EXECUTION_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_PIPELINE_EXECUTION", "Execute a pipeline" );
  public static Metrics METRIC_PIPELINE_EXECUTION_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_PIPELINE_EXECUTION", "Execute a pipeline" );
  public static Metrics METRIC_PIPELINE_INIT_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_PIPELINE_INIT", "Initialize a pipeline" );
  public static Metrics METRIC_PIPELINE_INIT_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_PIPELINE_INIT", "Initialize a pipeline" );
  public static Metrics METRIC_TRANSFORM_EXECUTION_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_TRANSFORM_EXECUTION", "Execute a transform" );
  public static Metrics METRIC_TRANSFORM_EXECUTION_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_TRANSFORM_EXECUTION", "Execute a transform" );
  public static Metrics METRIC_TRANSFORM_INIT_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_TRANSFORM_INIT", "Initialize a transform" );
  public static Metrics METRIC_TRANSFORM_INIT_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_TRANSFORM_INIT", "Initialize a transform" );

  // Logging back-end
  //
  public static Metrics METRIC_CENTRAL_LOG_STORE_TIMEOUT_CLEAN_TIME = new Metrics(
    MetricsSnapshotType.SUM, "METRIC_CENTRAL_LOG_STORE_TIMEOUT_CLEAN_TIME",
    "Time consumed removing old log records (ms)" );
  public static Metrics METRIC_CENTRAL_LOG_STORE_TIMEOUT_CLEAN_COUNT = new Metrics(
    MetricsSnapshotType.COUNT, "METRIC_CENTRAL_LOG_STORE_TIMEOUT_CLEAN_COUNT",
    "Number of times removed old log records" );
  public static Metrics METRIC_LOGGING_REGISTRY_CLEAN_TIME = new Metrics(
    MetricsSnapshotType.SUM, "METRIC_LOGGING_REGISTRY_CLEAN_TIME",
    "Time consumed removing old log registry entries (ms)" );
  public static Metrics METRIC_LOGGING_REGISTRY_CLEAN_COUNT = new Metrics(
    MetricsSnapshotType.COUNT, "METRIC_LOGGING_REGISTRY_CLEAN_COUNT",
    "Number of times removed old log registry entries" );
  public static Metrics METRIC_LOGGING_REGISTRY_GET_CHILDREN_TIME = new Metrics(
    MetricsSnapshotType.SUM, "METRIC_LOGGING_REGISTRY_GET_CHILDREN_TIME",
    "Time consumed getting log registry children (ms)" );
  public static Metrics METRIC_LOGGING_REGISTRY_GET_CHILDREN_COUNT = new Metrics(
    MetricsSnapshotType.COUNT, "METRIC_LOGGING_REGISTRY_GET_CHILDREN_COUNT",
    "Number of times retrieved log registry children" );

  // Workflow
  //
  public static Metrics METRIC_WORKFLOW_START = new Metrics(
    MetricsSnapshotType.START, "METRIC_JOB_EXECUTION", "Execute a workflow" );
  public static Metrics METRIC_WORKFLOW_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_JOB_EXECUTION", "Execute a workflow" );
  public static Metrics METRIC_ACTION_START = new Metrics(
  // TODO: Rename METRIC_JOBENTRY_EXECUTION to METRIC_ACTION_EXECUTION
    MetricsSnapshotType.START, "METRIC_JOBENTRY_EXECUTION", "Execute a action" );
  // TODO: Rename METRIC_JOBENTRY_STOP to METRIC_ACTION_STOP
  public static Metrics METRIC_ACTION_STOP = new Metrics(
    MetricsSnapshotType.STOP, "METRIC_JOBENTRY_EXECUTION", "Execute a action" );

  private String code;
  private String description;
  private MetricsSnapshotType type;

  public Metrics( MetricsSnapshotType type, String code, String description ) {
    this.type = type;
    this.code = code;
    this.description = description;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public MetricsSnapshotType getType() {
    return type;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( !( obj instanceof IMetrics ) ) {
      return false;
    }
    if ( this == obj ) {
      return true;
    }

    return ( (IMetrics) obj ).getCode().equalsIgnoreCase( code );
  }

  public static List<IMetrics> getDefaultMetrics() {
    List<IMetrics> metrics = new ArrayList<>();

    for ( Field field : Metrics.class.getDeclaredFields() ) {
      if ( field.getType().equals( Metrics.class ) && field.getName().startsWith( "METRIC_" ) ) {
        field.setAccessible( true );
        try {
          metrics.add( (IMetrics) field.get( null ) );
        } catch ( Exception e ) {
          e.printStackTrace(); // it either works or doesn't, seems more like a JRE problem if it doesn't.
        }
      }
    }

    return metrics;
  }
}
