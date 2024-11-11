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

package org.apache.hop.metadata.api;

/** A HopMetadataPropertyType provides information about the purpose of a HopMetadataProperty. */
public enum HopMetadataPropertyType {
  NONE,

  // MISC
  HOP_FILE,
  EXEC_INFO_LOCATION,
  EXEC_INFO_DATA_PROFILE,
  PARTITION_SCHEMA,
  CASSANDRA_CONNECTION,
  MONGODB_CONNECTION,
  SPLUNK_CONNECTION,

  // PIPELINE
  PIPELINE_FILE,
  PIPELINE_RUN_CONFIG,
  PIPELINE_LOG,
  PIPELINE_PROBE,
  PIPELINE_UNIT_TEST,
  PIPELINE_DATA_SET,

  // PIPELINE
  BEAM_FILE_DEFINITION,

  // WORKFLOW
  WORKFLOW_FILE,
  WORKFLOW_RUN_CONFIG,
  WORKFLOW_LOG,

  // SERVER
  SERVER_DEFINITION,
  SERVER_WEB_SERVICE,
  SERVER_WEB_SERVICE_ASYNC,

  // FIELDS
  FIELD_LIST,

  // FILES
  FILE_PATH,
  FILE_REFERENCE,
  FILE_NAME,
  FILE_WILDCARD,
  FILE_ENCODING,
  FILE_FIELD,
  FILE_SEPARATOR,
  FILE_ENCLOSURE,
  FILE_EXTENSION,

  // RDBMS
  RDBMS_CONNECTION,
  RDBMS_SCHEMA,
  RDBMS_TABLE,
  RDBMS_COLUMN,
  RDBMS_SQL,
  RDBMS_TRUNCATE,
  RDBMS_SQL_SELECT,
  RDBMS_SQL_INSERT,
  RDBMS_SQL_UPDATE,
  RDBMS_SQL_DELETE,
  RDBMS_SQL_BULK,

  // REST
  REST_CONNECTION,

  // GRAPH
  GRAPH_CONNECTION,
  GRAPH_MODEL,
  GRAPH_NODE,
  GRAPH_QUERY,
  GRAPH_RELATIONSHIP,

  // STATIC SCHEMA
  STATIC_SCHEMA_DEFINITION,

  // VFS
  VFS_AZURE_CONNECTION,
}
