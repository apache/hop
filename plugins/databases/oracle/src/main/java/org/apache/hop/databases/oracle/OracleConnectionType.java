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

package org.apache.hop.databases.oracle;

/**
 * How the "Database name" field of an Oracle connection should be interpreted when building the
 * JDBC URL.
 *
 * <p>The constant names are shown as-is in the connection dialog and are what gets serialized, so
 * renaming one breaks existing connections.
 */
public enum OracleConnectionType {
  /**
   * Historical Hop behaviour, kept as the default so existing connections keep working: the
   * database name is a SID, unless it starts with {@code /} (service name) or {@code :} (SID), or
   * unless host and port are both empty in which case it is taken to be a full TNS descriptor.
   */
  AUTOMATIC,

  /** The database name is a SID: {@code jdbc:oracle:thin:@host:port:SID}. */
  SID,

  /** The database name is a service name: {@code jdbc:oracle:thin:@//host:port/service}. */
  SERVICE_NAME,

  /**
   * The database name is an alias from a {@code tnsnames.ora} file: {@code
   * jdbc:oracle:thin:@alias}. Host and port are ignored; point TNS_ADMIN at the directory holding
   * the file.
   */
  TNS_ALIAS,

  /**
   * The database name is a complete TNS descriptor, used verbatim. Host and port are ignored. Use
   * this for RAC address lists, or any descriptor Hop cannot generate itself.
   */
  DESCRIPTOR
}
