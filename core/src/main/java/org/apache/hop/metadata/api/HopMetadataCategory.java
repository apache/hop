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

/**
 * Stable category ids referenced by {@link HopMetadata#category()}. These ids only group metadata
 * types for presentation; the human readable label, display order and icon are resolved in the UI
 * layer so that this {@code core} class carries no presentation concerns.
 */
public final class HopMetadataCategory {

  /** Database, NoSQL, graph and other data-source connections. */
  public static final String CONNECTIONS = "connections";

  /** Virtual File System / cloud storage connections (S3, Azure, GCS, MinIO, WebDAV, ...). */
  public static final String FILE_STORAGE = "file-storage";

  /** Pipeline and workflow run configurations. */
  public static final String RUN_CONFIG = "run-config";

  /** Hop servers and published web services. */
  public static final String SERVERS = "servers";

  /** Execution information locations and data profiling. */
  public static final String EXECUTION = "execution";

  /** Pipeline/workflow logging, probes and log readers. */
  public static final String LOGGING = "logging";

  /** Unit tests and data sets. */
  public static final String TESTING = "testing";

  /** Schema, partition and file definitions describing data shape. */
  public static final String DATA_DEFINITION = "data-definition";

  /** Variable resolvers. */
  public static final String VARIABLES = "variables";

  private HopMetadataCategory() {
    // Constants holder, do not instantiate.
  }
}
