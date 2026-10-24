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

package org.apache.hop.replay;

public class ReplayDefaults {

  public static final String REPLAY_GATE_GROUP = "replay-gate";

  public static final String GATE_ATTR_ENABLED = "enabled";
  public static final String GATE_ATTR_SPOOL_DIR = "spool_directory";
  public static final String GATE_ATTR_COMPRESSION = "compression";
  public static final String GATE_ATTR_ROW_LIMIT = "row_limit";
  public static final String GATE_ATTR_DESCRIPTION = "description";

  public static final String DEFAULT_SPOOL_DIR = "${PROJECT_HOME}/.hop/spool";
  public static final String DEFAULT_COMPRESSION = "Snappy";

  public static final String COMPRESSION_NONE = "None";
  public static final String COMPRESSION_SNAPPY = "Snappy";
  public static final String COMPRESSION_GZIP = "Gzip";

  public static final String[] COMPRESSION_OPTIONS =
      new String[] {COMPRESSION_SNAPPY, COMPRESSION_GZIP, COMPRESSION_NONE};

  private ReplayDefaults() {
    // Utility class
  }
}
