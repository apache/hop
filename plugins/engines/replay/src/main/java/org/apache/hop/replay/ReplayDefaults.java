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
  public static final String GATE_ATTR_DESCRIPTION = "description";

  public static final String DEFAULT_SPOOL_DIR = "${PROJECT_HOME}/.hop/spool";

  /**
   * Run-scoped variable set by the Execution perspective Replay button. When it is set, sealed
   * gates are read and written under that execution ID instead of the run configuration value.
   */
  public static final String VARIABLE_SOURCE_EXECUTION_ID = "HOP_REPLAY_SOURCE_EXECUTION_ID";

  private ReplayDefaults() {
    // Utility class
  }
}
