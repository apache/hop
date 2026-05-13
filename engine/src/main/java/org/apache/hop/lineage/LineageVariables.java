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

package org.apache.hop.lineage;

import org.apache.hop.core.variables.Variable;
import org.apache.hop.core.variables.VariableScope;

/** Hop variables that configure the lineage observation hub and sinks. */
public final class LineageVariables {

  private LineageVariables() {}

  @Variable(
      scope = VariableScope.ENGINE,
      value = "N",
      description =
          "Set to Y to enable the lineage hub (async dispatch of lineage events to registered sinks).")
  public static final String HOP_LINEAGE_ENABLED = "HOP_LINEAGE_ENABLED";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "10000",
      description =
          "Maximum number of lineage events queued in memory before new events are dropped.")
  public static final String HOP_LINEAGE_QUEUE_CAPACITY = "HOP_LINEAGE_QUEUE_CAPACITY";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "100",
      description = "Maximum lineage events per batch delivered to sinks.")
  public static final String HOP_LINEAGE_BATCH_MAX = "HOP_LINEAGE_BATCH_MAX";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "250",
      description =
          "Maximum time in milliseconds to wait for more lineage events before dispatching a partial batch.")
  public static final String HOP_LINEAGE_BATCH_LINGER_MS = "HOP_LINEAGE_BATCH_LINGER_MS";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "",
      description =
          "Comma-separated lineage sink plugin ids to enable; leave empty to register all discovered sinks.")
  public static final String HOP_LINEAGE_SINK_IDS = "HOP_LINEAGE_SINK_IDS";
}
