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

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variable;
import org.apache.hop.core.variables.VariableScope;
import org.apache.hop.core.variables.Variables;

/** Hop variables that configure the lineage observation hub and sinks. */
public final class LineageVariables {

  /** Prefix shared by every lineage hub and sink configuration variable. */
  private static final String LINEAGE_PREFIX = "HOP_LINEAGE";

  private LineageVariables() {}

  /**
   * Variable space used to resolve the engine-scoped lineage settings (hub configuration and sink
   * plugin settings), with {@code HOP_LINEAGE_*} OS environment variables overlaid so they can be
   * injected via Docker, Kubernetes or CI — the documented deployment path.
   *
   * <p>The environment is overlaid <b>unconditionally</b> for these keys, on purpose: {@code
   * HopEnvironment} publishes the described ENGINE-variable <i>defaults</i> (e.g. {@code
   * HOP_LINEAGE_ENABLED=N}) as JVM system properties during initialization, which {@link
   * Variables#initializeFrom} then loads. A plain "only when not already a system property" overlay
   * would therefore always lose to that default and the environment would be silently ignored. For
   * the {@code HOP_LINEAGE_*} settings the environment wins over the system-property default; this
   * is scoped to that prefix so no other variables are affected.
   */
  public static IVariables engineVariables() {
    Variables variables = new Variables();
    variables.initializeFrom(null);
    System.getenv()
        .forEach(
            (key, value) -> {
              if (key.startsWith(LINEAGE_PREFIX)) {
                variables.setVariable(key, value);
              }
            });
    return variables;
  }

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
