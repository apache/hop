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

package org.apache.hop.lineage.hub;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.LineageVariables;

/** Effective lineage hub settings resolved from Hop variables. */
@Getter
public final class LineageConfiguration {

  private final boolean enabled;
  private final int queueCapacity;
  private final int batchMax;
  private final long batchLingerMs;

  /**
   * Empty means all discovered sinks are eligible; otherwise only ids in this set
   * (case-insensitive) are loaded.
   */
  private final Set<String> sinkIds;

  private LineageConfiguration(
      boolean enabled, int queueCapacity, int batchMax, long batchLingerMs, Set<String> sinkIds) {
    this.enabled = enabled;
    this.queueCapacity = queueCapacity;
    this.batchMax = batchMax;
    this.batchLingerMs = batchLingerMs;
    this.sinkIds = sinkIds;
  }

  /** Builds a fixed configuration for unit tests without reading Hop variables. */
  public static LineageConfiguration forTesting(
      boolean enabled, int queueCapacity, int batchMax, long batchLingerMs, Set<String> sinkIds) {
    return new LineageConfiguration(enabled, queueCapacity, batchMax, batchLingerMs, sinkIds);
  }

  public static LineageConfiguration resolve() {
    Variables variables = new Variables();
    variables.initializeFrom(null);
    return resolve(variables);
  }

  public static LineageConfiguration resolve(IVariables variables) {
    String enabledRaw = variables.getVariable(LineageVariables.HOP_LINEAGE_ENABLED, "N");
    boolean enabled = "Y".equalsIgnoreCase(enabledRaw);

    int queueCapacity =
        parsePositiveInt(
            variables.getVariable(LineageVariables.HOP_LINEAGE_QUEUE_CAPACITY, "10000"), 10000);
    int batchMax =
        parsePositiveInt(variables.getVariable(LineageVariables.HOP_LINEAGE_BATCH_MAX, "100"), 100);
    long lingerMs =
        parsePositiveLong(
            variables.getVariable(LineageVariables.HOP_LINEAGE_BATCH_LINGER_MS, "250"), 250L);

    String rawIds = variables.getVariable(LineageVariables.HOP_LINEAGE_SINK_IDS, "");
    Set<String> sinkIds;
    if (Utils.isEmpty(rawIds)) {
      sinkIds = Collections.emptySet();
    } else {
      sinkIds =
          Arrays.stream(rawIds.split(","))
              .map(s -> s.trim().toLowerCase(Locale.ROOT))
              .filter(StringUtils::isNotEmpty)
              .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    return new LineageConfiguration(enabled, queueCapacity, batchMax, lingerMs, sinkIds);
  }

  private static int parsePositiveInt(String raw, int defaultValue) {
    try {
      int v = Integer.parseInt(raw.trim());
      return v > 0 ? v : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static long parsePositiveLong(String raw, long defaultValue) {
    try {
      long v = Long.parseLong(raw.trim());
      return v > 0 ? v : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
