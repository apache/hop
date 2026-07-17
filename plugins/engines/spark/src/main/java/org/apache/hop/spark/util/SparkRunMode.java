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

package org.apache.hop.spark.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;

/**
 * Effective execution mode for generic (mapPartitions) Hop transforms on the native Spark engine.
 *
 * <p>{@link #DISTRIBUTED} runs {@code mapPartitions} across the input Dataset partitions (default).
 * {@link #DRIVER_ONLY} materializes the input on the Spark driver and runs the Hop mini-pipeline
 * once there, so nested work (Workflow Executor, etc.) does not fan out across executors.
 *
 * <p>Per-transform override is stored on {@link TransformMeta#attributesMap} under group {@link
 * #ATTRIBUTE_GROUP} / key {@link #ATTRIBUTE_KEY}.
 */
public enum SparkRunMode {
  DISTRIBUTED,
  DRIVER_ONLY;

  /** Transform attribute group for Spark-specific canvas settings. */
  public static final String ATTRIBUTE_GROUP = "spark";

  /** Transform attribute key for the run-mode override. */
  public static final String ATTRIBUTE_KEY = "run_mode";

  /** Inherit the pipeline run-configuration default (or absent attribute). */
  public static final String OVERRIDE_INHERIT = "INHERIT";

  /** Force {@link #DISTRIBUTED} regardless of run configuration. */
  public static final String OVERRIDE_FORCE_DISTRIBUTED = "FORCE_DISTRIBUTED";

  /** Force {@link #DRIVER_ONLY} regardless of run configuration. */
  public static final String OVERRIDE_FORCE_DRIVER_ONLY = "FORCE_DRIVER_ONLY";

  public static final String DISPLAY_INHERIT = "Inherit";
  public static final String DISPLAY_FORCE_DISTRIBUTED = "Force distributed";
  public static final String DISPLAY_FORCE_DRIVER_ONLY = "Force Driver Only";

  /** Parse a run-configuration value. Unknown / empty values default to {@link #DISTRIBUTED}. */
  public static SparkRunMode parseConfigValue(String raw) {
    if (StringUtils.isBlank(raw)) {
      return DISTRIBUTED;
    }
    String n = raw.trim();
    if (DRIVER_ONLY.name().equalsIgnoreCase(n)
        || "DRIVER ONLY".equalsIgnoreCase(n)
        || "DRIVER-ONLY".equalsIgnoreCase(n)) {
      return DRIVER_ONLY;
    }
    return DISTRIBUTED;
  }

  /**
   * Parse a transform override attribute. Unknown / empty values mean {@link #OVERRIDE_INHERIT}.
   */
  public static String parseOverride(String raw) {
    if (StringUtils.isBlank(raw)) {
      return OVERRIDE_INHERIT;
    }
    String n = raw.trim();
    if (OVERRIDE_FORCE_DISTRIBUTED.equalsIgnoreCase(n) || "FORCE DISTRIBUTED".equalsIgnoreCase(n)) {
      return OVERRIDE_FORCE_DISTRIBUTED;
    }
    if (OVERRIDE_FORCE_DRIVER_ONLY.equalsIgnoreCase(n)
        || "FORCE DRIVER ONLY".equalsIgnoreCase(n)
        || "FORCE_DRIVER".equalsIgnoreCase(n)) {
      return OVERRIDE_FORCE_DRIVER_ONLY;
    }
    if (OVERRIDE_INHERIT.equalsIgnoreCase(n)) {
      return OVERRIDE_INHERIT;
    }
    // Legacy / accidental storage of effective mode names as override
    if (DRIVER_ONLY.name().equalsIgnoreCase(n)) {
      return OVERRIDE_FORCE_DRIVER_ONLY;
    }
    if (DISTRIBUTED.name().equalsIgnoreCase(n)) {
      return OVERRIDE_FORCE_DISTRIBUTED;
    }
    return OVERRIDE_INHERIT;
  }

  /** Read the raw override from a transform (never null; defaults to inherit). */
  public static String getOverride(TransformMeta transformMeta) {
    if (transformMeta == null) {
      return OVERRIDE_INHERIT;
    }
    return parseOverride(transformMeta.getAttribute(ATTRIBUTE_GROUP, ATTRIBUTE_KEY));
  }

  /** Store or clear the override on a transform. Inherit removes the attribute when possible. */
  public static void setOverride(TransformMeta transformMeta, String overrideCode) {
    if (transformMeta == null) {
      return;
    }
    String code = parseOverride(overrideCode);
    if (OVERRIDE_INHERIT.equals(code)) {
      Map<String, String> attrs = transformMeta.getAttributes(ATTRIBUTE_GROUP);
      if (attrs != null) {
        attrs.remove(ATTRIBUTE_KEY);
        if (attrs.isEmpty()) {
          transformMeta.getAttributesMap().remove(ATTRIBUTE_GROUP);
        }
      }
      return;
    }
    transformMeta.setAttribute(ATTRIBUTE_GROUP, ATTRIBUTE_KEY, code);
  }

  /** Resolve the effective mode for a generic transform from override + run configuration. */
  public static SparkRunMode resolve(
      TransformMeta transformMeta, ISparkPipelineEngineRunConfiguration runConfiguration) {
    String override = getOverride(transformMeta);
    if (OVERRIDE_FORCE_DISTRIBUTED.equals(override)) {
      return DISTRIBUTED;
    }
    if (OVERRIDE_FORCE_DRIVER_ONLY.equals(override)) {
      return DRIVER_ONLY;
    }
    String configValue =
        runConfiguration != null ? runConfiguration.getGenericTransformRunMode() : null;
    return parseConfigValue(configValue);
  }

  /** Combo values for the Native Spark run configuration. */
  public static List<String> configDisplayValues() {
    List<String> list = new ArrayList<>(2);
    list.add(DISTRIBUTED.name());
    list.add(DRIVER_ONLY.name());
    return list;
  }

  /** Labels for the transform context-action selection dialog. */
  public static String[] overrideDisplayLabels() {
    return new String[] {DISPLAY_INHERIT, DISPLAY_FORCE_DISTRIBUTED, DISPLAY_FORCE_DRIVER_ONLY};
  }

  public static String overrideFromDisplayLabel(String label) {
    if (DISPLAY_FORCE_DISTRIBUTED.equals(label)) {
      return OVERRIDE_FORCE_DISTRIBUTED;
    }
    if (DISPLAY_FORCE_DRIVER_ONLY.equals(label)) {
      return OVERRIDE_FORCE_DRIVER_ONLY;
    }
    return OVERRIDE_INHERIT;
  }

  public static String displayLabelForOverride(String overrideCode) {
    String code = parseOverride(overrideCode);
    return switch (code) {
      case OVERRIDE_FORCE_DISTRIBUTED -> DISPLAY_FORCE_DISTRIBUTED;
      case OVERRIDE_FORCE_DRIVER_ONLY -> DISPLAY_FORCE_DRIVER_ONLY;
      default -> DISPLAY_INHERIT;
    };
  }

  /** True when a non-inherit override should paint a canvas badge. */
  public static boolean hasForcedOverride(TransformMeta transformMeta) {
    String o = getOverride(transformMeta);
    return OVERRIDE_FORCE_DISTRIBUTED.equals(o) || OVERRIDE_FORCE_DRIVER_ONLY.equals(o);
  }
}
