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

package org.apache.hop.spark.engines.check;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.PathSchemeMapping;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.engines.SparkSetting;

/** The settings can be read: sizes, numbers, and the rows of the Spark settings and path map. */
public class SettingsCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "Settings";

  /** What Spark reads as a size: 512m, 2g, 1024 (MiB) ... */
  private static final Pattern SIZE = Pattern.compile("(?i)\\d+[kmgtp]?b?");

  public SettingsCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return true;
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    SparkPipelineRunConfiguration config = context.getConfiguration();
    List<String> errors = new ArrayList<>();
    List<String> warnings = new ArrayList<>();
    size(context, "driver memory", config.getDriverMemory(), errors);
    size(context, "executor memory", config.getExecutorMemory(), errors);
    positiveNumber(context, "executor cores", config.getExecutorCores(), errors);
    positiveNumber(context, "source partitions", config.getSourcePartitions(), errors);
    sparkSettings(context, config.getSparkSettings(), errors, warnings);
    pathSchemes(config.getPathSchemes(), errors);
    if (!errors.isEmpty()) {
      return error(
          String.join(" ", errors), "Correct these values: Spark refuses them when it starts.");
    }
    if (!warnings.isEmpty()) {
      return warning(
          String.join(" ", warnings),
          "Spark only reads keys that start with 'spark.' (Hadoop settings go in as"
              + " 'spark.hadoop.<key>'); a key set twice keeps the value of its last row.");
    }
    return ok("Every setting can be read.");
  }

  private static void size(
      SparkCheckContext context, String name, String value, List<String> errors) {
    String resolved = context.resolve(value);
    if (!resolved.isEmpty() && !SIZE.matcher(resolved).matches()) {
      errors.add("The " + name + " '" + resolved + "' is not a size such as 512m or 2g.");
    }
  }

  private static void positiveNumber(
      SparkCheckContext context, String name, String value, List<String> errors) {
    String resolved = context.resolve(value);
    if (resolved.isEmpty()) {
      return;
    }
    try {
      if (Integer.parseInt(resolved) < 1) {
        errors.add("The " + name + " must be 1 or more, not " + resolved + ".");
      }
    } catch (NumberFormatException e) {
      errors.add("The " + name + " '" + resolved + "' is not a whole number.");
    }
  }

  private static void sparkSettings(
      SparkCheckContext context,
      List<SparkSetting> settings,
      List<String> errors,
      List<String> warnings) {
    Set<String> seen = new HashSet<>();
    for (int row = 0; row < settings.size(); row++) {
      SparkSetting setting = settings.get(row);
      String key = context.resolve(setting.getKey());
      if (key.isEmpty()) {
        if (!context.resolve(setting.getValue()).isEmpty()) {
          errors.add("Spark settings row " + (row + 1) + " has a value but no key.");
        }
        continue;
      }
      if (!seen.add(key)) {
        warnings.add("The Spark setting '" + key + "' is set twice: the last row wins.");
      }
      if (!key.startsWith("spark.")) {
        warnings.add("Spark ignores the Spark setting '" + key + "'.");
      }
    }
  }

  private static void pathSchemes(List<PathSchemeMapping> mappings, List<String> errors) {
    for (int row = 0; row < mappings.size(); row++) {
      PathSchemeMapping mapping = mappings.get(row);
      boolean from = mapping.getFrom() != null && !mapping.getFrom().isBlank();
      boolean to = mapping.getTo() != null && !mapping.getTo().isBlank();
      if (from != to) {
        errors.add("Path scheme map row " + (row + 1) + " needs both a from and a to scheme.");
      }
    }
  }
}
