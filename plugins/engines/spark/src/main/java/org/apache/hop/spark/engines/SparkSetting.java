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

package org.apache.hop.spark.engines;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** One Spark setting of a native Spark run configuration: a row of the Spark settings table. */
@Getter
@Setter
public class SparkSetting {

  @HopMetadataProperty
  @GuiTableColumn(
      order = "10",
      type = GuiTableColumnType.COMBO,
      label = "i18n::SparkEngine.SparkSetting.Key.Label",
      toolTip = "i18n::SparkEngine.SparkSetting.Key.ToolTip",
      comboValuesMethod = "getSparkSettingKeys",
      width = 320)
  private String key;

  @HopMetadataProperty
  @GuiTableColumn(
      order = "20",
      type = GuiTableColumnType.TEXT,
      label = "i18n::SparkEngine.SparkSetting.Value.Label",
      toolTip = "i18n::SparkEngine.SparkSetting.Value.ToolTip",
      width = 420)
  private String value;

  public SparkSetting() {}

  public SparkSetting(String key, String value) {
    this.key = key;
    this.value = value;
  }

  /** The rows of {@code key=value} lines, as run configurations stored them before the table. */
  public static List<SparkSetting> parse(String lines) {
    List<SparkSetting> settings = new ArrayList<>();
    if (lines == null) {
      return settings;
    }
    for (String line : lines.split("\\r?\\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      int eq = trimmed.indexOf('=');
      settings.add(
          eq < 0
              ? new SparkSetting(trimmed, "")
              : new SparkSetting(
                  trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim()));
    }
    return settings;
  }

  /** The rows as {@code key=value} lines; rows without a key are left out. */
  public static String toLines(List<SparkSetting> settings) {
    StringBuilder lines = new StringBuilder();
    if (settings != null) {
      for (SparkSetting setting : settings) {
        if (setting.getKey() == null || setting.getKey().isBlank()) {
          continue;
        }
        if (!lines.isEmpty()) {
          lines.append('\n');
        }
        lines
            .append(setting.getKey().trim())
            .append('=')
            .append(setting.getValue() == null ? "" : setting.getValue().trim());
      }
    }
    return lines.toString();
  }

  public SparkSetting copy() {
    return new SparkSetting(key, value);
  }

  /**
   * Settings Hop sets itself whose value is a list. A row of the Spark settings adds to Hop's
   * entries instead of replacing them: the executors still need the Hop fat jar next to a JDBC
   * driver jar of a row. A Spark worker splits a class path with its own path separator, ':' on the
   * Linux hosts Spark clusters run on.
   */
  static final Map<String, String> LIST_SEPARATORS =
      Map.of("spark.jars", ",", "spark.executor.extraClassPath", ":");

  /**
   * The value of a setting that Hop and a row both set: for a list, Hop's entries followed by the
   * row's new ones; else the row's value.
   */
  public static String merge(String key, String hopValue, String rowValue) {
    String separator = LIST_SEPARATORS.get(key);
    if (separator == null || hopValue == null || hopValue.isBlank()) {
      return rowValue;
    }
    if (rowValue == null || rowValue.isBlank()) {
      return hopValue;
    }
    Set<String> entries = new LinkedHashSet<>(entries(hopValue, separator));
    entries.addAll(entries(rowValue, separator));
    return String.join(separator, entries);
  }

  /**
   * A list setting without one of its entries.
   *
   * @return what is left, or null when nothing is
   */
  public static String without(String key, String value, String entry) {
    String separator = LIST_SEPARATORS.getOrDefault(key, ",");
    if (value == null) {
      return null;
    }
    List<String> left = new ArrayList<>(entries(value, separator));
    left.remove(entry == null ? null : entry.trim());
    return left.isEmpty() ? null : String.join(separator, left);
  }

  private static List<String> entries(String value, String separator) {
    List<String> entries = new ArrayList<>();
    for (String entry : value.split(Pattern.quote(separator))) {
      if (!entry.isBlank()) {
        entries.add(entry.trim());
      }
    }
    return entries;
  }
}
