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

package org.apache.hop.projects.resources;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.IAttributes;

/**
 * Attribute group/key constants for system resource requirements stored via {@link IAttributes} on
 * lifecycle environments. Group name is {@link #GROUP}.
 */
public final class ResourceAttributes {

  /** IAttributes group for system resource settings. */
  public static final String GROUP = "resources";

  /**
   * When environment is enabled: {@link #ON_ENABLE_OFF}, {@link #ON_ENABLE_WARN}, {@link
   * #ON_ENABLE_ENFORCE}.
   */
  public static final String KEY_ON_ENABLE = "onEnable";

  /** Minimum JVM max heap ({@link Runtime#maxMemory()}, ≈ {@code -Xmx}) in mebibytes. */
  public static final String KEY_MIN_MAX_MEMORY_MB = "minMaxMemoryMb";

  /** Minimum {@link Runtime#availableProcessors()}. */
  public static final String KEY_MIN_PROCESSORS = "minProcessors";

  /**
   * Disk free-space rules: newline-separated {@code path|minFreeMb} records. Paths and min-free
   * expressions may contain Hop variables; resolve with {@code variables.resolve} and {@link
   * org.apache.hop.core.Const#toLongExpanded(String, long)} before checking.
   */
  public static final String KEY_DISK_CHECKS = "diskChecks";

  public static final String ON_ENABLE_OFF = "off";
  public static final String ON_ENABLE_WARN = "warn";
  public static final String ON_ENABLE_ENFORCE = "enforce";

  /** Bytes in one mebibyte (1024 * 1024). */
  public static final long BYTES_PER_MIB = 1024L * 1024L;

  private ResourceAttributes() {}

  /**
   * Resolve on-enable policy from attributes, falling back to purpose-based defaults when unset.
   *
   * <ul>
   *   <li>Production → enforce
   *   <li>Testing / Acceptance → warn
   *   <li>Development / CI / CB / other → off
   * </ul>
   */
  // False positive: StringUtils.isNotBlank() is false for null, so 'explicit' can't be null here
  @SuppressWarnings("javabugs:S2259")
  public static String resolveOnEnable(IAttributes attributes, String purpose) {
    String explicit = attributes != null ? attributes.getAttribute(GROUP, KEY_ON_ENABLE) : null;
    if (StringUtils.isNotBlank(explicit)) {
      return explicit.trim().toLowerCase();
    }
    return defaultOnEnableForPurpose(purpose);
  }

  public static String defaultOnEnableForPurpose(String purpose) {
    if (StringUtils.isBlank(purpose)) {
      return ON_ENABLE_OFF;
    }
    String p = purpose.trim().toLowerCase();
    if (p.contains("production") || p.contains("prod")) {
      return ON_ENABLE_ENFORCE;
    }
    if (p.contains("test") || p.contains("accept")) {
      return ON_ENABLE_WARN;
    }
    return ON_ENABLE_OFF;
  }

  /**
   * Build a requirement object from attributes. Blank keys mean that dimension is not checked.
   * Returns a requirement with no thresholds when attributes are empty.
   */
  public static SystemResourceRequirement toRequirement(IAttributes attributes) {
    Long minMaxMemoryMb = parsePositiveLong(attribute(attributes, KEY_MIN_MAX_MEMORY_MB));
    Integer minProcessors = parsePositiveInt(attribute(attributes, KEY_MIN_PROCESSORS));
    List<DiskSpaceRequirement> diskChecks = parseDiskChecks(attribute(attributes, KEY_DISK_CHECKS));
    return new SystemResourceRequirement(minMaxMemoryMb, minProcessors, diskChecks);
  }

  public static String formatDiskChecks(List<DiskSpaceRequirement> diskChecks) {
    if (diskChecks == null || diskChecks.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (DiskSpaceRequirement req : diskChecks) {
      if (req == null
          || StringUtils.isBlank(req.getPath())
          || StringUtils.isBlank(req.getMinFreeBytes())) {
        continue;
      }
      if (!sb.isEmpty()) {
        sb.append('\n');
      }
      sb.append(req.getPath().trim()).append('|').append(req.getMinFreeBytes().trim());
    }
    return sb.toString();
  }

  /**
   * Parse {@link #KEY_DISK_CHECKS} value: newline-separated {@code path|minFreeMb} lines. Blank
   * lines ignored. Lines without {@code |} are skipped. The min-free side is kept as a string so
   * Hop variables and expanded number forms are preserved until check time.
   */
  public static List<DiskSpaceRequirement> parseDiskChecks(String raw) {
    List<DiskSpaceRequirement> result = new ArrayList<>();
    if (StringUtils.isBlank(raw)) {
      return result;
    }
    for (String line : raw.split("\\R")) {
      if (StringUtils.isBlank(line)) {
        continue;
      }
      String trimmed = line.trim();
      int sep = trimmed.lastIndexOf('|');
      if (sep <= 0 || sep >= trimmed.length() - 1) {
        continue;
      }
      String path = trimmed.substring(0, sep).trim();
      String mbText = trimmed.substring(sep + 1).trim();
      if (StringUtils.isBlank(path) || StringUtils.isBlank(mbText)) {
        continue;
      }
      result.add(new DiskSpaceRequirement(path, mbText));
    }
    return result;
  }

  private static String attribute(IAttributes attributes, String key) {
    return attributes != null ? attributes.getAttribute(GROUP, key) : null;
  }

  public static Long parsePositiveLong(String text) {
    if (StringUtils.isBlank(text)) {
      return null;
    }
    try {
      long value = Long.parseLong(text.trim());
      return value > 0 ? value : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Integer parsePositiveInt(String text) {
    if (StringUtils.isBlank(text)) {
      return null;
    }
    try {
      int value = Integer.parseInt(text.trim());
      return value > 0 ? value : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
