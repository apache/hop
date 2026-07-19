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

package org.apache.hop.projects.environment;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Suggests default environment names from a project name and purpose. Used when creating a new
 * environment so the name field can be pre-filled and updated as the user picks a purpose.
 */
public final class LifecycleEnvironmentNaming {

  /** Fixed English technical suffixes for standard purposes. */
  public static final String SUFFIX_DEVELOPMENT = "development";

  public static final String SUFFIX_TEST = "test";
  public static final String SUFFIX_ACCEPTANCE = "acceptance";
  public static final String SUFFIX_PRODUCTION = "production";
  public static final String SUFFIX_CONTINUOUS_INTEGRATION = "continuous-integration";
  public static final String SUFFIX_COMMON_BUILD = "common-build";

  private LifecycleEnvironmentNaming() {
    // utility
  }

  /**
   * Builds a map from known purpose display labels (localized) to fixed English suffixes.
   *
   * @param developmentLabel localized "Development" (or equivalent)
   * @param testingLabel localized "Testing"
   * @param acceptanceLabel localized "Acceptance"
   * @param productionLabel localized "Production"
   * @param continuousIntegrationLabel localized "Continuous Integration"
   * @param commonBuildLabel localized "Common Build"
   * @return unmodifiable map of label → suffix
   */
  public static Map<String, String> knownPurposeSuffixes(
      String developmentLabel,
      String testingLabel,
      String acceptanceLabel,
      String productionLabel,
      String continuousIntegrationLabel,
      String commonBuildLabel) {
    Map<String, String> map = new LinkedHashMap<>();
    putIfNotBlank(map, developmentLabel, SUFFIX_DEVELOPMENT);
    putIfNotBlank(map, testingLabel, SUFFIX_TEST);
    putIfNotBlank(map, acceptanceLabel, SUFFIX_ACCEPTANCE);
    putIfNotBlank(map, productionLabel, SUFFIX_PRODUCTION);
    putIfNotBlank(map, continuousIntegrationLabel, SUFFIX_CONTINUOUS_INTEGRATION);
    putIfNotBlank(map, commonBuildLabel, SUFFIX_COMMON_BUILD);
    return Collections.unmodifiableMap(map);
  }

  private static void putIfNotBlank(Map<String, String> map, String label, String suffix) {
    if (StringUtils.isNotBlank(label)) {
      map.put(label, suffix);
    }
  }

  /**
   * Maps a purpose string to a technical suffix for use in an environment name.
   *
   * <p>Known purpose labels (from {@code knownPurposeLabelsToSuffix}) map to fixed English
   * suffixes. Free-text purposes are sanitized to a lowercase slug.
   *
   * @param purpose purpose combo text (may be localized label or free text)
   * @param knownPurposeLabelsToSuffix map of known display labels → fixed suffixes
   * @return suffix without leading dash, or empty string if purpose is blank
   */
  public static String purposeToSuffix(
      String purpose, Map<String, String> knownPurposeLabelsToSuffix) {
    if (StringUtils.isBlank(purpose)) {
      return "";
    }
    String trimmed = purpose.trim();
    if (knownPurposeLabelsToSuffix != null) {
      String known = knownPurposeLabelsToSuffix.get(trimmed);
      if (known != null) {
        return known;
      }
    }
    return sanitizePurposeSuffix(trimmed);
  }

  /**
   * Suggests an environment name from project and purpose suffix.
   *
   * @param projectName project name (required for a non-empty suggestion)
   * @param purposeSuffix technical suffix without leading dash; empty means project only
   * @return suggested name, or empty string if project is blank
   */
  public static String suggestEnvironmentName(String projectName, String purposeSuffix) {
    if (StringUtils.isBlank(projectName)) {
      return "";
    }
    String project = projectName.trim();
    if (StringUtils.isBlank(purposeSuffix)) {
      return project;
    }
    return project + "-" + purposeSuffix.trim();
  }

  /**
   * Suggests an environment name from project and purpose text.
   *
   * @param projectName project name
   * @param purpose purpose combo text
   * @param knownPurposeLabelsToSuffix map of known display labels → fixed suffixes
   * @return suggested environment name
   */
  public static String suggestEnvironmentName(
      String projectName, String purpose, Map<String, String> knownPurposeLabelsToSuffix) {
    return suggestEnvironmentName(
        projectName, purposeToSuffix(purpose, knownPurposeLabelsToSuffix));
  }

  /**
   * Sanitizes free-text purpose into a stable technical suffix (lowercase, hyphenated).
   *
   * @param purpose free-text purpose
   * @return sanitized suffix, or empty if nothing usable remains
   */
  public static String sanitizePurposeSuffix(String purpose) {
    if (StringUtils.isBlank(purpose)) {
      return "";
    }
    String sanitized =
        purpose
            .trim()
            .toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9._-]+", "-")
            .replaceAll("-+", "-");
    return StringUtils.strip(sanitized, "-");
  }
}
