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

package org.apache.hop.projects.environment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/** Checks and tidies embedded environment definitions before they are written. */
public final class EmbeddedEnvironmentValidator {

  private EmbeddedEnvironmentValidator() {}

  /**
   * Trim names and drop variables that have no name. A blank description or default is stored as
   * null so it is left out of {@code project-config.json}.
   *
   * @param environment definition to tidy, ignored when null
   */
  public static void normalize(EmbeddedEnvironment environment) {
    if (environment == null) {
      return;
    }
    environment.absorbLegacyVariables();
    environment.setName(StringUtils.trimToNull(environment.getName()));
    environment.setDescription(StringUtils.trimToNull(environment.getDescription()));
    environment.setVariables(normalizeVariables(environment.getVariables()));
  }

  /**
   * @param environment definition to check
   * @return true when the name is missing
   */
  public static boolean missingName(EmbeddedEnvironment environment) {
    return environment == null || StringUtils.isBlank(environment.getName());
  }

  /**
   * @param environment definition whose variables are checked
   * @return the first variable name that appears more than once, or null
   */
  public static String duplicateVariableName(EmbeddedEnvironment environment) {
    if (environment == null || environment.getVariables() == null) {
      return null;
    }
    Set<String> seen = new HashSet<>();
    for (EmbeddedEnvironmentVariable variable : environment.getVariables()) {
      if (variable == null || StringUtils.isBlank(variable.getName())) {
        continue;
      }
      if (!seen.add(variable.getName().trim())) {
        return variable.getName().trim();
      }
    }
    return null;
  }

  /**
   * @param environments definitions in one project
   * @return the first environment name that appears more than once, or null. Blank names are
   *     ignored.
   */
  public static String duplicateEnvironmentName(List<EmbeddedEnvironment> environments) {
    if (environments == null) {
      return null;
    }
    Set<String> seen = new HashSet<>();
    for (EmbeddedEnvironment environment : environments) {
      if (environment == null || StringUtils.isBlank(environment.getName())) {
        continue;
      }
      String name = environment.getName().trim();
      if (!seen.add(name)) {
        return name;
      }
    }
    return null;
  }

  /**
   * @param name candidate environment name
   * @param otherNames names already used by the other definitions in the project
   * @return true when {@code name} matches one of {@code otherNames}
   */
  public static boolean nameTaken(String name, List<String> otherNames) {
    if (StringUtils.isBlank(name) || otherNames == null) {
      return false;
    }
    String trimmed = name.trim();
    for (String other : otherNames) {
      if (trimmed.equals(StringUtils.trimToEmpty(other))) {
        return true;
      }
    }
    return false;
  }

  private static List<EmbeddedEnvironmentVariable> normalizeVariables(
      List<EmbeddedEnvironmentVariable> source) {
    List<EmbeddedEnvironmentVariable> normalized = new ArrayList<>();
    if (source == null) {
      return normalized;
    }
    for (EmbeddedEnvironmentVariable variable : source) {
      if (variable == null || StringUtils.isBlank(variable.getName())) {
        continue;
      }
      variable.setName(variable.getName().trim());
      variable.setDefaultValue(StringUtils.trimToNull(variable.getDefaultValue()));
      variable.setDescription(StringUtils.trimToNull(variable.getDescription()));
      normalized.add(variable);
    }
    return normalized;
  }
}
