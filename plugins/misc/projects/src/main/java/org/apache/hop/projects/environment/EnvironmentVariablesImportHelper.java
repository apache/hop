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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchQuery;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.SearchQuery;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.search.ProjectsSearchablesLocation;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.hopgui.search.HopGuiSearchHelper;

/**
 * Finds {@code ${VARIABLE}} expressions used in a project's pipelines, workflows and metadata via
 * the project search infrastructure, and prepares described variables that are not yet defined in
 * environment configuration files.
 */
public final class EnvironmentVariablesImportHelper {

  /**
   * Regex used with {@link SearchQuery} (regex mode) to find property values that contain a Unix
   * variable expression. Partial match via {@link java.util.regex.Matcher#find()}.
   */
  public static final String VARIABLE_EXPRESSION_REGEX = "\\$\\{[^}]+\\}";

  /** Extracts the variable name from each {@code ${…}} occurrence in a string. */
  public static final Pattern VARIABLE_NAME_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

  /** JVM temporary directory system property often referenced as {@code ${java.io.tmpdir}}. */
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

  /** Max distinct search locations kept in a variable description. */
  private static final int MAX_DESCRIPTION_LOCATIONS = 10;

  private static final Set<String> MANAGED_VARIABLE_NAMES;

  static {
    Set<String> managed = new HashSet<>();
    managed.add(ProjectsUtil.VARIABLE_PROJECT_HOME);
    managed.add(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME);
    managed.add(ProjectsUtil.VARIABLE_PARENT_PROJECT_NAME);
    managed.add(ProjectsUtil.VARIABLE_HOP_DATASETS_FOLDER);
    managed.add(ProjectsUtil.VARIABLE_HOP_UNIT_TESTS_FOLDER);
    managed.add(Defaults.VARIABLE_HOP_PROJECT_NAME);
    managed.add(Defaults.VARIABLE_HOP_ENVIRONMENT_NAME);
    managed.add(Const.HOP_METADATA_FOLDER);
    managed.add(JAVA_IO_TMPDIR);
    MANAGED_VARIABLE_NAMES = Collections.unmodifiableSet(managed);
  }

  private EnvironmentVariablesImportHelper() {
    // Utility class
  }

  /**
   * Extract distinct variable names from {@code ${NAME}} expressions in the given text. Only Unix
   * style is considered; {@code %%NAME%%} and {@code #{…}} are ignored.
   *
   * @param text text that may contain variable expressions (may be null)
   * @return ordered set of variable names (insertion order of first occurrence)
   */
  public static Set<String> extractVariableNames(String text) {
    Set<String> names = new LinkedHashSet<>();
    if (StringUtils.isEmpty(text)) {
      return names;
    }
    Matcher matcher = VARIABLE_NAME_PATTERN.matcher(text);
    while (matcher.find()) {
      String name = matcher.group(1);
      if (StringUtils.isNotBlank(name)) {
        names.add(name.trim());
      }
    }
    return names;
  }

  /**
   * Whether the variable is managed by Hop / the project runtime and should not be proposed for an
   * environment configuration file.
   *
   * @param name variable name
   * @return true if the name should be skipped
   */
  public static boolean isManagedVariable(String name) {
    if (StringUtils.isBlank(name)) {
      return true;
    }
    if (name.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
      return true;
    }
    return MANAGED_VARIABLE_NAMES.contains(name);
  }

  /**
   * Load variable names already defined in the given environment configuration files.
   *
   * @param configurationFiles paths (may contain variables) listed on the environment
   * @param variables variables used to resolve paths
   * @return set of configured variable names
   * @throws HopException if a config file exists but cannot be read
   */
  public static Set<String> loadConfiguredVariableNames(
      List<String> configurationFiles, IVariables variables) throws HopException {
    Set<String> names = new HashSet<>();
    if (configurationFiles == null) {
      return names;
    }
    for (String configurationFile : configurationFiles) {
      if (StringUtils.isBlank(configurationFile)) {
        continue;
      }
      String realFilename = variables.resolve(configurationFile);
      try {
        if (!HopVfs.fileExists(realFilename)) {
          continue;
        }
        DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(realFilename);
        configFile.readFromFile();
        for (DescribedVariable describedVariable : configFile.getDescribedVariables()) {
          if (describedVariable != null && StringUtils.isNotBlank(describedVariable.getName())) {
            names.add(describedVariable.getName());
          }
        }
      } catch (Exception e) {
        throw new HopException(
            "Error reading variables from environment configuration file '" + realFilename + "'",
            e);
      }
    }
    return names;
  }

  /**
   * Crawl the project associated with the environment using project search and a regex for {@code
   * ${…}}, then return described variables not already present in the environment configuration
   * files.
   *
   * @param projectConfig project linked to the lifecycle environment
   * @param variables parent variable space (typically HopGui variables)
   * @param configurationFiles environment configuration file paths (as currently listed in the
   *     dialog)
   * @param environmentName optional environment name for project variable setup
   * @return sorted list of proposed described variables (empty values when unset)
   * @throws HopException if the project cannot be loaded or search fails
   */
  public static List<DescribedVariable> findMissingVariables(
      ProjectConfig projectConfig,
      IVariables variables,
      List<String> configurationFiles,
      String environmentName)
      throws HopException {
    if (projectConfig == null) {
      throw new HopException(
          "Please select a project for this environment before importing variables.");
    }

    IVariables scanVariables = new Variables();
    scanVariables.initializeFrom(variables);

    Project project = projectConfig.loadProject(scanVariables);
    List<String> configFiles =
        configurationFiles != null ? configurationFiles : Collections.emptyList();
    project.modifyVariables(scanVariables, projectConfig, configFiles, environmentName);

    IHopMetadataProvider metadataProvider =
        HopMetadataUtil.getStandardHopMetadataProvider(scanVariables);

    ProjectsSearchablesLocation location = new ProjectsSearchablesLocation(projectConfig);
    ISearchQuery query = new SearchQuery(VARIABLE_EXPRESSION_REGEX, true, true);

    @SuppressWarnings("rawtypes")
    Map<Class<ISearchableAnalyser>, ISearchableAnalyser> analysers =
        HopGuiSearchHelper.loadSearchableAnalysers();
    List<ISearchResult> results =
        HopGuiSearchHelper.searchLocation(
            location, query, analysers, metadataProvider, scanVariables);

    // Preserve a stable order of names while collecting where each was found.
    Map<String, Set<String>> locationsByName = new LinkedHashMap<>();
    Map<String, String> canonicalNameByKey = new LinkedHashMap<>();
    for (ISearchResult result : results) {
      if (result == null) {
        continue;
      }
      // Prefer the full property value so multiple ${…} in one field are all captured.
      String text = Const.NVL(result.getValue(), result.getMatchingString());
      Set<String> namesInResult = extractVariableNames(text);
      if (namesInResult.isEmpty()) {
        continue;
      }
      String foundAt = formatFoundLocation(result);
      for (String name : namesInResult) {
        if (isManagedVariable(name)) {
          continue;
        }
        String key = name.toLowerCase(Locale.ROOT);
        canonicalNameByKey.putIfAbsent(key, name);
        if (StringUtils.isNotEmpty(foundAt)) {
          locationsByName.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(foundAt);
        } else {
          locationsByName.computeIfAbsent(key, k -> new LinkedHashSet<>());
        }
      }
    }

    Set<String> configured = loadConfiguredVariableNames(configFiles, scanVariables);

    // Sorted presentation order
    Set<String> sortedKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    sortedKeys.addAll(canonicalNameByKey.keySet());

    List<DescribedVariable> proposed = new ArrayList<>();
    for (String key : sortedKeys) {
      String name = canonicalNameByKey.get(key);
      if (containsIgnoreCase(configured, name)) {
        continue;
      }
      String value = Const.NVL(scanVariables.getVariable(name), "");
      String description = formatLocationsDescription(locationsByName.get(key));
      proposed.add(new DescribedVariable(name, value, description));
    }
    return proposed;
  }

  /**
   * Build a short human-readable location for a search hit (type, object name, field/component).
   *
   * @param result a project-search match
   * @return e.g. {@code Pipeline 'load-data' → Table input → filename}, or empty when unknown
   */
  public static String formatFoundLocation(ISearchResult result) {
    if (result == null || result.getMatchingSearchable() == null) {
      return "";
    }
    ISearchable<?> searchable = result.getMatchingSearchable();
    StringBuilder sb = new StringBuilder();
    String type = Const.NVL(searchable.getType(), "");
    String name = Const.NVL(searchable.getName(), "");
    if (StringUtils.isNotEmpty(type) && StringUtils.isNotEmpty(name)) {
      sb.append(type).append(" '").append(name).append("'");
    } else if (StringUtils.isNotEmpty(name)) {
      sb.append(name);
    } else if (StringUtils.isNotEmpty(type)) {
      sb.append(type);
    }

    String where = HopGuiSearchHelper.matchWhere(result);
    if (StringUtils.isNotEmpty(where)) {
      if (sb.length() > 0) {
        sb.append(" → ");
      }
      sb.append(where);
    }
    return sb.toString();
  }

  /**
   * Join unique found-at locations into a description string.
   *
   * @param locations distinct location strings (may be null/empty)
   * @return description text, never null
   */
  public static String formatLocationsDescription(Set<String> locations) {
    if (locations == null || locations.isEmpty()) {
      return "";
    }
    List<String> list = new ArrayList<>();
    for (String location : locations) {
      if (StringUtils.isNotEmpty(location)) {
        list.add(location);
      }
    }
    if (list.isEmpty()) {
      return "";
    }
    if (list.size() <= MAX_DESCRIPTION_LOCATIONS) {
      return String.join("; ", list);
    }
    List<String> head = list.subList(0, MAX_DESCRIPTION_LOCATIONS);
    int remaining = list.size() - MAX_DESCRIPTION_LOCATIONS;
    return String.join("; ", head) + "; (+" + remaining + " more)";
  }

  /**
   * Merge the given variables into a described-variables JSON configuration file (create or
   * update).
   *
   * @param configFilename resolved filesystem path
   * @param variables variables to add or update
   * @throws HopException on I/O errors
   */
  public static void saveVariablesToConfigFile(
      String configFilename, List<DescribedVariable> variables) throws HopException {
    if (StringUtils.isBlank(configFilename)) {
      throw new HopException("Configuration filename is empty");
    }
    try {
      DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(configFilename);
      if (HopVfs.fileExists(configFilename)) {
        configFile.readFromFile();
      }
      if (variables != null) {
        for (DescribedVariable variable : variables) {
          if (variable != null && StringUtils.isNotBlank(variable.getName())) {
            configFile.setDescribedVariable(variable);
          }
        }
      }
      configFile.saveToFile();
    } catch (Exception e) {
      throw new HopException(
          "Error saving variables to configuration file '" + configFilename + "'", e);
    }
  }

  private static boolean containsIgnoreCase(Set<String> names, String name) {
    if (names.contains(name)) {
      return true;
    }
    for (String existing : names) {
      if (existing != null && existing.equalsIgnoreCase(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return unmodifiable set of runtime/project managed variable names (for tests)
   */
  static Set<String> managedVariableNames() {
    return MANAGED_VARIABLE_NAMES;
  }

  /**
   * Suggest a default config filename for an environment.
   *
   * @param environmentName environment name
   * @return e.g. {@code dev-config.json}
   */
  public static String defaultConfigFilename(String environmentName) {
    if (StringUtils.isBlank(environmentName)) {
      return "environment-config.json";
    }
    String sanitized =
        environmentName
            .trim()
            .toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9._-]+", "-")
            .replaceAll("-+", "-");
    sanitized = StringUtils.strip(sanitized, "-");
    if (sanitized.isEmpty()) {
      return "environment-config.json";
    }
    return sanitized + "-config.json";
  }
}
