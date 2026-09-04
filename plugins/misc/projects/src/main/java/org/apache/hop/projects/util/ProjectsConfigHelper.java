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

package org.apache.hop.projects.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;

/**
 * Helper class for in-memory Hop configuration and dynamic project and environment registration.
 */
public class ProjectsConfigHelper {

  private static final String[] CONFIG_CANDIDATES = {
    ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME, "hop-project.config", "hop-config.json"
  };

  /**
   * Project names registered in this process via {@code --project-locations}. Used so a subcommand
   * mixin (for example hop-run) can enable a project that was registered on the root command.
   */
  private static final List<String> sessionRegisteredProjects = new CopyOnWriteArrayList<>();

  /** Private constructor to prevent instantiation. */
  private ProjectsConfigHelper() {}

  /**
   * Project names registered via {@code --project-locations} in this process.
   *
   * @return unmodifiable copy of the session list
   */
  public static List<String> getSessionRegisteredProjects() {
    return Collections.unmodifiableList(new ArrayList<>(sessionRegisteredProjects));
  }

  /** Clears the session list of dynamically registered projects (for tests). */
  public static void clearSessionRegisteredProjects() {
    sessionRegisteredProjects.clear();
  }

  private static void rememberRegisteredProject(String projectName) {
    if (StringUtils.isNotEmpty(projectName) && !sessionRegisteredProjects.contains(projectName)) {
      sessionRegisteredProjects.add(projectName);
    }
  }

  private static void logBasic(ILogChannel log, String message) {
    if (log != null && HopLogStore.isInitialized() && log.isBasic()) {
      log.logBasic(message);
    }
  }

  private static void logError(ILogChannel log, String message, Throwable e) {
    if (log != null && HopLogStore.isInitialized()) {
      log.logError(message, e);
    }
  }

  /** Activates in-memory mode on HopConfig to ensure that changes are never persisted to disk. */
  public static void enableInMemoryMode(ILogChannel log) {
    if (!HopConfig.isInMemoryMode()) {
      HopConfig.setInMemoryMode(true);
      logBasic(
          log, "Hop configuration is running in in-memory mode. No changes will be saved to disk.");
    }
  }

  /**
   * Normalizes a project location. If the location points to an archive file (.zip or .jar), it is
   * converted to a Commons VFS archive URI (e.g. zip:file:///path/to/archive.zip!/).
   */
  public static String normalizeProjectHome(String path, IVariables variables) {
    if (StringUtils.isEmpty(path)) {
      return path;
    }
    String resolved = (variables != null) ? variables.resolve(path).trim() : path.trim();
    if (ProjectConfig.isArchiveUri(resolved)) {
      return resolved;
    }

    String lower = resolved.toLowerCase(Locale.ROOT);
    if (lower.endsWith(".zip") || lower.endsWith(".jar")) {
      try {
        FileObject fileObject = HopVfs.getFileObject(resolved, variables);
        String uri = fileObject.getName().getURI();
        return "zip:" + uri + "!/";
      } catch (Exception e) {
        return "zip:file://" + resolved + "!/";
      }
    }

    if (resolved.contains(".zip!") || resolved.contains(".jar!")) {
      if (!resolved.startsWith("zip:") && !resolved.startsWith("jar:")) {
        return "zip:" + resolved;
      }
    }

    return resolved;
  }

  /**
   * Registers dynamic project locations in the in-memory ProjectsConfig.
   *
   * @param log logger
   * @param variables variables space
   * @param locations array of location strings (can be comma-separated or key=val)
   * @return list of registered project names
   * @throws HopException on error
   */
  public static List<String> addProjectLocations(
      ILogChannel log, IVariables variables, String[] locations) throws HopException {
    List<String> registeredNames = new ArrayList<>();
    if (locations == null || locations.length == 0) {
      return registeredNames;
    }

    enableInMemoryMode(log);
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    List<String> entries = new ArrayList<>();
    for (String locItem : locations) {
      if (StringUtils.isNotEmpty(locItem)) {
        for (String splitItem : splitRespectingQuotes(locItem, ',')) {
          if (StringUtils.isNotEmpty(splitItem)) {
            entries.add(splitItem.trim());
          }
        }
      }
    }

    for (String entry : entries) {
      int equalsIdx = entry.indexOf('=');
      if (equalsIdx > 0) {
        String projectName = entry.substring(0, equalsIdx).trim();
        String locPart = entry.substring(equalsIdx + 1).trim();
        String explicitConfigFile = null;

        // Check for optional :configFilename suffix (e.g. /path/project:hop-project.config)
        int colonIdx = locPart.lastIndexOf(':');
        if (colonIdx > 0 && colonIdx < locPart.length() - 1) {
          String suffix = locPart.substring(colonIdx + 1);
          if (suffix.endsWith(".json") || suffix.endsWith(".config")) {
            // Ensure this colon is not part of a URI scheme (like zip:file:///...)
            String prefix = locPart.substring(0, colonIdx);
            if (!prefix.endsWith("!/")) {
              explicitConfigFile = suffix;
              locPart = prefix;
            }
          }
        }

        String normalizedHome = normalizeProjectHome(locPart, variables);
        normalizedHome = resolveExportSubfolder(normalizedHome, projectName, variables);
        String configFile =
            (explicitConfigFile != null)
                ? explicitConfigFile
                : detectConfigFilename(normalizedHome, variables);

        ProjectConfig pc = new ProjectConfig(projectName, normalizedHome, configFile);
        if (ProjectConfig.isArchiveUri(normalizedHome)) {
          pc.setReadOnly(true);
        }

        config.addProjectConfig(pc);
        registeredNames.add(projectName);
        rememberRegisteredProject(projectName);

        logBasic(
            log,
            "Registered in-memory project '"
                + projectName
                + "' with home '"
                + normalizedHome
                + "' (config: '"
                + configFile
                + "', read-only: "
                + pc.isReadOnly()
                + ")");
      } else {
        // Standalone folder or zip archive without name= prefix
        String normalizedHome = normalizeProjectHome(entry, variables);
        List<String> discovered =
            discoverAndRegisterProjects(log, variables, config, normalizedHome);
        registeredNames.addAll(discovered);
      }
    }

    return registeredNames;
  }

  /**
   * If an archive contains a single subfolder matching projectName or containing the project
   * config, resolve projectHome to point inside that subfolder.
   */
  private static String resolveExportSubfolder(
      String homeUri, String projectName, IVariables variables) {
    if (!ProjectConfig.isArchiveUri(homeUri)) {
      return homeUri;
    }
    try {
      FileObject rootObj = HopVfs.getFileObject(homeUri, variables);
      if (rootObj.exists()) {
        // If root has candidate config file, keep root
        for (String candidate : CONFIG_CANDIDATES) {
          if (rootObj.resolveFile(candidate).exists()) {
            return homeUri;
          }
        }
        if (rootObj.resolveFile("metadata.json").exists()) {
          return homeUri;
        }

        // Check if there is a subfolder named after the project
        if (StringUtils.isNotEmpty(projectName)) {
          FileObject projectSubfolder = rootObj.resolveFile(projectName);
          if (projectSubfolder.exists() && projectSubfolder.getType() == FileType.FOLDER) {
            return projectSubfolder.getName().getURI();
          }
        }

        // Check if there is exactly one subfolder containing a project config
        FileObject[] children = rootObj.getChildren();
        if (children != null) {
          for (FileObject child : children) {
            if (child.getType() == FileType.FOLDER) {
              for (String candidate : CONFIG_CANDIDATES) {
                if (child.resolveFile(candidate).exists()) {
                  return child.getName().getURI();
                }
              }
              if (child.resolveFile("metadata.json").exists()) {
                return child.getName().getURI();
              }
            }
          }
        }
      }
    } catch (Exception e) {
      // Fallback to original URI
    }
    return homeUri;
  }

  /** Discovers and registers projects from a standalone folder or archive. */
  private static List<String> discoverAndRegisterProjects(
      ILogChannel log, IVariables variables, ProjectsConfig config, String homeUri)
      throws HopException {
    List<String> registered = new ArrayList<>();
    try {
      FileObject homeObj = HopVfs.getFileObject(homeUri, variables);
      if (!homeObj.exists()) {
        throw new HopException("Project location '" + homeUri + "' does not exist");
      }

      // Check subfolders for projects
      FileObject[] children = homeObj.getChildren();
      if (children != null) {
        for (FileObject child : children) {
          if (child.getType() == FileType.FOLDER) {
            String detectedConfig = detectConfigFilename(child.getName().getURI(), variables);
            if (detectedConfig != null || child.resolveFile("metadata.json").exists()) {
              String name = child.getName().getBaseName();
              String configFilename =
                  (detectedConfig != null)
                      ? detectedConfig
                      : ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME;
              ProjectConfig pc = new ProjectConfig(name, child.getName().getURI(), configFilename);
              if (ProjectConfig.isArchiveUri(homeUri)) {
                pc.setReadOnly(true);
              }
              config.addProjectConfig(pc);
              registered.add(name);
              rememberRegisteredProject(name);
              logBasic(
                  log,
                  "Discovered in-memory project '"
                      + name
                      + "' in '"
                      + child.getName().getURI()
                      + "'");
            }
          }
        }
      }

      // If no subfolder projects were discovered, check the root itself
      if (registered.isEmpty()) {
        String detectedConfig = detectConfigFilename(homeUri, variables);
        String name = homeObj.getName().getBaseName();
        if (name.endsWith("!/")) {
          name = name.substring(0, name.length() - 2);
        }
        if (name.contains(".")) {
          name = FilenameUtils.getBaseName(name);
        }
        if (StringUtils.isEmpty(name)) {
          name = "default";
        }
        String configFilename =
            (detectedConfig != null)
                ? detectedConfig
                : ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME;
        ProjectConfig pc = new ProjectConfig(name, homeUri, configFilename);
        if (ProjectConfig.isArchiveUri(homeUri)) {
          pc.setReadOnly(true);
        }
        config.addProjectConfig(pc);
        registered.add(name);
        rememberRegisteredProject(name);
        logBasic(
            log, "Registered standalone in-memory project '" + name + "' at '" + homeUri + "'");
      }
    } catch (Exception e) {
      throw new HopException("Error discovering projects in location '" + homeUri + "'", e);
    }
    return registered;
  }

  /** Detects the config filename within a project home directory. */
  public static String detectConfigFilename(String homeUri, IVariables variables) {
    try {
      FileObject homeObj = HopVfs.getFileObject(homeUri, variables);
      if (homeObj.exists()) {
        for (String candidate : CONFIG_CANDIDATES) {
          if (homeObj.resolveFile(candidate).exists()) {
            return candidate;
          }
        }
      }
    } catch (Exception e) {
      // Ignored
    }
    return ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME;
  }

  /**
   * Registers dynamic lifecycle environments in the in-memory ProjectsConfig. Format:
   * envName=[project:]file1[;file2...] or envName=file1
   */
  public static void addEnvironments(
      ILogChannel log, IVariables variables, String[] envDefs, List<String> registeredProjectNames)
      throws HopException {
    if (envDefs == null || envDefs.length == 0) {
      return;
    }

    enableInMemoryMode(log);
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    List<String> entries = new ArrayList<>();
    for (String envItem : envDefs) {
      if (StringUtils.isNotEmpty(envItem)) {
        for (String splitItem : splitRespectingQuotes(envItem, ',')) {
          if (StringUtils.isNotEmpty(splitItem)) {
            entries.add(splitItem.trim());
          }
        }
      }
    }

    for (String entry : entries) {
      int equalsIdx = entry.indexOf('=');
      if (equalsIdx <= 0) {
        continue;
      }
      String envName = entry.substring(0, equalsIdx).trim();
      String valPart = entry.substring(equalsIdx + 1).trim();

      String targetProject = null;
      String filesStr = valPart;

      // Check if project name is specified before a colon: e.g. edw:/path/conf.json
      // Be careful of Windows drive letters (e.g. C:/...)
      int colonIdx = valPart.indexOf(':');
      if (colonIdx > 0 && colonIdx < valPart.length() - 1) {
        boolean isDriveLetter =
            (colonIdx == 1)
                && Character.isLetter(valPart.charAt(0))
                && (valPart.charAt(2) == '/' || valPart.charAt(2) == '\\');
        if (!isDriveLetter && !valPart.startsWith("file:") && !valPart.startsWith("zip:")) {
          targetProject = valPart.substring(0, colonIdx).trim();
          filesStr = valPart.substring(colonIdx + 1).trim();
        }
      }

      // If target project not explicitly given, infer from envName or registered projects
      if (StringUtils.isEmpty(targetProject)) {
        targetProject = inferTargetProject(envName, registeredProjectNames, config, variables);
      }

      List<String> configFiles = new ArrayList<>();
      for (String file : filesStr.split("[;,]")) {
        String trimmed = file.trim();
        if (StringUtils.isNotEmpty(trimmed)) {
          configFiles.add(trimmed);
        }
      }

      LifecycleEnvironment env =
          new LifecycleEnvironment(envName, "In-memory", targetProject, configFiles);
      config.addEnvironment(env);

      logBasic(
          log,
          "Registered in-memory lifecycle environment '"
              + envName
              + "' for project '"
              + targetProject
              + "' with configuration files: "
              + configFiles);
    }
  }

  /** Infers which project an environment belongs to. */
  private static String inferTargetProject(
      String envName,
      List<String> registeredProjectNames,
      ProjectsConfig config,
      IVariables variables) {
    // 1. Check prefix match with registered projects: e.g. "edw-prod" starts with "edw"
    if (registeredProjectNames != null) {
      for (String proj : registeredProjectNames) {
        if (envName.equalsIgnoreCase(proj)
            || envName.toLowerCase(Locale.ROOT).startsWith(proj.toLowerCase(Locale.ROOT) + "-")
            || envName.toLowerCase(Locale.ROOT).startsWith(proj.toLowerCase(Locale.ROOT) + "_")
            || envName.toLowerCase(Locale.ROOT).startsWith(proj.toLowerCase(Locale.ROOT) + ".")) {
          return proj;
        }
      }
    }

    // 2. Check leaf project among registered projects (a project that is child and not parent)
    if (registeredProjectNames != null && !registeredProjectNames.isEmpty()) {
      String leaf = findLeafProject(registeredProjectNames, config, variables);
      if (leaf != null) {
        return leaf;
      }
      return registeredProjectNames.get(registeredProjectNames.size() - 1);
    }

    // 3. Fallback to first configured project
    if (!config.getProjectConfigurations().isEmpty()) {
      return config.getProjectConfigurations().get(0).getProjectName();
    }

    return "default";
  }

  /** Finds the leaf project in a hierarchy (e.g. child referencing a parent). */
  public static String findLeafProject(
      List<String> projectNames, ProjectsConfig config, IVariables variables) {
    if (projectNames == null || projectNames.isEmpty()) {
      return null;
    }
    if (projectNames.size() == 1) {
      return projectNames.get(0);
    }

    Set<String> parents = new HashSet<>();
    for (String name : projectNames) {
      ProjectConfig pc = config.findProjectConfig(name);
      if (pc != null) {
        try {
          Project p = pc.loadProject(variables);
          if (p != null && StringUtils.isNotEmpty(p.getParentProjectName())) {
            parents.add(p.getParentProjectName());
          }
        } catch (Exception e) {
          // Ignored
        }
      }
    }

    // A leaf project is one that is NOT a parent of any other project
    for (int i = projectNames.size() - 1; i >= 0; i--) {
      String name = projectNames.get(i);
      if (!parents.contains(name)) {
        return name;
      }
    }

    return projectNames.get(projectNames.size() - 1);
  }

  /** Determines the active project if not explicitly supplied via CLI. */
  public static String determineActiveProject(
      String projectName,
      String environmentName,
      List<String> registeredProjectNames,
      IVariables variables) {
    if (StringUtils.isNotEmpty(projectName)) {
      return projectName;
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    if (StringUtils.isEmpty(environmentName) && variables != null) {
      environmentName = variables.getVariable(Defaults.VARIABLE_HOP_ENVIRONMENT_NAME);
    }
    if (StringUtils.isNotEmpty(environmentName)) {
      LifecycleEnvironment env = config.findEnvironment(environmentName);
      if (env != null && StringUtils.isNotEmpty(env.getProjectName())) {
        return env.getProjectName();
      }
    }

    List<String> candidates = registeredProjectNames;
    if ((candidates == null || candidates.isEmpty()) && !sessionRegisteredProjects.isEmpty()) {
      candidates = sessionRegisteredProjects;
    }
    if (candidates != null && !candidates.isEmpty()) {
      return findLeafProject(candidates, config, variables);
    }

    if (variables != null) {
      String activeProject = variables.getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME);
      if (StringUtils.isNotEmpty(activeProject)
          && config.findProjectConfig(activeProject) != null) {
        return activeProject;
      }
    }

    return null;
  }

  /**
   * Loads metadata and variables from project export files (metadata.json, variables.json) if
   * present in the project home.
   */
  public static void applyProjectExportFiles(
      ILogChannel log,
      String projectHome,
      IVariables variables,
      MultiMetadataProvider metadataProvider) {
    if (StringUtils.isEmpty(projectHome)) {
      return;
    }
    try {
      String realProjectHome = (variables != null) ? variables.resolve(projectHome) : projectHome;
      FileObject projectHomeObj = HopVfs.getFileObject(realProjectHome, variables);
      if (!projectHomeObj.exists()) {
        return;
      }

      // Check for exported metadata.json
      FileObject metadataJsonObj = projectHomeObj.resolveFile("metadata.json");
      if (metadataJsonObj.exists() && metadataJsonObj.isFile()) {
        try (InputStream in = HopVfs.getInputStream(metadataJsonObj)) {
          String json = IOUtils.toString(in, StandardCharsets.UTF_8);
          SerializableMetadataProvider exportedProvider = new SerializableMetadataProvider(json);
          if (metadataProvider != null) {
            metadataProvider.getProviders().add(exportedProvider);
          }
          logBasic(log, "Loaded exported metadata from: " + metadataJsonObj.getName().getURI());
        }
      }

      // Check for exported variables.json
      FileObject variablesJsonObj = projectHomeObj.resolveFile("variables.json");
      if (variablesJsonObj.exists() && variablesJsonObj.isFile() && variables != null) {
        try (InputStream in = HopVfs.getInputStream(variablesJsonObj)) {
          ObjectMapper mapper = new ObjectMapper();
          Map<String, String> varMap =
              mapper.readValue(in, new TypeReference<Map<String, String>>() {});
          for (Map.Entry<String, String> entry : varMap.entrySet()) {
            variables.setVariable(entry.getKey(), entry.getValue());
          }
          logBasic(log, "Loaded exported variables from: " + variablesJsonObj.getName().getURI());
        }
      }
    } catch (Exception e) {
      logError(log, "Error applying project export files from home: " + projectHome, e);
    }
  }

  /** Splits a string by delimiter, respecting single or double quotes. */
  private static List<String> splitRespectingQuotes(String str, char delimiter) {
    List<String> tokens = new ArrayList<>();
    if (str == null) {
      return tokens;
    }
    StringBuilder current = new StringBuilder();
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;

    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == '\'' && !inDoubleQuote) {
        inSingleQuote = !inSingleQuote;
      } else if (c == '"' && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote;
      } else if (c == delimiter && !inSingleQuote && !inDoubleQuote) {
        tokens.add(current.toString().trim());
        current.setLength(0);
        continue;
      }
      current.append(c);
    }
    if (current.length() > 0) {
      tokens.add(current.toString().trim());
    }
    return tokens;
  }
}
