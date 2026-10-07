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
import java.util.List;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.project.Project;

/**
 * Creates local lifecycle environments from the embedded definitions in a project.
 *
 * <p>The configuration file for an environment receives mandatory variables and secrets only.
 * Optional variables stay in the project and are applied when the environment is enabled.
 */
public final class EmbeddedEnvironmentMaterializer {

  /** Project variable that points at the folder for local environment configuration files. */
  public static final String VARIABLE_ENVIRONMENTS_FOLDER = "ENVIRONMENTS_FOLDER";

  private EmbeddedEnvironmentMaterializer() {}

  /**
   * Resolve {@link #VARIABLE_ENVIRONMENTS_FOLDER}. A missing or blank value is left unresolved.
   *
   * @param variables variable space that already contains the project variables
   * @return the folder, or null when the variable is missing or blank
   */
  public static String resolveFolder(IVariables variables) {
    if (variables == null) {
      return null;
    }
    String raw = variables.getVariable(VARIABLE_ENVIRONMENTS_FOLDER);
    if (StringUtils.isBlank(raw)) {
      return null;
    }
    return StringUtils.trimToNull(variables.resolve(raw));
  }

  /**
   * @param projectName project the folder is for
   * @return an unresolved suggestion outside the project, using {@code ${HOP_CONFIG_FOLDER}}
   */
  public static String suggestedConfigFolder(String projectName) {
    String safe = configFileName(StringUtils.defaultIfBlank(projectName, "project"));
    safe = safe.substring(0, safe.length() - ".json".length());
    return "${HOP_CONFIG_FOLDER}/environments/" + safe;
  }

  /**
   * @param environmentName environment name
   * @return a single path segment ending in {@code .json}
   */
  public static String configFileName(String environmentName) {
    String sanitized = StringUtils.defaultString(environmentName).trim();
    sanitized = sanitized.replaceAll("[\\\\/]+", "_");
    sanitized = sanitized.replaceAll("[^A-Za-z0-9._-]", "_");
    // "../dev" is ".._dev" after the slash replacement. Collapse that whole prefix.
    sanitized = sanitized.replaceAll("^[._]+", "_");
    if (StringUtils.isBlank(sanitized.replace("_", ""))) {
      sanitized = "environment";
    }
    return sanitized + ".json";
  }

  /**
   * @param folder folder that will hold the file
   * @param environmentName environment name
   * @return path of {@code <folder>/<name>.json}
   * @throws HopException when the folder cannot be resolved
   */
  public static String configFilePath(String folder, String environmentName) throws HopException {
    String fileName = configFileName(environmentName);
    try (FileObject parent = HopVfs.getFileObject(folder)) {
      FileObject file = parent.resolveFile(fileName);
      String scheme = file.getName().getScheme();
      if (scheme == null || "file".equalsIgnoreCase(scheme)) {
        return file.getName().getPath();
      }
      return file.getName().getURI();
    } catch (Exception e) {
      throw new HopException(
          "Error building a configuration file path in folder '" + folder + "'", e);
    }
  }

  /**
   * @param folder candidate folder
   * @param projectHome project home folder
   * @return true when {@code folder} is the project home or a folder inside it
   */
  public static boolean isInsideProjectHome(String folder, String projectHome) {
    if (StringUtils.isBlank(folder) || StringUtils.isBlank(projectHome)) {
      return false;
    }
    try (FileObject home = HopVfs.getFileObject(projectHome);
        FileObject target = HopVfs.getFileObject(folder)) {
      String homePath = withTrailingSlash(home.getName().getPath());
      String targetPath = withTrailingSlash(target.getName().getPath());
      return targetPath.startsWith(homePath);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Mandatory variables and secrets, in that order. Optional variables are not included.
   *
   * @param environment embedded definition
   * @return variables to store in the local configuration file
   */
  public static List<DescribedVariable> variablesToStore(EmbeddedEnvironment environment) {
    List<DescribedVariable> stored = new ArrayList<>();
    addVariables(stored, environment == null ? null : environment.getMandatoryVariables());
    addVariables(stored, environment == null ? null : environment.getSecretVariables());
    return stored;
  }

  /**
   * Create a lifecycle environment for every embedded definition that is not already registered.
   * Existing configuration files are left as they are. Nothing is saved to {@code hop-config.json};
   * the caller registers {@link MaterializeResult#getCreated()}.
   *
   * @param config environments already on this computer
   * @param project project that holds the definitions
   * @param projectName project the new environments belong to
   * @param folder folder for the configuration files
   * @return what was created and what was left alone
   * @throws HopException when a configuration file cannot be written
   */
  public static MaterializeResult materialize(
      ProjectsConfig config, Project project, String projectName, String folder)
      throws HopException {
    MaterializeResult result = new MaterializeResult();
    if (project == null || project.getEmbeddedEnvironments() == null) {
      return result;
    }
    if (StringUtils.isBlank(folder)) {
      throw new HopException("A folder is required to create environment configuration files");
    }
    for (EmbeddedEnvironment embedded : project.getEmbeddedEnvironments()) {
      if (embedded == null || StringUtils.isBlank(embedded.getName())) {
        continue;
      }
      if (config != null && config.findEnvironment(embedded.getName()) != null) {
        result.skippedExistingNames.add(embedded.getName());
        continue;
      }
      String path = configFilePath(folder, embedded.getName());
      if (!writeConfigFileIfMissing(path, embedded)) {
        result.keptExistingFiles.add(embedded.getName());
      }
      LifecycleEnvironment environment =
          new LifecycleEnvironment(
              embedded.getName(), "", projectName, new ArrayList<>(List.of(path)));
      environment.setEmbeddedEnvironmentName(embedded.getName());
      result.created.add(environment);
    }
    return result;
  }

  /**
   * @param path configuration file path
   * @param embedded definition whose mandatory variables and secrets are written
   * @return true when a new file was written, false when a file was already there
   * @throws HopException when the file cannot be written
   */
  public static boolean writeConfigFileIfMissing(String path, EmbeddedEnvironment embedded)
      throws HopException {
    try (FileObject file = HopVfs.getFileObject(path)) {
      if (file.exists()) {
        return false;
      }
      if (file.getParent() != null && !file.getParent().exists()) {
        file.getParent().createFolder();
      }
    } catch (Exception e) {
      throw new HopException("Error checking configuration file '" + path + "'", e);
    }
    DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(path);
    configFile.setDescription(embedded.getDescription());
    configFile.setDescribedVariables(variablesToStore(embedded));
    configFile.saveToFile();
    return true;
  }

  private static void addVariables(
      List<DescribedVariable> stored, List<EmbeddedEnvironmentVariable> source) {
    if (source == null) {
      return;
    }
    for (EmbeddedEnvironmentVariable variable : source) {
      if (variable == null || StringUtils.isBlank(variable.getName())) {
        continue;
      }
      stored.add(
          new DescribedVariable(
              variable.getName().trim(),
              Const.NVL(variable.getDefaultValue(), ""),
              Const.NVL(variable.getDescription(), "")));
    }
  }

  private static String withTrailingSlash(String path) {
    String normalized = path.replace('\\', '/');
    if (!normalized.endsWith("/")) {
      normalized = normalized + "/";
    }
    return normalized;
  }

  /** Environments created by {@link #materialize} and the ones that were left alone. */
  @Getter
  public static final class MaterializeResult {
    private final List<LifecycleEnvironment> created = new ArrayList<>();
    private final List<String> skippedExistingNames = new ArrayList<>();
    private final List<String> keptExistingFiles = new ArrayList<>();
  }
}
