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

package org.apache.hop.projects.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsConfigHelper;
import org.apache.hop.projects.util.ProjectsUtil;
import picocli.CommandLine;

@Getter
@Setter
public class ProjectsOptionPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"-e", "--environment"},
      description = "The name of the lifecycle environment to use")
  private String environmentOption = null;

  @CommandLine.Option(
      names = {"-j", "--project"},
      description = "The name of the project to use")
  private String projectOption = null;

  @CommandLine.Option(
      names = {"-pl", "--project-locations"},
      description = "Comma-separated list of project locations (name=location or archive files)",
      split = ",")
  protected String[] projectLocations = null;

  @CommandLine.Option(
      names = {"--environments"},
      description =
          "Comma-separated list of environment definitions (name=[project:]configFile1;configFile2)",
      split = ",")
  protected String[] environments = null;

  @CommandLine.Option(
      names = {"--environment-conf-files", "--environment-config-files"},
      description =
          "Comma-separated list of configuration files to apply to the project or environment",
      split = ",")
  protected String[] environmentConfigFiles = null;

  @CommandLine.Option(
      names = {"-im", "--in-memory"},
      description = "Keep configuration in memory without persisting to hop-config.json")
  protected boolean inMemory = false;

  protected String projectName;
  protected String environmentName;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {

    if (inMemory
        || projectLocations != null
        || environments != null
        || environmentConfigFiles != null) {
      ProjectsConfigHelper.enableInMemoryMode(log);
    }

    List<String> registeredProjects = new ArrayList<>();
    if (projectLocations != null && projectLocations.length > 0) {
      registeredProjects =
          ProjectsConfigHelper.addProjectLocations(log, variables, projectLocations);
    }

    if (environments != null && environments.length > 0) {
      ProjectsConfigHelper.addEnvironments(log, variables, environments, registeredProjects);
    }

    projectName = projectOption;
    environmentName = environmentOption;

    if (StringUtils.isEmpty(projectName) && StringUtils.isEmpty(environmentName)) {
      projectName =
          ProjectsConfigHelper.determineActiveProject(
              projectName, environmentName, registeredProjects, variables);
    }

    if (hasHopMetadataProvider == null
        && StringUtils.isEmpty(projectName)
        && StringUtils.isEmpty(environmentName)) {
      return false;
    }

    List<String> extraConfigFiles = new ArrayList<>();
    if (environmentConfigFiles != null) {
      for (String cf : environmentConfigFiles) {
        if (StringUtils.isNotEmpty(cf)) {
          extraConfigFiles.add(cf.trim());
        }
      }
    }

    return configure(
        log, variables, hasHopMetadataProvider, projectName, environmentName, extraConfigFiles);
  }

  public static final boolean configure(
      ILogChannel log,
      IVariables variables,
      IHasHopMetadataProvider hasHopMetadataProvider,
      String projectName,
      String environmentName)
      throws HopException {
    return configure(
        log,
        variables,
        hasHopMetadataProvider,
        projectName,
        environmentName,
        Collections.emptyList());
  }

  public static final boolean configure(
      ILogChannel log,
      IVariables variables,
      IHasHopMetadataProvider hasHopMetadataProvider,
      String projectName,
      String environmentName,
      List<String> extraConfigFiles)
      throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig;
    List<String> configurationFiles = new ArrayList<>();
    LifecycleEnvironment environment;

    // You can specify the project using -p (project) or -e (lifecycle environment)
    // The main difference is that the environment provides extra configuration files to consider.
    //

    // If there is no environment specified but we have a default set, take that one...
    //
    if (StringUtils.isEmpty(environmentName)) {
      environmentName = config.getDefaultEnvironment();
    }

    // See if an environment is mandatory...
    //
    if (config.isEnvironmentMandatory() && StringUtils.isEmpty(environmentName)) {
      throw new HopException(
          "Use of an environment is configured to be mandatory and none was specified.");
    }

    // If there is no project specified but we have a default set, take that one...
    //
    if (StringUtils.isEmpty(projectName)) {
      projectName = config.getDefaultProject();
    }

    // See if a project is mandatory...
    //
    if (config.isProjectMandatory() && StringUtils.isEmpty(projectName)) {
      throw new HopException(
          "Use of a project is configured to be mandatory and none was specified.");
    }

    if (StringUtils.isNotEmpty(environmentName)) {
      // The environment contains extra configuration options we need to pass along...
      //
      environment = config.findEnvironment(environmentName);
      if (environment == null) {
        throw new HopException("Unable to find lifecycle environment '" + environmentName + "'");
      }
      projectName = environment.getProjectName();

      if (StringUtils.isEmpty(projectName)) {
        throw new HopException(
            "Lifecycle environment '" + environmentName + "' is not referencing a project.");
      }
      projectConfig = config.findProjectConfig(projectName);
      if (projectConfig == null) {
        throw new HopException(
            "Unable to find project '"
                + projectName
                + "' referenced in environment '"
                + environmentName);
      }
      configurationFiles.addAll(environment.getConfigurationFiles());

      log.logBasic(
          "Referencing environment '"
              + environmentName
              + "' for project "
              + projectName
              + "' in "
              + environment.getPurpose());
    } else if (StringUtils.isNotEmpty(projectName)) {
      // Simply reference the project directly without extra configuration files...
      //
      projectConfig = config.findProjectConfig(projectName);
      if (projectConfig == null) {
        throw new HopException("Unable to find project '" + projectName + "'");
      }
      projectName = projectConfig.getProjectName();
    } else {
      log.logDebug("No project or environment referenced.");
      return false;
    }

    if (extraConfigFiles != null && !extraConfigFiles.isEmpty()) {
      configurationFiles.addAll(extraConfigFiles);
    }

    try {
      Project project = projectConfig.loadProject(variables);
      log.logBasic("Enabling project '" + projectName + "'");

      if (project == null) {
        throw new HopException("Project '" + projectName + "' couldn't be found");
      }
      // Now we just enable this project
      //
      ProjectsUtil.enableProject(
          log,
          projectName,
          project,
          variables,
          configurationFiles,
          environmentName,
          hasHopMetadataProvider);

      return true;
    } catch (Exception e) {
      throw new HopException("Error enabling project '" + projectName + "'", e);
    }
  }
}
