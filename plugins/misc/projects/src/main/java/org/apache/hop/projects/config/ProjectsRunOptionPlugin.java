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

package org.apache.hop.projects.config;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

@ConfigPlugin(
    id = "ProjectsRunOptionPlugin",
    description = "Project and Environment configuration options for hop-run",
    category = ConfigPlugin.CATEGORY_RUN)
public class ProjectsRunOptionPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"-e", "--environment"},
      description = "The name of the lifecycle environment to use")
  private String environmentOption = null;

  @CommandLine.Option(
      names = {"-j", "--project"},
      description = "The name of the project to use")
  private String projectOption = null;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = null;
    List<String> configurationFiles = new ArrayList<>();
    String projectName = projectOption;
    String environmentName = environmentOption;

    // You can specify the project using -p (project) or -e (lifecycle environment)
    // The main difference is that the environment provides extra configuration files to consider.
    //

    // If there is no environment specified but we have a default set, take that one...
    //
    if (StringUtils.isEmpty( environmentName )) {
      environmentName = config.getDefaultEnvironment();
    }

    // See if an environment is mandatory...
    //
    if (config.isEnvironmentMandatory() && StringUtils.isEmpty( environmentName )) {
      throw new HopException("Use of an environment is configured to be mandatory and none was specified.");
    }

    // If there is no project specified but we have a default set, take that one...
    //
    if (StringUtils.isEmpty( projectName )) {
      projectName = config.getDefaultProject();
    }

    // See if a project is mandatory...
    //
    if (config.isProjectMandatory() && StringUtils.isEmpty( projectName )) {
      throw new HopException("Use of a project is configured to be mandatory and none was specified.");
    }

    if ( StringUtils.isNotEmpty(environmentName)) {
      // The environment contains extra configuration options we need to pass along...
      //
      LifecycleEnvironment environment = config.findEnvironment(environmentName);
      if (environment == null) {
        throw new HopException(
          "Unable to find lifecycle environment '" + environmentName + "'");
      }
      projectName = environment.getProjectName();

      if (StringUtils.isEmpty(projectName)) {
        throw new HopException(
          "Lifecycle environment '"
            + environmentOption
            + "' is not referencing a project.");
      }
      projectConfig = config.findProjectConfig(projectName);
      if (projectConfig == null) {
        throw new HopException("Unable to find project '" + projectName + "' referenced in environment '"+environmentName);
      }
      configurationFiles.addAll(environment.getConfigurationFiles());

      log.logBasic(
        "Referencing environment '"
          + environmentOption
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
        log.logDebug(
          "No project or environment referenced.");
        return false;
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
