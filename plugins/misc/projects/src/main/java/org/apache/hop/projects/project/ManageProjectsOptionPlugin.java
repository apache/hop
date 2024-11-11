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

package org.apache.hop.projects.project;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.util.ProjectsUtil;
import picocli.CommandLine;

@ConfigPlugin(
    id = "ManageProjectsOptionPlugin",
    description = "Allows command line editing of the projects")
public class ManageProjectsOptionPlugin implements IConfigOptions {

  public static final String CONST_PROJECT = "Project '";

  @CommandLine.Option(
      names = {"-pc", "--project-create"},
      description = "Create a new project. Also specify the name and its home")
  private boolean createProject;

  @CommandLine.Option(
      names = {"-p", "--project"},
      description = "The name of the project to manage")
  private String projectName;

  @CommandLine.Option(
      names = {"-ph", "--project-home"},
      description = "The home directory of the project")
  private String projectHome;

  @CommandLine.Option(
      names = {"-pr", "--project-parent"},
      description = "The name of the parent project to inherit metadata and variables from")
  private String projectParent;

  @CommandLine.Option(
      names = {"-pf", "--project-config-file"},
      description =
          "The configuration file relative to the home folder. The default value is project-config.json")
  private String projectConfigFile;

  @CommandLine.Option(
      names = {"-ps", "--project-description"},
      description = "The description of the project")
  private String projectDescription;

  @CommandLine.Option(
      names = {"-pp", "--project-company"},
      description = "The company")
  private String projectCompany;

  @CommandLine.Option(
      names = {"-pt", "--project-department"},
      description = "The department")
  private String projectDepartment;

  @CommandLine.Option(
      names = {"-pa", "--project-metadata-base"},
      description = "The metadata base folder (relative to home)")
  private String projectMetadataBaseFolder;

  @CommandLine.Option(
      names = {"-pu", "--project-unit-tests-base"},
      description = "The unit tests base folder (relative to home)")
  private String projectUnitTestsBasePath;

  @CommandLine.Option(
      names = {"-pb", "--project-datasets-base"},
      description = "The data sets CSV folder (relative to home)")
  private String projectDataSetsCsvFolder;

  @CommandLine.Option(
      names = {"-px", "--project-enforce-execution"},
      description =
          "Validate before execution that a workflow or pipeline is located in the project home folder or a sub-folder (true/false).")
  private String projectEnforceExecutionInHome;

  @CommandLine.Option(
      names = {"-pv", "--project-variables"},
      description = "A list of variable=value combinations separated by a comma",
      split = ",")
  private String[] projectVariables;

  @CommandLine.Option(
      names = {"-pm", "--project-modify"},
      description = "Modify a project")
  private boolean modifyProject;

  @CommandLine.Option(
      names = {"-pd", "--project-delete"},
      description = "Delete a project")
  private boolean deleteProject;

  @CommandLine.Option(
      names = {"-pl", "--projects-list"},
      description = "List the defined projects")
  private boolean listProjects;

  @CommandLine.Option(
      names = {"-xm", "--export-metadata"},
      description =
          "Export project metadata to a single JSON file which you can specify with this option. Also specify the -p option.")
  private String metadataJsonFilename;

  @CommandLine.Option(
      names = {"-plt", "--project-list-transform-types"},
      description = "List transform types used in this project")
  private boolean projectTransformTypes;

  @CommandLine.Option(
      names = {"-pla", "--project-list-action-types"},
      description = "List action types used in this project")
  private boolean projectActionTypes;

  @CommandLine.Option(
      names = {"-plm", "--project-list-metadata-types"},
      description = "List metadata types used in this project")
  private boolean projectMetadataTypes;

  @CommandLine.Option(
      names = {"-plmi", "--project-list-metadata-item-usages"},
      description = "List all files that use a given metadata item in this project")
  private String projectFilesForMetadataItem;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    try {
      boolean changed = false;
      if (createProject) {
        createProject(log, config, variables);
        changed = true;
      } else if (modifyProject) {
        modifyProject(log, config, variables);
        changed = true;
      } else if (deleteProject) {
        deleteProject(log, config, variables);
        changed = true;
      } else if (listProjects) {
        listProjects(log, config, variables);
        changed = true;
      } else if (projectTransformTypes) {
        listTransformTypes(log, config, variables, hasHopMetadataProvider);
        changed = true;
      } else if (projectActionTypes) {
        listActionTypes(log, config, variables, hasHopMetadataProvider);
        changed = true;
      } else if (projectMetadataTypes) {
        listMetadataTypes(log, config, variables, hasHopMetadataProvider);
        changed = true;
      } else if (StringUtils.isNotEmpty(metadataJsonFilename)) {
        exportMetadataToJson(log, config, variables, hasHopMetadataProvider);
        changed = true;
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling environment configuration options", e);
    }
  }

  private void listProjects(ILogChannel log, ProjectsConfig config, IVariables variables)
      throws HopException {
    log.logBasic("Projects:");
    List<String> names = config.listProjectConfigNames();
    for (String name : names) {
      ProjectConfig projectConfig = config.findProjectConfig(name);
      try {
        Project project = projectConfig.loadProject(Variables.getADefaultVariableSpace());
        logProjectDetails(log, projectConfig, project);
      } catch (Exception e) {
        log.logBasic("  " + projectConfig.getProjectName() + " : " + projectHome);
        log.logBasic("    Configuration error: " + Const.getSimpleStackTrace(e));
      }
    }
  }

  private void logProjectDetails(ILogChannel log, ProjectConfig projectConfig, Project project)
      throws HopException {
    String projectHome = projectConfig.getProjectHome();
    log.logBasic("  " + projectConfig.getProjectName() + " : " + projectHome);
    if (StringUtils.isNotEmpty(project.getParentProjectName())) {
      log.logBasic("    Parent project: " + project.getParentProjectName());
    }
    log.logBasic(
        "    Configuration file: "
            + projectConfig.getActualProjectConfigFilename(Variables.getADefaultVariableSpace()));
    if (!project.getDescribedVariables().isEmpty()) {
      log.logBasic("    Described variables: ");
      for (DescribedVariable variable : project.getDescribedVariables()) {
        log.logBasic(
            "      "
                + variable.getName()
                + " = "
                + variable.getValue()
                + (StringUtils.isEmpty(variable.getDescription())
                    ? ""
                    : " (" + variable.getDescription() + ")"));
      }
    }
  }

  private void deleteProject(ILogChannel log, ProjectsConfig config, IVariables variables)
      throws Exception {
    validateProjectNameSpecified();

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      throw new HopException(CONST_PROJECT + projectName + "' doesn't exist, it can't be deleted");
    }
    config.removeProjectConfig(projectName);
    ProjectsConfigSingleton.saveConfig();

    log.logBasic(CONST_PROJECT + projectName + " was removed from the configuration");
  }

  private void modifyProject(ILogChannel log, ProjectsConfig config, IVariables variables)
      throws Exception {
    validateProjectNameSpecified();

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      throw new HopException(CONST_PROJECT + projectName + "' doesn't exist, it can't be modified");
    }

    if (StringUtils.isNotEmpty(projectHome)) {
      projectConfig.setProjectHome(projectHome);
    }
    if (StringUtils.isNotEmpty(projectConfigFile)) {
      projectConfig.setConfigFilename(projectConfigFile);
    }

    config.addProjectConfig(projectConfig);
    ProjectsConfigSingleton.saveConfig();

    String projectConfigFilename = projectConfig.getActualProjectConfigFilename(variables);

    Project project = projectConfig.loadProject(variables);

    log.logBasic(
        "Project configuration for '"
            + projectName
            + "' was modified in "
            + HopConfig.getInstance().getConfigFilename());

    if (modifyProjectSettings(project)) {

      // Check to see if there's not a loop in the project parent hierarchy
      //
      project.verifyProjectsChain(projectName, variables);

      project.saveToFile();

      log.logBasic(
          "Project settings for '"
              + projectName
              + "' were modified in file "
              + projectConfigFilename);
    }
  }

  private boolean modifyProjectSettings(Project project) {
    boolean changed = false;
    if (StringUtils.isNotEmpty(projectParent)) {
      project.setParentProjectName(projectParent);
      changed = true;
    }
    if (StringUtils.isNotEmpty(projectDescription)) {
      project.setDescription(projectDescription);
      changed = true;
    }
    if (StringUtils.isNotEmpty(projectCompany)) {
      project.setCompany(projectCompany);
      changed = true;
    }
    if (StringUtils.isNotEmpty(projectDepartment)) {
      project.setDepartment(projectDepartment);
      changed = true;
    }
    if (StringUtils.isNotEmpty(projectMetadataBaseFolder)) {
      project.setMetadataBaseFolder(projectMetadataBaseFolder);
      changed = true;
    }
    if (StringUtils.isNotEmpty(projectUnitTestsBasePath)) {
      project.setUnitTestsBasePath(projectUnitTestsBasePath);
      changed = true;
    }
    if (StringUtils.isNotEmpty(projectDataSetsCsvFolder)) {
      project.setDataSetsCsvFolder(projectDataSetsCsvFolder);
      changed = true;
    }
    if (StringUtils.isNotEmpty(projectEnforceExecutionInHome)) {
      boolean enabled =
          "y".equalsIgnoreCase(projectEnforceExecutionInHome)
              || "true".equalsIgnoreCase(projectEnforceExecutionInHome);
      project.setEnforcingExecutionInHome(enabled);
      changed = true;
    }
    if (projectVariables != null) {
      for (String projectVariable : projectVariables) {
        int equalsIndex = projectVariable.indexOf("=");
        if (equalsIndex > 0) {
          String varName = projectVariable.substring(0, equalsIndex);
          String varValue = projectVariable.substring(equalsIndex + 1);
          if (varValue != null && varValue.startsWith("\"") && varValue.endsWith("\"")) {
            varValue = varValue.substring(1, varValue.length() - 1);
          }
          DescribedVariable describedVariable = new DescribedVariable(varName, varValue, "");
          project.setDescribedVariable(describedVariable);
          changed = true;
        }
      }
    }

    return changed;
  }

  private void createProject(ILogChannel log, ProjectsConfig config, IVariables variables)
      throws Exception {
    validateProjectNameSpecified();
    validateProjectHomeSpecified();

    log.logBasic("Creating project '" + projectName + "'");

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig != null) {
      throw new HopException(CONST_PROJECT + projectName + "' already exists.");
    }

    String defaultProjectConfigFilename = variables.resolve(config.getDefaultProjectConfigFile());

    projectConfig =
        new ProjectConfig(
            projectName, projectHome, Const.NVL(projectConfigFile, defaultProjectConfigFilename));

    config.addProjectConfig(projectConfig);
    ProjectsConfigSingleton.saveConfig();

    log.logBasic(CONST_PROJECT + projectName + "' was created for home folder : " + projectHome);

    Project project = projectConfig.loadProject(variables);
    project.setParentProjectName(config.getStandardParentProject());
    modifyProjectSettings(project);

    // Check to see if there's not a loop in the project parent hierarchy
    //
    project.verifyProjectsChain(projectName, variables);

    // Always save, even if it's an empty file
    //
    project.saveToFile();

    log.logBasic(
        "Configuration file for project '"
            + projectName
            + "' was saved to : "
            + project.getConfigFilename());
  }

  private void validateProjectNameSpecified() throws Exception {
    if (StringUtil.isEmpty(projectName)) {
      throw new HopException("Please specify the name of the project");
    }
  }

  private void validateProjectHomeSpecified() throws Exception {
    if (StringUtil.isEmpty(projectHome)) {
      throw new HopException("Please specify the home directory of the project to create");
    }
  }

  private void exportMetadataToJson(
      ILogChannel log,
      ProjectsConfig config,
      IVariables variables,
      IHasHopMetadataProvider hasHopMetadataProvider)
      throws HopException {

    if (StringUtils.isEmpty(projectName)) {
      throw new HopException(
          "Please specify the name of the project for which you want to export the metadata");
    }

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      throw new HopException(
          CONST_PROJECT + projectName + "' couldn't be found in the Hop configuration");
    }
    Project project = projectConfig.loadProject(Variables.getADefaultVariableSpace());
    ProjectsUtil.enableProject(
        log, projectName, project, variables, new ArrayList<>(), null, hasHopMetadataProvider);
    log.logBasic("Enabled project " + projectName);

    String realFilename = variables.resolve(metadataJsonFilename);
    log.logBasic("Exporting project metadata to a single file: " + realFilename);

    // This is the metadata to export
    //
    SerializableMetadataProvider metadataProvider =
        new SerializableMetadataProvider(hasHopMetadataProvider.getMetadataProvider());
    String jsonString = metadataProvider.toJson();

    try {
      try (OutputStream outputStream = HopVfs.getOutputStream(realFilename, false)) {
        outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
      }
      log.logBasic("Metadata was exported successfully.");
    } catch (Exception e) {
      throw new HopException("There was an error exporting metadata to file: " + realFilename, e);
    }
  }

  public void listActionTypes(
      ILogChannel log,
      ProjectsConfig config,
      IVariables variables,
      IHasHopMetadataProvider hasHopMetadataProvider)
      throws HopException {
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    Project project = projectConfig.loadProject(variables);
    ProjectsUtil.enableProject(
        log, projectName, project, variables, new ArrayList<>(), null, hasHopMetadataProvider);
    try {
      List<String> actionTypes = project.getActionTypes(variables);
      if (actionTypes.isEmpty()) {
        log.logBasic("No action types found for project " + projectName);
      } else {
        log.logBasic("This project contains " + actionTypes.size() + " action type(s)");
        for (String actionType : actionTypes) {
          log.logBasic("   " + actionType);
        }
      }
    } catch (Exception e) {
      log.logError("Error listing action types for " + projectName);
      e.printStackTrace();
    }
  }

  public void listTransformTypes(
      ILogChannel log,
      ProjectsConfig config,
      IVariables variables,
      IHasHopMetadataProvider hasHopMetadataProvider)
      throws HopException {
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    Project project = projectConfig.loadProject(variables);
    ProjectsUtil.enableProject(
        log, projectName, project, variables, new ArrayList<>(), null, hasHopMetadataProvider);
    log.logBasic("Enabled project " + projectName);
    try {
      List<String> transformTypes = project.getTransformTypes(variables);
      if (transformTypes.isEmpty()) {
        log.logBasic("No transform types found for project " + projectName);
      } else {
        log.logBasic("This project contains " + transformTypes.size() + " transform type(s)");
        for (String transformType : transformTypes) {
          log.logBasic("   " + transformType);
        }
      }
    } catch (Exception e) {
      log.logError("Error getting transform types from project " + projectName);
      e.printStackTrace();
    }
  }

  /**
   * List the metadata types used in this project.
   *
   * @param log
   * @param config
   * @param variables
   * @param hasHopMetadataProvider
   * @throws HopException
   */
  public void listMetadataTypes(
      ILogChannel log,
      ProjectsConfig config,
      IVariables variables,
      IHasHopMetadataProvider hasHopMetadataProvider)
      throws HopException,
          InvocationTargetException,
          NoSuchMethodException,
          IllegalAccessException,
          IOException {
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    Project project = projectConfig.loadProject(variables);
    ProjectsUtil.enableProject(
        log, projectName, project, variables, new ArrayList<>(), null, hasHopMetadataProvider);

    // list pipelines and transforms where the provided metadata item was found.
    List<String> metadataTypeNames = project.getMetadataTypes();
    if (!Utils.isEmpty(projectFilesForMetadataItem)) {
      List<String> hplUsageResults =
          project.getPipelinesForMetadataItem(variables, projectFilesForMetadataItem);
      if (!Utils.isEmpty(hplUsageResults)) {
        log.logBasic(
            "metadata item '"
                + projectFilesForMetadataItem
                + "' was found in the following pipelines: ");
        for (String hplUsageResult : hplUsageResults) {
          log.logBasic("   " + hplUsageResult);
        }
      }

      // list workflows and actions where the provided metadata item was found.
      List<String> hwfUsageResults =
          project.getWorkflowsForMetadataItem(variables, projectFilesForMetadataItem);
      if (!Utils.isEmpty(hwfUsageResults)) {
        log.logBasic(
            "metadata item '"
                + projectFilesForMetadataItem
                + "' was found in the following workflows: ");
        for (String hwfUsageResult : hwfUsageResults) {
          log.logBasic("   " + hwfUsageResult);
        }
      }
    }

    if (projectFilesForMetadataItem.isEmpty()) {
      if (metadataTypeNames.isEmpty()) {
        log.logBasic("This project doesn't contain any metadata types");
      } else {
        log.logBasic("This project uses " + metadataTypeNames.size() + " metadata types");
        for (String metadataTypeName : metadataTypeNames) {
          log.logBasic("   " + metadataTypeName);
        }
      }
    }
  }

  /**
   * Gets createProject
   *
   * @return value of createProject
   */
  public boolean isCreateProject() {
    return createProject;
  }

  /**
   * @param createProject The createProject to set
   */
  public void setCreateProject(boolean createProject) {
    this.createProject = createProject;
  }

  /**
   * Gets projectName
   *
   * @return value of projectName
   */
  public String getProjectName() {
    return projectName;
  }

  /**
   * @param projectName The projectName to set
   */
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  /**
   * Gets projectHome
   *
   * @return value of projectHome
   */
  public String getProjectHome() {
    return projectHome;
  }

  /**
   * @param projectHome The projectHome to set
   */
  public void setProjectHome(String projectHome) {
    this.projectHome = projectHome;
  }

  /**
   * Gets projectConfigFile
   *
   * @return value of projectConfigFile
   */
  public String getProjectConfigFile() {
    return projectConfigFile;
  }

  /**
   * @param projectConfigFile The projectConfigFile to set
   */
  public void setProjectConfigFile(String projectConfigFile) {
    this.projectConfigFile = projectConfigFile;
  }

  /**
   * Gets projectDescription
   *
   * @return value of projectDescription
   */
  public String getProjectDescription() {
    return projectDescription;
  }

  /**
   * @param projectDescription The projectDescription to set
   */
  public void setProjectDescription(String projectDescription) {
    this.projectDescription = projectDescription;
  }

  /**
   * Gets projectCompany
   *
   * @return value of projectCompany
   */
  public String getProjectCompany() {
    return projectCompany;
  }

  /**
   * @param projectCompany The projectCompany to set
   */
  public void setProjectCompany(String projectCompany) {
    this.projectCompany = projectCompany;
  }

  /**
   * Gets projectDepartment
   *
   * @return value of projectDepartment
   */
  public String getProjectDepartment() {
    return projectDepartment;
  }

  /**
   * @param projectDepartment The projectDepartment to set
   */
  public void setProjectDepartment(String projectDepartment) {
    this.projectDepartment = projectDepartment;
  }

  /**
   * Gets projectMetadataBaseFolder
   *
   * @return value of projectMetadataBaseFolder
   */
  public String getProjectMetadataBaseFolder() {
    return projectMetadataBaseFolder;
  }

  /**
   * @param projectMetadataBaseFolder The projectMetadataBaseFolder to set
   */
  public void setProjectMetadataBaseFolder(String projectMetadataBaseFolder) {
    this.projectMetadataBaseFolder = projectMetadataBaseFolder;
  }

  /**
   * Gets projectUnitTestsBasePath
   *
   * @return value of projectUnitTestsBasePath
   */
  public String getProjectUnitTestsBasePath() {
    return projectUnitTestsBasePath;
  }

  /**
   * @param projectUnitTestsBasePath The projectUnitTestsBasePath to set
   */
  public void setProjectUnitTestsBasePath(String projectUnitTestsBasePath) {
    this.projectUnitTestsBasePath = projectUnitTestsBasePath;
  }

  /**
   * Gets projectDataSetsCsvFolder
   *
   * @return value of projectDataSetsCsvFolder
   */
  public String getProjectDataSetsCsvFolder() {
    return projectDataSetsCsvFolder;
  }

  /**
   * @param projectDataSetsCsvFolder The projectDataSetsCsvFolder to set
   */
  public void setProjectDataSetsCsvFolder(String projectDataSetsCsvFolder) {
    this.projectDataSetsCsvFolder = projectDataSetsCsvFolder;
  }

  /**
   * Gets projectEnforceExecutionInHome
   *
   * @return value of projectEnforceExecutionInHome
   */
  public String getProjectEnforceExecutionInHome() {
    return projectEnforceExecutionInHome;
  }

  /**
   * @param projectEnforceExecutionInHome The projectEnforceExecutionInHome to set
   */
  public void setProjectEnforceExecutionInHome(String projectEnforceExecutionInHome) {
    this.projectEnforceExecutionInHome = projectEnforceExecutionInHome;
  }

  /**
   * Gets projectVariables
   *
   * @return value of projectVariables
   */
  public String[] getProjectVariables() {
    return projectVariables;
  }

  /**
   * @param projectVariables The projectVariables to set
   */
  public void setProjectVariables(String[] projectVariables) {
    this.projectVariables = projectVariables;
  }

  /**
   * Gets modifyProject
   *
   * @return value of modifyProject
   */
  public boolean isModifyProject() {
    return modifyProject;
  }

  /**
   * @param modifyProject The modifyProject to set
   */
  public void setModifyProject(boolean modifyProject) {
    this.modifyProject = modifyProject;
  }

  /**
   * Gets deleteProject
   *
   * @return value of deleteProject
   */
  public boolean isDeleteProject() {
    return deleteProject;
  }

  /**
   * @param deleteProject The deleteProject to set
   */
  public void setDeleteProject(boolean deleteProject) {
    this.deleteProject = deleteProject;
  }

  /**
   * Gets listProjects
   *
   * @return value of listProjects
   */
  public boolean isListProjects() {
    return listProjects;
  }

  /**
   * @param listProjects The listProjects to set
   */
  public void setListProjects(boolean listProjects) {
    this.listProjects = listProjects;
  }

  /**
   * Gets projectParent
   *
   * @return value of projectParent
   */
  public String getProjectParent() {
    return projectParent;
  }

  /**
   * @param projectParent The projectParent to set
   */
  public void setProjectParent(String projectParent) {
    this.projectParent = projectParent;
  }
}
