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

package org.apache.hop.projects.project;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.config.IConfigFile;
import org.apache.hop.core.config.plugin.ConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Project extends ConfigFile implements IConfigFile {

  @JsonIgnore private String configFilename;

  private String description;

  private String company;

  private String department;

  private String metadataBaseFolder;

  private String unitTestsBasePath;

  private String dataSetsCsvFolder;

  private boolean enforcingExecutionInHome;

  private String parentProjectName;

  public Project() {
    super();
    metadataBaseFolder = "${" + ProjectsUtil.VARIABLE_PROJECT_HOME + "}/metadata";
    dataSetsCsvFolder = "${" + ProjectsUtil.VARIABLE_PROJECT_HOME + "}/datasets";
    unitTestsBasePath = "${" + ProjectsUtil.VARIABLE_PROJECT_HOME + "}";
    enforcingExecutionInHome = true;
  }

  public Project(String configFilename) {
    this();
    this.configFilename = configFilename;
  }

  @Override
  public void saveToFile() throws HopException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
      objectMapper.writeValue(new File(configFilename), this);
    } catch (Exception e) {
      throw new HopException(
          "Error saving project configuration to file '" + configFilename + "'", e);
    }
  }

  @Override
  public void readFromFile() throws HopException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      Project project = objectMapper.readValue(new File(configFilename), Project.class);

      this.description = project.description;
      this.company = project.company;
      this.department = project.department;
      this.metadataBaseFolder = project.metadataBaseFolder;
      this.unitTestsBasePath = project.unitTestsBasePath;
      this.dataSetsCsvFolder = project.dataSetsCsvFolder;
      this.enforcingExecutionInHome = project.enforcingExecutionInHome;
      this.configMap = project.configMap;
      this.parentProjectName = project.parentProjectName;
    } catch (Exception e) {
      throw new HopException(
          "Error saving project configuration to file '" + configFilename + "'", e);
    }
  }

  public void modifyVariables(
      IVariables variables,
      ProjectConfig projectConfig,
      List<String> configurationFiles,
      String environmentName)
      throws HopException {

    if (variables == null) {
      variables = Variables.getADefaultVariableSpace();
    }

    // See if we don't have an infinite loop in the project-parent-parent-... hierarchy...
    //
    verifyProjectsChain(projectConfig.getProjectName(), variables);

    // If there is a parent project we want to pick up the variables defined in the project
    // definition as well
    //
    Project parentProject = null;
    String realParentProjectName = variables.resolve(parentProjectName);
    if (StringUtils.isNotEmpty(realParentProjectName)) {

      ProjectConfig parentProjectConfig =
          ProjectsConfigSingleton.getConfig().findProjectConfig(realParentProjectName);
      if (parentProjectConfig != null) {
        try {
          parentProject = parentProjectConfig.loadProject(variables);
          // Apply the variables set in the parent project
          //
          parentProject.modifyVariables(variables, parentProjectConfig, new ArrayList<>(), null);
        } catch (HopException he) {
          LogChannel.GENERAL.logError(
              "Error loading configuration file of parent project '" + realParentProjectName + "'",
              he);
        }
      }
    }

    // Set the name of the active environment
    //
    variables.setVariable(
        Defaults.VARIABLE_HOP_PROJECT_NAME, Const.NVL(projectConfig.getProjectName(), ""));
    variables.setVariable(Defaults.VARIABLE_HOP_ENVIRONMENT_NAME, Const.NVL(environmentName, ""));

    // To allow circular logic where an environment file is relative to the project home
    //
    if (StringUtils.isNotEmpty(projectConfig.getProjectHome())) {
      String realValue = variables.resolve(projectConfig.getProjectHome());
      variables.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, realValue);
    }

    // Apply the described variables from the various configuration files in the given order...
    //
    for (String configurationFile : configurationFiles) {
      String realConfigurationFile = variables.resolve(configurationFile);

      File file = new File(realConfigurationFile);
      if (file.exists()) {
        ConfigFile configFile = new DescribedVariablesConfigFile(realConfigurationFile);
        try {
          configFile.readFromFile();

          // Apply the variable values...
          //
          for (DescribedVariable describedVariable : configFile.getDescribedVariables()) {
            variables.setVariable(describedVariable.getName(), describedVariable.getValue());
          }

        } catch (Exception e) {
          LogChannel.GENERAL.logError(
              "Error reading described variables from configuration file '"
                  + realConfigurationFile
                  + "'",
              e);
        }
      } else {
        LogChannel.GENERAL.logError(
            "Configuration file '"
                + realConfigurationFile
                + "' does not exist to read variables from.");
      }
    }

    if (StringUtils.isNotEmpty(metadataBaseFolder)) {
      String realMetadataBaseFolder = variables.resolve(metadataBaseFolder);

      // If we have more than one metadata base folder to read metadata from, we can specify it
      // using comma separated values...
      //
      if (parentProject != null) {
        // HOP_METADATA_FOLDER was set above in the variables.
        // We're going to simply append to it.
        //
        String parentMetadataFolder = variables.getVariable(Const.HOP_METADATA_FOLDER);
        if (StringUtils.isNotEmpty(parentMetadataFolder)) {
          realMetadataBaseFolder = parentMetadataFolder + "," + realMetadataBaseFolder;
        }
      }
      variables.setVariable(Const.HOP_METADATA_FOLDER, realMetadataBaseFolder);
    }
    if (StringUtils.isNotEmpty(unitTestsBasePath)) {
      String realValue = variables.resolve(unitTestsBasePath);
      variables.setVariable(ProjectsUtil.VARIABLE_HOP_UNIT_TESTS_FOLDER, realValue);
    }
    if (StringUtils.isNotEmpty(dataSetsCsvFolder)) {
      String realValue = variables.resolve(dataSetsCsvFolder);
      variables.setVariable(ProjectsUtil.VARIABLE_HOP_DATASETS_FOLDER, realValue);
    }
    for (DescribedVariable variable : getDescribedVariables()) {
      if (variable.getName() != null) {
        variables.setVariable(variable.getName(), variable.getValue());
      }
    }
  }

  /**
   * Let's check to see if there isn't an infinite loop in the project definition
   *
   * @throws HopException
   */
  public void verifyProjectsChain(String projectName, IVariables variables) throws HopException {

    // No parent project: no danger
    //
    if (StringUtils.isEmpty(parentProjectName)) {
      return;
    }

    if (parentProjectName.equals(projectName)) {
      throw new HopException(
          "Parent project '" + parentProjectName + "' can not be the same as the project itself");
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    String realParentProjectName = variables.resolve(parentProjectName);
    List<String> projectsList = new ArrayList<>();
    while (StringUtils.isNotEmpty(realParentProjectName)) {
      projectsList.add(realParentProjectName);
      ProjectConfig projectConfig = config.findProjectConfig(realParentProjectName);
      if (projectConfig != null) {
        Project parentProject = projectConfig.loadProject(variables);
        if (parentProject == null) {
          // Can't be loaded, break out of the loop
          realParentProjectName = null;
        } else {
          // See if this project has a parent...
          //
          if (StringUtils.isEmpty(parentProject.parentProjectName)) {
            // We're done
            realParentProjectName = null;
          } else {
            realParentProjectName = variables.resolve(parentProject.parentProjectName);
            if (StringUtils.isNotEmpty(realParentProjectName)) {
              // See if we've had this one before...
              //
              if (projectsList.contains(realParentProjectName)) {
                throw new HopException(
                    "There is a loop in the parent projects hierarchy: project "
                        + realParentProjectName
                        + " references itself");
              }
            }
          }
        }
      } else {
        // Project not found: config error, stop looking
        realParentProjectName = null;
      }
    }
  }

  /**
   * Gets configFilename
   *
   * @return value of configFilename
   */
  @Override
  public String getConfigFilename() {
    return configFilename;
  }

  /** @param configFilename The configFilename to set */
  @Override
  public void setConfigFilename(String configFilename) {
    this.configFilename = configFilename;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /** @param description The description to set */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Gets company
   *
   * @return value of company
   */
  public String getCompany() {
    return company;
  }

  /** @param company The company to set */
  public void setCompany(String company) {
    this.company = company;
  }

  /**
   * Gets department
   *
   * @return value of department
   */
  public String getDepartment() {
    return department;
  }

  /** @param department The department to set */
  public void setDepartment(String department) {
    this.department = department;
  }

  /**
   * Gets metadataBaseFolder
   *
   * @return value of metadataBaseFolder
   */
  public String getMetadataBaseFolder() {
    return metadataBaseFolder;
  }

  /** @param metadataBaseFolder The metadataBaseFolder to set */
  public void setMetadataBaseFolder(String metadataBaseFolder) {
    this.metadataBaseFolder = metadataBaseFolder;
  }

  /**
   * Gets unitTestsBasePath
   *
   * @return value of unitTestsBasePath
   */
  public String getUnitTestsBasePath() {
    return unitTestsBasePath;
  }

  /** @param unitTestsBasePath The unitTestsBasePath to set */
  public void setUnitTestsBasePath(String unitTestsBasePath) {
    this.unitTestsBasePath = unitTestsBasePath;
  }

  /**
   * Gets dataSetsCsvFolder
   *
   * @return value of dataSetsCsvFolder
   */
  public String getDataSetsCsvFolder() {
    return dataSetsCsvFolder;
  }

  /** @param dataSetsCsvFolder The dataSetsCsvFolder to set */
  public void setDataSetsCsvFolder(String dataSetsCsvFolder) {
    this.dataSetsCsvFolder = dataSetsCsvFolder;
  }

  /**
   * Gets enforcingExecutionInHome
   *
   * @return value of enforcingExecutionInHome
   */
  public boolean isEnforcingExecutionInHome() {
    return enforcingExecutionInHome;
  }

  /** @param enforcingExecutionInHome The enforcingExecutionInHome to set */
  public void setEnforcingExecutionInHome(boolean enforcingExecutionInHome) {
    this.enforcingExecutionInHome = enforcingExecutionInHome;
  }

  /**
   * Gets parentProjectName
   *
   * @return value of parentProjectName
   */
  public String getParentProjectName() {
    return parentProjectName;
  }

  /** @param parentProjectName The parentProjectName to set */
  public void setParentProjectName(String parentProjectName) {
    this.parentProjectName = parentProjectName;
  }
}
