/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.projects.project;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.IConfigFile;
import org.apache.hop.core.config.plugin.ConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.core.config.DescribedVariablesConfigFile;

import java.io.File;
import java.util.List;

public class Project extends ConfigFile implements IConfigFile {

  @JsonIgnore
  private String configFilename;

  private String description;

  private String company;

  private String department;

  private String metadataBaseFolder;

  private String unitTestsBasePath;

  private String dataSetsCsvFolder;

  private boolean enforcingExecutionInHome;

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

  @Override public void saveToFile() throws HopException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion( JsonInclude.Include.NON_DEFAULT );
      objectMapper.enable( SerializationFeature.INDENT_OUTPUT );
      objectMapper.writeValue(new File(configFilename), this );
    } catch(Exception e) {
      throw new HopException("Error saving project configuration to file '"+configFilename+"'", e);
    }
  }

  @Override public void readFromFile() throws HopException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      Project project = objectMapper.readValue(new File(configFilename), Project.class );

      this.description = project.description;
      this.company = project.company;
      this.department = project.department;
      this.metadataBaseFolder = project.metadataBaseFolder;
      this.unitTestsBasePath = project.unitTestsBasePath;
      this.dataSetsCsvFolder = project.dataSetsCsvFolder;
      this.enforcingExecutionInHome = project.enforcingExecutionInHome;
      this.configMap = project.configMap;
    } catch(Exception e) {
      throw new HopException("Error saving project configuration to file '"+configFilename+"'", e);
    }
  }

  public void modifyVariables( IVariables variables, ProjectConfig projectConfig, List<String> configurationFiles, String environmentName ) {

    if ( variables == null ) {
      variables = Variables.getADefaultVariableSpace();
    }

    // Set the name of the active environment
    //
    variables.setVariable( Defaults.VARIABLE_HOP_PROJECT_NAME, Const.NVL( projectConfig.getProjectName(), "" ) );
    variables.setVariable( Defaults.VARIABLE_HOP_ENVIRONMENT_NAME, Const.NVL( environmentName, "" ) );

    // To allow circular logic where an environment file is relative to the project home
    //
    if ( StringUtils.isNotEmpty( projectConfig.getProjectHome() ) ) {
      String realValue = variables.resolve( projectConfig.getProjectHome() );
      variables.setVariable( ProjectsUtil.VARIABLE_PROJECT_HOME, realValue );
    }

    // Apply the described variables from the various configuration files in the given order...
    //
    for (String configurationFile : configurationFiles) {
      String realConfigurationFile = variables.resolve( configurationFile );

      File file = new File(realConfigurationFile);
      if (file.exists()) {
        ConfigFile configFile = new DescribedVariablesConfigFile( realConfigurationFile );
        try {
          configFile.readFromFile();

          // Apply the variable values...
          //
          for (DescribedVariable describedVariable : configFile.getDescribedVariables()) {
            variables.setVariable( describedVariable.getName(), describedVariable.getValue() );
          }

        } catch(Exception e) {
          LogChannel.GENERAL.logError( "Error reading described variables from configuration file '"+realConfigurationFile+"'", e);
        }
      } else {
        LogChannel.GENERAL.logError( "Configuration file '"+realConfigurationFile+"' does not exist to read variables from.");
      }
    }

    if ( StringUtils.isNotEmpty( metadataBaseFolder ) ) {
      String realValue = variables.resolve( metadataBaseFolder );
      variables.setVariable( Const.HOP_METADATA_FOLDER, realValue );
    }
    if ( StringUtils.isNotEmpty( unitTestsBasePath ) ) {
      String realValue = variables.resolve( unitTestsBasePath );
      variables.setVariable( ProjectsUtil.VARIABLE_HOP_UNIT_TESTS_FOLDER, realValue );
    }
    if ( StringUtils.isNotEmpty( dataSetsCsvFolder ) ) {
      String realValue = variables.resolve( dataSetsCsvFolder );
      variables.setVariable( ProjectsUtil.VARIABLE_HOP_DATASETS_FOLDER, realValue );
    }
    for ( DescribedVariable variable : getDescribedVariables() ) {
      if ( variable.getName() != null ) {
        variables.setVariable( variable.getName(), variable.getValue() );
      }
    }

  }

  /**
   * Gets configFilename
   *
   * @return value of configFilename
   */
  @Override public String getConfigFilename() {
    return configFilename;
  }

  /**
   * @param configFilename The configFilename to set
   */
  @Override public void setConfigFilename( String configFilename ) {
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

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
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

  /**
   * @param company The company to set
   */
  public void setCompany( String company ) {
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

  /**
   * @param department The department to set
   */
  public void setDepartment( String department ) {
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

  /**
   * @param metadataBaseFolder The metadataBaseFolder to set
   */
  public void setMetadataBaseFolder( String metadataBaseFolder ) {
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

  /**
   * @param unitTestsBasePath The unitTestsBasePath to set
   */
  public void setUnitTestsBasePath( String unitTestsBasePath ) {
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

  /**
   * @param dataSetsCsvFolder The dataSetsCsvFolder to set
   */
  public void setDataSetsCsvFolder( String dataSetsCsvFolder ) {
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

  /**
   * @param enforcingExecutionInHome The enforcingExecutionInHome to set
   */
  public void setEnforcingExecutionInHome( boolean enforcingExecutionInHome ) {
    this.enforcingExecutionInHome = enforcingExecutionInHome;
  }

}
