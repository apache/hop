package org.apache.hop.env.environment;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.env.util.Defaults;
import org.apache.hop.env.util.EnvironmentUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "Hop Environment",
  description = "An environment to tie together all sorts of configuration options"
)
public class Environment {

  // Information about the environment itself
  //
  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private String version;

  // Environment metadata (nice to know)
  //
  @MetaStoreAttribute
  private String company;

  @MetaStoreAttribute
  private String department;

  @MetaStoreAttribute
  private String project;

  // Technical information
  //
  @MetaStoreAttribute
  private String environmentHomeFolder;

  @MetaStoreAttribute( key = "homeFolder" )
  private String kettleHomeFolder;

  @MetaStoreAttribute
  private String metaStoreBaseFolder;

  // Git information
  //
  @MetaStoreAttribute
  private String spoonGitProject;

  // Data Sets , Unit tests
  //
  @MetaStoreAttribute
  private String unitTestsBasePath;

  @MetaStoreAttribute
  private String dataSetsCsvFolder;

  @MetaStoreAttribute( key = "enforce_execution_in_environment" )
  private boolean enforcingExecutionInHome;

  @MetaStoreAttribute
  private boolean autoSavingSpoonSession;

  @MetaStoreAttribute
  private boolean autoRestoringSpoonSession;

  // Variables
  //
  @MetaStoreAttribute
  private List<EnvironmentVariable> variables;


  public Environment() {
    variables = new ArrayList<>();
  }

  public String toJsonString() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion( JsonInclude.Include.NON_DEFAULT );
    objectMapper.enable( SerializationFeature.INDENT_OUTPUT );
    return objectMapper.writeValueAsString( this );
  }

  public static Environment fromJsonString( String jsonString ) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue( jsonString, Environment.class );
  }


  public void applySuggestedSettings() {
    environmentHomeFolder = "/path/to/your/environment/folder/";
    metaStoreBaseFolder = "${" + EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME + "}";
    dataSetsCsvFolder = "${" + EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME + "}/datasets";
    unitTestsBasePath = "${" + EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME + "}";
    enforcingExecutionInHome = true;
    autoSavingSpoonSession = true;
    autoRestoringSpoonSession = true;
  }

  public void applyKettleDefaultSettings() {
    name = "Default";
    description = "These are the default settings for Kettle";
    kettleHomeFolder = "${user.home}";
    metaStoreBaseFolder = "${user.home}/.pentaho/";
  }

  public void modifySystem() {
    modifyIVariables( null, true );
  }

  public void modifyIVariables( IVariables space ) {
    modifyIVariables( space, false );
  }

  public void modifyIVariables( IVariables space, boolean modifySystem ) {

    if ( space == null ) {
      space = Variables.getADefaultVariableSpace();
    }

    // Set the name of the active environment
    //
    space.setVariable( Defaults.VARIABLE_ACTIVE_ENVIRONMENT, Const.NVL( name, "" ) );
    if ( modifySystem ) {
      System.setProperty( Defaults.VARIABLE_ACTIVE_ENVIRONMENT, Const.NVL( name, "" ) );
    }

    if ( StringUtils.isNotEmpty( environmentHomeFolder ) ) {
      String realValue = space.environmentSubstitute( environmentHomeFolder );
      space.setVariable( EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME, realValue );
      if ( modifySystem ) {
        System.setProperty( EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME, realValue );
      }
    }
    if ( StringUtils.isNotEmpty( kettleHomeFolder ) ) {
      String realValue = space.environmentSubstitute( kettleHomeFolder );
      space.setVariable( "KETTLE_HOME", realValue );
      if ( modifySystem ) {
        System.setProperty( "KETTLE_HOME", realValue );
      }
    }
    if ( StringUtils.isNotEmpty( metaStoreBaseFolder ) ) {
      String realValue = space.environmentSubstitute( metaStoreBaseFolder );
      space.setVariable( Const.HOP_METASTORE_FOLDER, realValue );
      if ( modifySystem ) {
        System.setProperty( Const.HOP_METASTORE_FOLDER, realValue );
      }
    }
    if ( StringUtils.isNotEmpty( unitTestsBasePath ) ) {
      String realValue = space.environmentSubstitute( unitTestsBasePath );
      space.setVariable( EnvironmentUtil.VARIABLE_UNIT_TESTS_BASE_PATH, realValue );
      if ( modifySystem ) {
        System.setProperty( EnvironmentUtil.VARIABLE_UNIT_TESTS_BASE_PATH, realValue );
      }
    }
    if ( StringUtils.isNotEmpty( dataSetsCsvFolder ) ) {
      String realValue = space.environmentSubstitute( dataSetsCsvFolder );
      space.setVariable( EnvironmentUtil.VARIABLE_DATASETS_BASE_PATH, realValue );
      if ( modifySystem ) {
        System.setProperty( EnvironmentUtil.VARIABLE_DATASETS_BASE_PATH, realValue );
      }
    }

    for ( EnvironmentVariable variable : variables ) {
      if ( variable.getName() != null ) {
        space.setVariable( variable.getName(), variable.getValue() );
        if ( modifySystem ) {
          if ( StringUtils.isEmpty( variable.getValue() ) ) {
            System.clearProperty( variable.getName() );
          } else {
            System.setProperty( variable.getName(), variable.getValue() );
          }
        }
      }
    }
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
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
   * Gets environmentHomeFolder
   *
   * @return value of environmentHomeFolder
   */
  public String getEnvironmentHomeFolder() {
    return environmentHomeFolder;
  }

  /**
   * @param environmentHomeFolder The environmentHomeFolder to set
   */
  public void setEnvironmentHomeFolder( String environmentHomeFolder ) {
    this.environmentHomeFolder = environmentHomeFolder;
  }

  /**
   * Gets kettleHomeFolder
   *
   * @return value of kettleHomeFolder
   */
  public String getKettleHomeFolder() {
    return kettleHomeFolder;
  }

  /**
   * @param kettleHomeFolder The kettleHomeFolder to set
   */
  public void setKettleHomeFolder( String kettleHomeFolder ) {
    this.kettleHomeFolder = kettleHomeFolder;
  }

  /**
   * Gets spoonGitProject
   *
   * @return value of spoonGitProject
   */
  public String getSpoonGitProject() {
    return spoonGitProject;
  }

  /**
   * @param spoonGitProject The spoonGitProject to set
   */
  public void setSpoonGitProject( String spoonGitProject ) {
    this.spoonGitProject = spoonGitProject;
  }

  /**
   * Gets metaStoreBaseFolder
   *
   * @return value of metaStoreBaseFolder
   */
  public String getMetaStoreBaseFolder() {
    return metaStoreBaseFolder;
  }

  /**
   * @param metaStoreBaseFolder The metaStoreBaseFolder to set
   */
  public void setMetaStoreBaseFolder( String metaStoreBaseFolder ) {
    this.metaStoreBaseFolder = metaStoreBaseFolder;
  }

  /**
   * Gets project
   *
   * @return value of project
   */
  public String getProject() {
    return project;
  }

  /**
   * @param project The project to set
   */
  public void setProject( String project ) {
    this.project = project;
  }

  /**
   * Gets version
   *
   * @return value of version
   */
  public String getVersion() {
    return version;
  }

  /**
   * @param version The version to set
   */
  public void setVersion( String version ) {
    this.version = version;
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
   * Gets variables
   *
   * @return value of variables
   */
  public List<EnvironmentVariable> getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables( List<EnvironmentVariable> variables ) {
    this.variables = variables;
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

  /**
   * Gets autoSavingSpoonSession
   *
   * @return value of autoSavingSpoonSession
   */
  public boolean isAutoSavingSpoonSession() {
    return autoSavingSpoonSession;
  }

  /**
   * @param autoSavingSpoonSession The autoSavingSpoonSession to set
   */
  public void setAutoSavingSpoonSession( boolean autoSavingSpoonSession ) {
    this.autoSavingSpoonSession = autoSavingSpoonSession;
  }

  /**
   * Gets autoRestoringSpoonSession
   *
   * @return value of autoRestoringSpoonSession
   */
  public boolean isAutoRestoringHopGuiSession() {
    return autoRestoringSpoonSession;
  }

  /**
   * @param autoRestoringSpoonSession The autoRestoringSpoonSession to set
   */
  public void setAutoRestoringSpoonSession( boolean autoRestoringSpoonSession ) {
    this.autoRestoringSpoonSession = autoRestoringSpoonSession;
  }
}
