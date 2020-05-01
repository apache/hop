package org.apache.hop.env.environment;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.util.Defaults;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.util.HopDefaults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "Hop Environment",
  description = "An environment to tie together all sorts of configuration options"
)
public class Environment implements IHopMetaStoreElement<Environment> {

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

  @MetaStoreAttribute
  private String metaStoreBaseFolder;


  // Data Sets , Unit tests
  //
  @MetaStoreAttribute
  private String unitTestsBasePath;

  @MetaStoreAttribute
  private String dataSetsCsvFolder;

  @MetaStoreAttribute( key = "enforce_execution_in_environment" )
  private boolean enforcingExecutionInHome;

  // Variables
  //
  @MetaStoreAttribute
  private List<EnvironmentVariable> variables;


  public Environment() {
    variables = new ArrayList<>();
    environmentHomeFolder = "/path/to/your/environment/folder/";
    metaStoreBaseFolder = "${" + EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME + "}";
    dataSetsCsvFolder = "${" + EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME + "}/datasets";
    unitTestsBasePath = "${" + EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME + "}";
    enforcingExecutionInHome = true;
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

  public void modifyVariables( IVariables variables ) {

    if ( variables == null ) {
      variables = Variables.getADefaultVariableSpace();
    }

    // Set the name of the active environment
    //
    variables.setVariable( Defaults.VARIABLE_ACTIVE_ENVIRONMENT, Const.NVL( name, "" ) );

    if ( StringUtils.isNotEmpty( environmentHomeFolder ) ) {
      String realValue = variables.environmentSubstitute( environmentHomeFolder );
      variables.setVariable( EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME, realValue );
    }
    if ( StringUtils.isNotEmpty( metaStoreBaseFolder ) ) {
      String realValue = variables.environmentSubstitute( metaStoreBaseFolder );
      variables.setVariable( Const.HOP_METASTORE_FOLDER, realValue );
    }
    if ( StringUtils.isNotEmpty( unitTestsBasePath ) ) {
      String realValue = variables.environmentSubstitute( unitTestsBasePath );
      variables.setVariable( EnvironmentUtil.VARIABLE_UNIT_TESTS_BASE_PATH, realValue );
    }
    if ( StringUtils.isNotEmpty( dataSetsCsvFolder ) ) {
      String realValue = variables.environmentSubstitute( dataSetsCsvFolder );
      variables.setVariable( EnvironmentUtil.VARIABLE_DATASETS_BASE_PATH, realValue );
    }

    for ( EnvironmentVariable variable : this.variables ) {
      if ( variable.getName() != null ) {
        variables.setVariable( variable.getName(), variable.getValue() );
      }
    }
  }

  public String getActualHomeFolder(IVariables variables) {
    if (StringUtils.isNotEmpty(environmentHomeFolder)) {
      return variables.environmentSubstitute( environmentHomeFolder );
    } else {
      return variables.environmentSubstitute( variables.getVariable( EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME ) );
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

  @Override public MetaStoreFactory<Environment> getFactory( IMetaStore metaStore ) {
    return createFactory( metaStore );
  }

  public static final MetaStoreFactory<Environment> createFactory( IMetaStore metaStore ) {
    // Environment doesn't use the default metastore.
    try {
      return EnvironmentSingleton.getEnvironmentFactory();
    } catch(Exception e) {
      throw new RuntimeException( "Environment MetaStore configuration error", e );
    }
  }
}
