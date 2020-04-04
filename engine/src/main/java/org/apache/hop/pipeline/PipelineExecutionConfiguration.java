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

package org.apache.hop.pipeline;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.IExecutionConfiguration;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XMLHandler;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PipelineExecutionConfiguration implements IExecutionConfiguration {
  public static final String XML_TAG = "pipeline_execution_configuration";

  private final ILogChannel log = LogChannel.GENERAL;

  private boolean executingLocally;

  private boolean executingRemotely;
  private SlaveServer remoteServer;
  private boolean passingExport;

  private Map<String, String> parametersMap;
  private Map<String, String> variablesMap;

  private LogLevel logLevel;
  private boolean clearingLog;

  private Result previousResult;

  private boolean showingSubComponents;
  private boolean setLogfile;
  private boolean setAppendLogfile;
  private String logFileName;
  private boolean createParentFolder;
  private Long passedBatchId;

  private String runConfiguration;
  private boolean logRemoteExecutionLocally;

  public PipelineExecutionConfiguration() {
    executingLocally = true;

    passingExport = false;

    parametersMap = new HashMap<>();
    variablesMap = new HashMap<>();

    logLevel = LogLevel.BASIC;

    clearingLog = true;

    showingSubComponents = true;
  }

  public Object clone() {
    try {
      PipelineExecutionConfiguration configuration = (PipelineExecutionConfiguration) super.clone();

      configuration.parametersMap = new HashMap<>();
      configuration.parametersMap.putAll( parametersMap );

      configuration.variablesMap = new HashMap<>();
      configuration.variablesMap.putAll( variablesMap );

      return configuration;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * @param parametersMap the parameters to set
   */
  public void setParametersMap( Map<String, String> parametersMap ) {
    this.parametersMap = parametersMap;
  }

  /**
   * @return the parameters.
   */
  public Map<String, String> getParametersMap() {
    return parametersMap;
  }

  /**
   * @return the variables
   */
  public Map<String, String> getVariablesMap() {
    return variablesMap;
  }

  /**
   * @param variablesMap the variables to set
   */
  public void setVariablesMap( Map<String, String> variablesMap ) {
    this.variablesMap = variablesMap;
  }

  public void setVariablesMap( IVariables variablesMap ) {
    this.variablesMap = new HashMap<>();

    for ( String name : variablesMap.listVariables() ) {
      String value = variablesMap.getVariable( name );
      this.variablesMap.put( name, value );
    }
  }

  /**
   * @return the remoteExecution
   */
  public boolean isExecutingRemotely() {
    return executingRemotely;
  }

  /**
   * @param remoteExecution the remoteExecution to set
   */
  public void setExecutingRemotely( boolean remoteExecution ) {
    this.executingRemotely = remoteExecution;
  }


  /**
   * @return the localExecution
   */
  public boolean isExecutingLocally() {
    return executingLocally;
  }

  /**
   * @param localExecution the localExecution to set
   */
  public void setExecutingLocally( boolean localExecution ) {
    this.executingLocally = localExecution;
  }

  /**
   * @return the remoteServer
   */
  public SlaveServer getRemoteServer() {
    return remoteServer;
  }

  /**
   * @param remoteServer the remoteServer to set
   */
  public void setRemoteServer( SlaveServer remoteServer ) {
    this.remoteServer = remoteServer;
  }

  public void getAllVariables( PipelineMeta pipelineMeta ) {
    Properties sp = new Properties();
    IVariables variables = Variables.getADefaultVariableSpace();

    String[] keys = variables.listVariables();
    for ( int i = 0; i < keys.length; i++ ) {
      sp.put( keys[ i ], variables.getVariable( keys[ i ] ) );
    }

    String[] vars = pipelineMeta.listVariables();
    if ( vars != null && vars.length > 0 ) {
      HashMap<String, String> newVariables = new HashMap<>();

      for ( int i = 0; i < vars.length; i++ ) {
        String varname = vars[ i ];
        newVariables.put( varname, Const.NVL( variablesMap.get( varname ), sp.getProperty( varname, "" ) ) );
      }
      // variables.clear();
      variablesMap.putAll( newVariables );
    }

    // Also add the internal job variables if these are set...
    //
    for ( String variableName : Const.INTERNAL_JOB_VARIABLES ) {
      String value = pipelineMeta.getVariable( variableName );
      if ( !Utils.isEmpty( value ) ) {
        variablesMap.put( variableName, value );
      }
    }
  }

  public void getUsedVariables( PipelineMeta pipelineMeta ) {
    Properties sp = new Properties();
    IVariables variables = Variables.getADefaultVariableSpace();

    String[] keys = variables.listVariables();
    for ( int i = 0; i < keys.length; i++ ) {
      sp.put( keys[ i ], variables.getVariable( keys[ i ] ) );
    }

    List<String> vars = pipelineMeta.getUsedVariables();
    if ( vars != null && vars.size() > 0 ) {
      HashMap<String, String> newVariables = new HashMap<>();

      for ( int i = 0; i < vars.size(); i++ ) {
        String varname = vars.get( i );
        if ( !varname.startsWith( Const.INTERNAL_VARIABLE_PREFIX ) ) {
          newVariables.put( varname, Const.NVL( variablesMap.get( varname ), sp.getProperty( varname, "" ) ) );
        }
      }
      // variables.clear();
      variablesMap.putAll( newVariables );
    }

    // Also add the internal job variables if these are set...
    //
    for ( String variableName : Const.INTERNAL_JOB_VARIABLES ) {
      String value = pipelineMeta.getVariable( variableName );
      if ( !Utils.isEmpty( value ) ) {
        variablesMap.put( variableName, value );
      }
    }
  }

  /**
   * @return the logLevel
   */
  public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * @param logLevel the logLevel to set
   */
  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  public String getXML() throws IOException {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "  <" + XML_TAG + ">" ).append( Const.CR );

    xml.append( "    " ).append( XMLHandler.addTagValue( "exec_local", executingLocally ) );

    xml.append( "    " ).append( XMLHandler.addTagValue( "exec_remote", executingRemotely ) );
    if ( remoteServer != null ) {
      xml.append( "    " ).append( remoteServer.getXml() ).append( Const.CR );
    }
    xml.append( "    " ).append( XMLHandler.addTagValue( "pass_export", passingExport ) );

    // Serialize the parameters...
    //
    xml.append( "    <parameters>" ).append( Const.CR );
    List<String> paramNames = new ArrayList<>( parametersMap.keySet() );
    Collections.sort( paramNames );
    for ( String name : paramNames ) {
      String value = parametersMap.get( name );
      xml.append( "    <parameter>" );
      xml.append( XMLHandler.addTagValue( "name", name, false ) );
      xml.append( XMLHandler.addTagValue( "value", value, false ) );
      xml.append( "</parameter>" ).append( Const.CR );
    }
    xml.append( "    </parameters>" ).append( Const.CR );

    // Serialize the variables...
    //
    xml.append( "    <variables>" ).append( Const.CR );
    List<String> variableNames = new ArrayList<>( variablesMap.keySet() );
    Collections.sort( variableNames );
    for ( String name : variableNames ) {
      String value = variablesMap.get( name );
      xml.append( "    <variable>" );
      xml.append( XMLHandler.addTagValue( "name", name, false ) );
      xml.append( XMLHandler.addTagValue( "value", value, false ) );
      xml.append( "</variable>" ).append( Const.CR );
    }
    xml.append( "    </variables>" ).append( Const.CR );

    // IMPORTANT remote debugging is not yet supported
    //
    // xml.append(pipelineDebugMeta.getXML());

    xml.append( "    " ).append( XMLHandler.addTagValue( "log_level", logLevel.getCode() ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "log_file", setLogfile ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "log_filename", logFileName ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "log_file_append", setAppendLogfile ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "create_parent_folder", createParentFolder ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "clear_log", clearingLog ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "show_subcomponents", showingSubComponents ) );
    if ( passedBatchId != null ) {
      xml.append( "    " ).append( XMLHandler.addTagValue( "passedBatchId", passedBatchId ) );
    }
    xml.append( "    " ).append( XMLHandler.addTagValue( "run_configuration", runConfiguration ) );

    // The source rows...
    //
    if ( previousResult != null ) {
      xml.append( previousResult.getXML() );
    }

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );
    return xml.toString();
  }

  public PipelineExecutionConfiguration( Node trecNode ) throws HopException {
    this();

    executingLocally = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "exec_local" ) );

    executingRemotely = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "exec_remote" ) );
    Node remoteHostNode = XMLHandler.getSubNode( trecNode, SlaveServer.XML_TAG );
    if ( remoteHostNode != null ) {
      remoteServer = new SlaveServer( remoteHostNode );
    }
    passingExport = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "pass_export" ) );

    // Read the variables...
    //
    Node varsNode = XMLHandler.getSubNode( trecNode, "variables" );
    int nrVariables = XMLHandler.countNodes( varsNode, "variable" );
    for ( int i = 0; i < nrVariables; i++ ) {
      Node argNode = XMLHandler.getSubNodeByNr( varsNode, "variable", i );
      String name = XMLHandler.getTagValue( argNode, "name" );
      String value = XMLHandler.getTagValue( argNode, "value" );
      if ( !Utils.isEmpty( name ) ) {
        variablesMap.put( name, Const.NVL( value, "" ) );
      }
    }

    // Read the parameters...
    //
    Node parmsNode = XMLHandler.getSubNode( trecNode, "parameters" );
    int nrParams = XMLHandler.countNodes( parmsNode, "parameter" );
    for ( int i = 0; i < nrParams; i++ ) {
      Node parmNode = XMLHandler.getSubNodeByNr( parmsNode, "parameter", i );
      String name = XMLHandler.getTagValue( parmNode, "name" );
      String value = XMLHandler.getTagValue( parmNode, "value" );
      if ( !Utils.isEmpty( name ) ) {
        parametersMap.put( name, value );
      }
    }

    // IMPORTANT: remote preview and remote debugging is NOT yet supported.
    //
    logLevel = LogLevel.getLogLevelForCode( XMLHandler.getTagValue( trecNode, "log_level" ) );
    setLogfile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "log_file" ) );
    logFileName = XMLHandler.getTagValue( trecNode, "log_filename" );
    setAppendLogfile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "log_file_append" ) );
    createParentFolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "create_parent_folder" ) );
    clearingLog = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "clear_log" ) );
    showingSubComponents = "Y".equalsIgnoreCase( XMLHandler.getTagValue( trecNode, "show_subcomponents" ) );
    String sPassedBatchId = XMLHandler.getTagValue( trecNode, "passedBatchId" );
    if ( !StringUtils.isEmpty( sPassedBatchId ) ) {
      passedBatchId = Long.parseLong( sPassedBatchId );
    }
    runConfiguration = XMLHandler.getTagValue( trecNode, "run_configuration" );

    Node resultNode = XMLHandler.getSubNode( trecNode, Result.XML_TAG );
    if ( resultNode != null ) {
      try {
        previousResult = new Result( resultNode );
      } catch ( HopException e ) {
        throw new HopException( "Unable to hydrate previous result", e );
      }
    }
  }

  /**
   * @return the previousResult
   */
  public Result getPreviousResult() {
    return previousResult;
  }

  /**
   * @param previousResult the previousResult to set
   */
  public void setPreviousResult( Result previousResult ) {
    this.previousResult = previousResult;
  }

  /**
   * @return the clearingLog
   */
  public boolean isClearingLog() {
    return clearingLog;
  }

  /**
   * @param clearingLog the clearingLog to set
   */
  public void setClearingLog( boolean clearingLog ) {
    this.clearingLog = clearingLog;
  }

  /**
   * @return the passingExport
   */
  public boolean isPassingExport() {
    return passingExport;
  }

  /**
   * @param passingExport the passingExport to set
   */
  public void setPassingExport( boolean passingExport ) {
    this.passingExport = passingExport;
  }


  /**
   * @return the showingSubComponents
   */
  public boolean isShowingSubComponents() {
    return showingSubComponents;
  }

  /**
   * @param showingSubComponents the showingSubComponents to set
   */
  public void setShowingSubComponents( boolean showingSubComponents ) {
    this.showingSubComponents = showingSubComponents;
  }

  public boolean isSetLogfile() {
    return setLogfile;
  }

  public void setSetLogfile( boolean setLogfile ) {
    this.setLogfile = setLogfile;
  }

  public boolean isSetAppendLogfile() {
    return setAppendLogfile;
  }

  public void setSetAppendLogfile( boolean setAppendLogfile ) {
    this.setAppendLogfile = setAppendLogfile;
  }

  public String getLogFileName() {
    return logFileName;
  }

  public void setLogFileName( String fileName ) {
    this.logFileName = fileName;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setCreateParentFolder( boolean createParentFolder ) {
    this.createParentFolder = createParentFolder;
  }

  public Long getPassedBatchId() {
    return passedBatchId;
  }

  public void setPassedBatchId( Long passedBatchId ) {
    this.passedBatchId = passedBatchId;
  }

  public String getRunConfiguration() {
    return runConfiguration;
  }

  public void setRunConfiguration( String runConfiguration ) {
    this.runConfiguration = runConfiguration;
  }

  public boolean isLogRemoteExecutionLocally() {
    return logRemoteExecutionLocally;
  }

  public void setLogRemoteExecutionLocally( boolean logRemoteExecutionLocally ) {
    this.logRemoteExecutionLocally = logRemoteExecutionLocally;
  }
}
