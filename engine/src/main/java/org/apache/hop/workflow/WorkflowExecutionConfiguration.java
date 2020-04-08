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

package org.apache.hop.workflow;

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
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class WorkflowExecutionConfiguration implements IExecutionConfiguration {
  public static final String XML_TAG = "job_execution_configuration";

  private final ILogChannel log = LogChannel.GENERAL;

  private boolean executingLocally;

  private boolean executingRemotely;

  private SlaveServer remoteServer;

  private Map<String, String> parametersMap;

  private Map<String, String> variablesMap;

  private boolean safeModeEnabled;

  private LogLevel logLevel;

  private boolean clearingLog;

  private Result previousResult;

  private boolean passingExport;

  private String startCopyName;

  private int startCopyNr;

  private boolean gatheringMetrics;

  private boolean expandingRemoteJob;

  private Map<String, String> extensionOptions;

  private Long passedBatchId;

  private String runConfiguration;

  public WorkflowExecutionConfiguration() {
    executingLocally = true;
    executingRemotely = false;
    passingExport = false;

    parametersMap = new HashMap<>();
    variablesMap = new HashMap<>();
    extensionOptions = new HashMap<>();

    logLevel = LogLevel.BASIC;

    clearingLog = true;
  }

  public Object clone() {
    try {
      WorkflowExecutionConfiguration configuration = (WorkflowExecutionConfiguration) super.clone();

      configuration.parametersMap = new HashMap<>();
      configuration.parametersMap.putAll( parametersMap );

      configuration.variablesMap = new HashMap<>();
      configuration.variablesMap.putAll( variablesMap );

      if ( previousResult != null ) {
        configuration.previousResult = previousResult.clone();
      }

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

  public void getUsedVariables( WorkflowMeta workflowMeta ) {
    Properties sp = new Properties();
    IVariables variables = Variables.getADefaultVariableSpace();

    String[] keys = variables.listVariables();
    for ( int i = 0; i < keys.length; i++ ) {
      sp.put( keys[ i ], variables.getVariable( keys[ i ] ) );
    }

    List<String> vars = workflowMeta.getUsedVariables();
    if ( vars != null && vars.size() > 0 ) {
      HashMap<String, String> newVariables = new HashMap<>();

      for ( int i = 0; i < vars.size(); i++ ) {
        String varname = vars.get( i );
        if ( !varname.startsWith( Const.INTERNAL_VARIABLE_PREFIX ) ) {
          // add all new non-internal variables to newVariablesMap
          newVariables.put( varname, Const.NVL( variablesMap.get( varname ), sp.getProperty( varname, "" ) ) );
        }
      }
      // variables.clear();
      variablesMap.putAll( newVariables );
    }

    // Also add the internal workflow variables if these are set...
    //
    for ( String variableName : Const.INTERNAL_WORKFLOW_VARIABLES ) {
      String value = workflowMeta.getVariable( variableName );
      if ( !Utils.isEmpty( value ) ) {
        variablesMap.put( variableName, value );
      }
    }
  }

  /**
   * @return the usingSafeMode
   */
  public boolean isSafeModeEnabled() {
    return safeModeEnabled;
  }

  /**
   * @param usingSafeMode the usingSafeMode to set
   */
  public void setSafeModeEnabled( boolean usingSafeMode ) {
    this.safeModeEnabled = usingSafeMode;
  }

  @Override public String getRunConfiguration() {
    return runConfiguration;
  }

  @Override public void setRunConfiguration( String runConfiguration ) {
    this.runConfiguration = runConfiguration;
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

  public String getXml() throws IOException {
    StringBuilder xml = new StringBuilder( 160 );

    xml.append( "  <" + XML_TAG + ">" ).append( Const.CR );

    xml.append( "    " ).append( XmlHandler.addTagValue( "exec_local", executingLocally ) );

    xml.append( "    " ).append( XmlHandler.addTagValue( "exec_remote", executingRemotely ) );
    if ( remoteServer != null ) {
      xml.append( "    " ).append( remoteServer.getXml() ).append( Const.CR );
    }
    xml.append( "    " ).append( XmlHandler.addTagValue( "pass_export", passingExport ) );

    // Serialize the parameters...
    //
    xml.append( "    <parameters>" ).append( Const.CR );
    List<String> paramNames = new ArrayList<>( parametersMap.keySet() );
    Collections.sort( paramNames );
    for ( String name : paramNames ) {
      String value = parametersMap.get( name );
      xml.append( "    <parameter>" );
      xml.append( XmlHandler.addTagValue( "name", name, false ) );
      xml.append( XmlHandler.addTagValue( "value", value, false ) );
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
      xml.append( XmlHandler.addTagValue( "name", name, false ) );
      xml.append( XmlHandler.addTagValue( "value", value, false ) );
      xml.append( "</variable>" ).append( Const.CR );
    }
    xml.append( "    </variables>" ).append( Const.CR );

    xml.append( "    " ).append( XmlHandler.addTagValue( "safe_mode", safeModeEnabled ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "log_level", logLevel.getCode() ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "clear_log", clearingLog ) );

    xml.append( "    " ).append( XmlHandler.addTagValue( "start_copy_name", startCopyName ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "start_copy_nr", startCopyNr ) );

    xml.append( "    " ).append( XmlHandler.addTagValue( "gather_metrics", gatheringMetrics ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "expand_remote_job", expandingRemoteJob ) );
    if ( passedBatchId != null ) {
      xml.append( "    " ).append( XmlHandler.addTagValue( "passedBatchId", passedBatchId ) );
    }

    // The source rows...
    //
    if ( previousResult != null ) {
      xml.append( previousResult.getXml() );
    }

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );
    return xml.toString();
  }

  public WorkflowExecutionConfiguration( Node trecNode ) throws HopException {
    this();

    executingLocally = "Y".equalsIgnoreCase( XmlHandler.getTagValue( trecNode, "exec_local" ) );

    executingRemotely = "Y".equalsIgnoreCase( XmlHandler.getTagValue( trecNode, "exec_remote" ) );
    Node remoteHostNode = XmlHandler.getSubNode( trecNode, SlaveServer.XML_TAG );
    if ( remoteHostNode != null ) {
      remoteServer = new SlaveServer( remoteHostNode );
    }
    passingExport = "Y".equalsIgnoreCase( XmlHandler.getTagValue( trecNode, "pass_export" ) );
    expandingRemoteJob = "Y".equalsIgnoreCase( XmlHandler.getTagValue( trecNode, "expand_remote_job" ) );

    // Read the variables...
    //
    Node varsNode = XmlHandler.getSubNode( trecNode, "variables" );
    int nrVariables = XmlHandler.countNodes( varsNode, "variable" );
    for ( int i = 0; i < nrVariables; i++ ) {
      Node argNode = XmlHandler.getSubNodeByNr( varsNode, "variable", i );
      String name = XmlHandler.getTagValue( argNode, "name" );
      String value = XmlHandler.getTagValue( argNode, "value" );
      if ( !Utils.isEmpty( name ) && !Utils.isEmpty( value ) ) {
        variablesMap.put( name, value );
      }
    }

    // Read the parameters...
    //
    Node parmsNode = XmlHandler.getSubNode( trecNode, "parameters" );
    int nrParams = XmlHandler.countNodes( parmsNode, "parameter" );
    for ( int i = 0; i < nrParams; i++ ) {
      Node parmNode = XmlHandler.getSubNodeByNr( parmsNode, "parameter", i );
      String name = XmlHandler.getTagValue( parmNode, "name" );
      String value = XmlHandler.getTagValue( parmNode, "value" );
      if ( !Utils.isEmpty( name ) ) {
        parametersMap.put( name, value );
      }
    }

    safeModeEnabled = "Y".equalsIgnoreCase( XmlHandler.getTagValue( trecNode, "safe_mode" ) );
    logLevel = LogLevel.getLogLevelForCode( XmlHandler.getTagValue( trecNode, "log_level" ) );
    clearingLog = "Y".equalsIgnoreCase( XmlHandler.getTagValue( trecNode, "clear_log" ) );

    startCopyName = XmlHandler.getTagValue( trecNode, "start_copy_name" );
    startCopyNr = Const.toInt( XmlHandler.getTagValue( trecNode, "start_copy_nr" ), 0 );

    gatheringMetrics = "Y".equalsIgnoreCase( XmlHandler.getTagValue( trecNode, "gather_metrics" ) );

    String sPassedBatchId = XmlHandler.getTagValue( trecNode, "passedBatchId" );
    if ( !StringUtils.isEmpty( sPassedBatchId ) ) {
      passedBatchId = Long.parseLong( sPassedBatchId );
    }

    Node resultNode = XmlHandler.getSubNode( trecNode, Result.XML_TAG );
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
   * @return the startCopyName
   */
  public String getStartCopyName() {
    return startCopyName;
  }

  /**
   * @param startCopyName the startCopyName to set
   */
  public void setStartCopyName( String startCopyName ) {
    this.startCopyName = startCopyName;
  }

  /**
   * @return the startCopyNr
   */
  public int getStartCopyNr() {
    return startCopyNr;
  }

  /**
   * @param startCopyNr the startCopyNr to set
   */
  public void setStartCopyNr( int startCopyNr ) {
    this.startCopyNr = startCopyNr;
  }

  /**
   * @return the gatheringMetrics
   */
  public boolean isGatheringMetrics() {
    return gatheringMetrics;
  }

  /**
   * @param gatheringMetrics the gatheringMetrics to set
   */
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    this.gatheringMetrics = gatheringMetrics;
  }

  public void setExpandingRemoteJob( boolean expandingRemoteJob ) {
    this.expandingRemoteJob = expandingRemoteJob;
  }

  public boolean isExpandingRemoteJob() {
    return expandingRemoteJob;
  }

  public Map<String, String> getExtensionOptions() {
    return extensionOptions;
  }

  public void setExtensionOptions( Map<String, String> extensionOptions ) {
    this.extensionOptions = extensionOptions;
  }

  public Long getPassedBatchId() {
    return passedBatchId;
  }

  public void setPassedBatchId( Long passedBatchId ) {
    this.passedBatchId = passedBatchId;
  }
}
