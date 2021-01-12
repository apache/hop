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

package org.apache.hop.workflow.engines.remote;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.server.HopServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.resource.ResourceUtil;
import org.apache.hop.resource.TopLevelResource;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.IDelegationListener;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePlugin;
import org.apache.hop.www.RegisterPackageServlet;
import org.apache.hop.www.RegisterWorkflowServlet;
import org.apache.hop.www.HopServerWorkflowStatus;
import org.apache.hop.www.WebResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@WorkflowEnginePlugin(
  id = "Remote",
  name = "Hop remote workflow engine",
  description = "Executes your workflow on a remote hop server"
)
public class RemoteWorkflowEngine extends Variables implements IWorkflowEngine<WorkflowMeta> {

  private static final Class<?> PKG = Workflow.class; // For Translator

  /**
   * Constant specifying a filename containing XML to inject into a ZIP file created during resource export.
   */
  public static final String CONFIGURATION_IN_EXPORT_FILENAME = "__workflow_execution_configuration__.xml";

  protected WorkflowMeta workflowMeta;
  protected String pluginId;
  protected WorkflowRunConfiguration workflowRunConfiguration;
  protected Result previousResult;
  protected Result result;
  protected IHopMetadataProvider metadataProvider;
  protected ILogChannel logChannel;
  protected LoggingObject loggingObject;
  protected LogLevel logLevel;
  protected long serverPollDelay;
  protected long serverPollInterval;
  protected HopServer hopServer;
  protected String containerId;
  protected int lastLogLineNr;
  protected boolean stopped;
  protected HopServerWorkflowStatus workflowStatus;

  protected boolean interactive;
  protected boolean finished;
  protected boolean initialized;
  protected boolean running;
  protected boolean active;
  protected String statusDescription;

  protected boolean gatheringMetrics;
  protected boolean forcingSeparateLogging;

  protected Date executionStartDate;
  protected Date executionEndDate;

  protected List<IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>>> workflowFinishedListeners;
  protected List<IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>>> workflowStartedListeners;

  protected List<IActionListener> actionListeners;

  protected List<IDelegationListener> delegationListeners;

  protected Map<ActionMeta, ActionPipeline> activeActionPipeline;

  protected Map<ActionMeta, ActionWorkflow> activeActionWorkflows;

  protected Map<String, Object> extensionDataMap;

  /**
   * The rows that were passed onto this workflow by a previous pipeline. These rows are passed onto the first workflow
   * entry in this workflow (on the result object)
   */
  private List<RowMetaAndData> sourceRows;

  /**
   * Parameters of the workflow.
   */
  private INamedParameters namedParams = new NamedParameters();

  private ActionMeta startActionMeta;

  /**
   * The workflow that's launching this (sub-) workflow. This gives us access to the whole chain, including the parent variables,
   * etc.
   */
  protected IWorkflowEngine<WorkflowMeta> parentWorkflow;

  /**
   * The parent pipeline
   */
  protected IPipelineEngine parentPipeline;

  /**
   * The parent logging interface to reference
   */
  private ILoggingObject parentLoggingObject;

  /**
   * Keep a list of the actions that were executed. org.apache.hop.core.logging.CentralLogStore.getInstance()
   */
  private WorkflowTracker workflowTracker;

  /**
   * A flat list of results in THIS workflow, in the order of execution of actions
   */
  private final LinkedList<ActionResult> actionResults = new LinkedList<>();

  public RemoteWorkflowEngine() {
    workflowStartedListeners = Collections.synchronizedList( new ArrayList<>() );
    workflowFinishedListeners = Collections.synchronizedList( new ArrayList<>() );
    actionListeners = new ArrayList<>();
    activeActionPipeline = new ConcurrentHashMap<>();
    activeActionWorkflows = new ConcurrentHashMap<>();
    extensionDataMap = new HashMap<>();
    logChannel = LogChannel.GENERAL;
    logLevel = LogLevel.BASIC;
  }

  @Override public IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration() {
    return new RemoteWorkflowRunConfiguration();
  }

  public void setInternalHopVariables() {
    if ( workflowMeta == null ) {
      Workflow.setInternalHopVariables( this, null, null );
    } else {
      Workflow.setInternalHopVariables( this, workflowMeta.getFilename(), workflowMeta.getName() );
    }
  }


  @Override public String getWorkflowName() {
    return workflowMeta == null ? null : workflowMeta.getName();
  }

  @Override public Result startExecution() {
    try {
      executionStartDate = new Date();

      // Create a new log channel when we start the action
      // It's only now that we use it
      //
      logChannel = new LogChannel( workflowMeta, parentLoggingObject, gatheringMetrics );
      loggingObject = new LoggingObject( this );
      logLevel = logChannel.getLogLevel();

      workflowTracker = new WorkflowTracker( workflowMeta );

      if ( previousResult == null ) {
        result = new Result();
      } else {
        result = previousResult;
      }

      IWorkflowEngineRunConfiguration engineRunConfiguration = workflowRunConfiguration.getEngineRunConfiguration();
      if ( !( engineRunConfiguration instanceof RemoteWorkflowRunConfiguration ) ) {
        throw new HopException( "The remote workflow engine expects a remote workflow configuration" );
      }
      RemoteWorkflowRunConfiguration remoteWorkflowRunConfiguration = (RemoteWorkflowRunConfiguration) workflowRunConfiguration.getEngineRunConfiguration();

      String hopServerName = remoteWorkflowRunConfiguration.getHopServerName();
      if ( StringUtils.isEmpty( hopServerName ) ) {
        throw new HopException( "No remote Hop server was specified to run the workflow on" );
      }
      String remoteRunConfigurationName = remoteWorkflowRunConfiguration.getRunConfigurationName();
      if ( StringUtils.isEmpty( remoteRunConfigurationName ) ) {
        throw new HopException( "No run configuration was specified to the remote workflow with" );
      }
      if ( metadataProvider == null ) {
        throw new HopException( "The remote workflow engine didn't receive a metadata to load hop server '" + hopServerName + "'" );
      }

      logChannel.logBasic( "Executing this workflow using the Remote Workflow Engine with run configuration '" + workflowRunConfiguration.getName() + "'" );

      serverPollDelay = Const.toLong( resolve( remoteWorkflowRunConfiguration.getServerPollDelay() ), 1000L );
      serverPollInterval = Const.toLong( resolve( remoteWorkflowRunConfiguration.getServerPollInterval() ), 2000L );

      hopServer = metadataProvider.getSerializer( HopServer.class ).load( hopServerName );
      if ( hopServer == null ) {
        throw new HopException( "Hop server '" + hopServerName + "' could not be found" );
      }

      WorkflowExecutionConfiguration workflowExecutionConfiguration = new WorkflowExecutionConfiguration();
      workflowExecutionConfiguration.setRunConfiguration( remoteRunConfigurationName );
      if ( logLevel != null ) {
        workflowExecutionConfiguration.setLogLevel( logLevel );
      }
      if ( previousResult != null ) {
        workflowExecutionConfiguration.setPreviousResult( previousResult );
      }
      workflowExecutionConfiguration.setGatheringMetrics( gatheringMetrics );

      // TODO: pass variables and source rows as well...
      //

      sendToHopServer( workflowMeta, workflowExecutionConfiguration, metadataProvider );
      fireWorkflowStartedListeners();

      initialized = true;

      monitorRemoteWorkflowUntilFinished();
      fireWorkflowFinishListeners();

      executionEndDate = new Date();
    } catch ( Exception e ) {
      logChannel.logError( "Error starting workflow", e );
      result.setNrErrors( result.getNrErrors() + 1 );
      try {
        fireWorkflowFinishListeners();
      } catch ( Exception ex ) {
        logChannel.logError( "Error executing workflow finished listeners", ex );
        result.setNrErrors( result.getNrErrors() + 1 );
      }
    }

    return result;
  }

  public void monitorRemoteWorkflowUntilFinished() {
    try {
      // Start with a little bit of a wait
      //
      Thread.sleep( serverPollDelay );

      while ( !stopped && !finished ) {
        getWorkflowStatus();
        Thread.sleep( serverPollInterval );
      }

    } catch ( Exception e ) {
      logChannel.logError( "Error monitoring remote workflow", e );
      result.setNrErrors( 1 );
    }
  }

  public synchronized void getWorkflowStatus() throws HopException {
    try {
      workflowStatus = hopServer.getWorkflowStatus( this, workflowMeta.getName(), containerId, lastLogLineNr );
      lastLogLineNr = workflowStatus.getLastLoggingLineNr();
      if ( StringUtils.isNotEmpty( workflowStatus.getLoggingString() ) ) {
        logChannel.logBasic( workflowStatus.getLoggingString() ); // TODO implement detailed logging and add option to log at all
      }
      finished = workflowStatus.isFinished();
      stopped = workflowStatus.isStopped();
      running = workflowStatus.isRunning();
      active = running; // TODO: differentiate
      statusDescription = workflowStatus.getStatusDescription();

      result = workflowStatus.getResult();
    } catch ( Exception e ) {
      throw new HopException( "Error getting workflow status", e );
    }
  }

  @Override public void stopExecution() {
    try {
      hopServer.stopWorkflow( this, workflowMeta.getName(), containerId );
      getWorkflowStatus();
    } catch ( Exception e ) {
      throw new RuntimeException( "Stopping of pipeline '" + workflowMeta.getName() + "' with ID " + containerId + " failed", e );
    }
  }

  /**
   * Send to hop server.
   *
   * @param workflowMeta           the workflow meta
   * @param executionConfiguration the execution configuration
   * @param metadataProvider       the metadataProvider
   * @throws HopException the hop exception
   */
  public void sendToHopServer( WorkflowMeta workflowMeta, WorkflowExecutionConfiguration executionConfiguration, IHopMetadataProvider metadataProvider ) throws HopException {

    if ( hopServer == null ) {
      throw new HopException( BaseMessages.getString( PKG, "Workflow.Log.NoHopServerSpecified" ) );
    }
    if ( Utils.isEmpty( workflowMeta.getName() ) ) {
      throw new HopException( BaseMessages.getString( PKG, "Workflow.Log.UniqueWorkflowName" ) );
    }

    // Align logging levels between execution configuration and remote server
    hopServer.getLogChannel().setLogLevel( executionConfiguration.getLogLevel() );

    try {
      // Inject certain internal variables to make it more intuitive.
      //
      for ( String var : Const.INTERNAL_PIPELINE_VARIABLES ) {
        executionConfiguration.getVariablesMap().put( var, getVariable( var ) );
      }
      for ( String var : Const.INTERNAL_WORKFLOW_VARIABLES ) {
        executionConfiguration.getVariablesMap().put( var, getVariable( var ) );
      }
      // Overwrite with all the other variables we know off
      //
      for ( String var : getVariableNames() ) {
        executionConfiguration.getVariablesMap().put( var, getVariable( var ) );
      }

      if ( executionConfiguration.isPassingExport() ) {
        // First export the workflow... hopServer.getVariable("MASTER_HOST")
        //
        FileObject tempFile = HopVfs.createTempFile( "workflowExport", ".zip", System.getProperty( "java.io.tmpdir" ) );

        TopLevelResource topLevelResource = ResourceUtil.serializeResourceExportInterface( tempFile.getName().toString(), workflowMeta, this,
          metadataProvider, executionConfiguration.getXml(), CONFIGURATION_IN_EXPORT_FILENAME );

        // Send the zip file over to the hop server...
        String result = hopServer.sendExport( this, topLevelResource.getArchiveName(), RegisterPackageServlet.TYPE_WORKFLOW, topLevelResource.getBaseResourceName() );
        WebResult webResult = WebResult.fromXmlString( result );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error passing the exported workflow to the remote server: " + Const.CR + webResult.getMessage() );
        }
        containerId = webResult.getId();
      } else {
        String xml = new WorkflowConfiguration( workflowMeta, executionConfiguration, metadataProvider ).getXml();

        String reply = hopServer.sendXml( this, xml, RegisterWorkflowServlet.CONTEXT_PATH + "/?xml=Y" );
        WebResult webResult = WebResult.fromXmlString( reply );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error posting the workflow on the remote server: " + Const.CR + webResult.getMessage() );
        }
        containerId = webResult.getId();
      }

      // Start the workflow
      //
      WebResult webResult = hopServer.startWorkflow( this, workflowMeta.getName(), containerId );
      if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
        throw new HopException( "There was an error starting the workflow on the remote server: " + Const.CR + webResult.getMessage() );
      }
    } catch ( HopException ke ) {
      throw ke;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public void addWorkflowFinishedListener( IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>> finishedListener ) {
    synchronized ( workflowFinishedListeners ) {
      workflowFinishedListeners.add( finishedListener );
    }
  }

  public void fireWorkflowFinishListeners() throws HopException {
    synchronized ( workflowFinishedListeners ) {
      for ( IExecutionFinishedListener listener : workflowFinishedListeners ) {
        listener.finished( this );
      }
    }
  }

  public void addWorkflowStartedListener( IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>> finishedListener ) {
    synchronized ( workflowStartedListeners ) {
      workflowStartedListeners.add( finishedListener );
    }
  }

  public void fireWorkflowStartedListeners() throws HopException {
    synchronized ( workflowStartedListeners ) {
      for ( IExecutionStartedListener listener : workflowStartedListeners ) {
        listener.started( this );
      }
    }
  }

  public void addActionListener( IActionListener actionListener ) {
    actionListeners.add( actionListener );
  }

  public void removeActionListener( IActionListener actionListener ) {
    actionListeners.remove( actionListener );
  }


  /**
   * Gets the registration date. For workflow, this always returns null
   *
   * @return null
   */
  public Date getRegistrationDate() {
    return null;
  }

  /**
   * Gets the log channel id.
   *
   * @return the logChannelId
   */
  public String getLogChannelId() {
    return logChannel.getLogChannelId();
  }

  /**
   * Gets LoggingObjectType.JOB, which is always the value for Workflow.
   *
   * @return LoggingObjectType LoggingObjectType.JOB
   */
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.WORKFLOW;
  }

  /**
   * Gets parent logging object.
   *
   * @return parentLoggingObject
   */
  public ILoggingObject getParent() {
    return parentLoggingObject;
  }

  /**
   * Gets the workflow name.
   *
   * @return workflowName
   */
  public String getObjectName() {
    return getWorkflowName();
  }

  /**
   * Always returns null for Workflow.
   *
   * @return null
   */
  public String getObjectCopy() {
    return null;
  }

  /**
   * Gets the file name.
   *
   * @return the filename
   */
  public String getFilename() {
    if ( workflowMeta == null ) {
      return null;
    }
    return workflowMeta.getFilename();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#addParameterDefinition(java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override public void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException {
    namedParams.addParameterDefinition( key, defValue, description );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDescription(java.lang.String)
   */
  @Override public String getParameterDescription( String key ) throws UnknownParamException {
    return namedParams.getParameterDescription( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDefault(java.lang.String)
   */
  @Override public String getParameterDefault( String key ) throws UnknownParamException {
    return namedParams.getParameterDefault( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterValue(java.lang.String)
   */
  @Override public String getParameterValue( String key ) throws UnknownParamException {
    return namedParams.getParameterValue( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#listParameters()
   */
  @Override public String[] listParameters() {
    return namedParams.listParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#setParameterValue(java.lang.String, java.lang.String)
   */
  @Override public void setParameterValue( String key, String value ) throws UnknownParamException {
    namedParams.setParameterValue( key, value );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#eraseParameters()
   */
  public void removeAllParameters() {
    namedParams.removeAllParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#clearParameters()
   */
  public void clearParameterValues() {
    namedParams.clearParameterValues();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#activateParameters()
   */
  public void activateParameters( IVariables variables ) {
    namedParams.activateParameters( variables );
  }

  @Override public void copyParametersFromDefinitions( INamedParameterDefinitions definitions ) {
    namedParams.copyParametersFromDefinitions( definitions );
  }

  /**
   * Gets workflowMeta
   *
   * @return value of workflowMeta
   */
  @Override public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  /**
   * @param workflowMeta The workflowMeta to set
   */
  @Override public void setWorkflowMeta( WorkflowMeta workflowMeta ) {
    this.workflowMeta = workflowMeta;
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  public String getPluginId() {
    return pluginId;
  }

  /**
   * @param pluginId The pluginId to set
   */
  public void setPluginId( String pluginId ) {
    this.pluginId = pluginId;
  }

  /**
   * Gets workflowRunConfiguration
   *
   * @return value of workflowRunConfiguration
   */
  public WorkflowRunConfiguration getWorkflowRunConfiguration() {
    return workflowRunConfiguration;
  }

  /**
   * @param workflowRunConfiguration The workflowRunConfiguration to set
   */
  @Override public void setWorkflowRunConfiguration( WorkflowRunConfiguration workflowRunConfiguration ) {
    this.workflowRunConfiguration = workflowRunConfiguration;
  }

  /**
   * Gets previousResult
   *
   * @return value of previousResult
   */
  public Result getPreviousResult() {
    return previousResult;
  }

  /**
   * @param previousResult The previousResult to set
   */
  public void setPreviousResult( Result previousResult ) {
    this.previousResult = previousResult;
  }

  /**
   * Gets result
   *
   * @return value of result
   */
  @Override public Result getResult() {
    return result;
  }

  /**
   * @param result The result to set
   */
  @Override public void setResult( Result result ) {
    this.result = result;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * Gets logChannel
   *
   * @return value of logChannel
   */
  @Override public ILogChannel getLogChannel() {
    return logChannel;
  }

  /**
   * @param logChannel The logChannel to set
   */
  public void setLogChannel( ILogChannel logChannel ) {
    this.logChannel = logChannel;
  }

  /**
   * Gets loggingObject
   *
   * @return value of loggingObject
   */
  public LoggingObject getLoggingObject() {
    return loggingObject;
  }

  /**
   * @param loggingObject The loggingObject to set
   */
  public void setLoggingObject( LoggingObject loggingObject ) {
    this.loggingObject = loggingObject;
  }

  /**
   * Gets logLevel
   *
   * @return value of logLevel
   */
  @Override public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * @param logLevel The logLevel to set
   */
  @Override public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  /**
   * Gets serverPollDelay
   *
   * @return value of serverPollDelay
   */
  public long getServerPollDelay() {
    return serverPollDelay;
  }

  /**
   * @param serverPollDelay The serverPollDelay to set
   */
  public void setServerPollDelay( long serverPollDelay ) {
    this.serverPollDelay = serverPollDelay;
  }

  /**
   * Gets serverPollInterval
   *
   * @return value of serverPollInterval
   */
  public long getServerPollInterval() {
    return serverPollInterval;
  }

  /**
   * @param serverPollInterval The serverPollInterval to set
   */
  public void setServerPollInterval( long serverPollInterval ) {
    this.serverPollInterval = serverPollInterval;
  }

  /**
   * Gets hopServer
   *
   * @return value of hopServer
   */
  public HopServer getHopServer() {
    return hopServer;
  }

  /**
   * @param hopServer The hopServer to set
   */
  public void setHopServer( HopServer hopServer ) {
    this.hopServer = hopServer;
  }

  /**
   * Gets serverObjectId
   *
   * @return value of serverObjectId
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * @param containerId The serverObjectId to set
   */
  public void setContainerId( String containerId ) {
    this.containerId = containerId;
  }

  /**
   * Gets lastLogLineNr
   *
   * @return value of lastLogLineNr
   */
  public int getLastLogLineNr() {
    return lastLogLineNr;
  }

  /**
   * @param lastLogLineNr The lastLogLineNr to set
   */
  public void setLastLogLineNr( int lastLogLineNr ) {
    this.lastLogLineNr = lastLogLineNr;
  }

  /**
   * Gets stopped
   *
   * @return value of stopped
   */
  @Override public boolean isStopped() {
    return stopped;
  }

  /**
   * @param stopped The stopped to set
   */
  @Override public void setStopped( boolean stopped ) {
    this.stopped = stopped;
  }

  /**
   * @param workflowStatus The workflowStatus to set
   */
  public void setWorkflowStatus( HopServerWorkflowStatus workflowStatus ) {
    this.workflowStatus = workflowStatus;
  }

  /**
   * Gets interactive
   *
   * @return value of interactive
   */
  @Override public boolean isInteractive() {
    return interactive;
  }

  /**
   * @param interactive The interactive to set
   */
  @Override public void setInteractive( boolean interactive ) {
    this.interactive = interactive;
  }

  /**
   * Gets finished
   *
   * @return value of finished
   */
  @Override public boolean isFinished() {
    return finished;
  }

  /**
   * @param finished The finished to set
   */
  @Override public void setFinished( boolean finished ) {
    this.finished = finished;
  }

  /**
   * Gets initialized
   *
   * @return value of initialized
   */
  @Override public boolean isInitialized() {
    return initialized;
  }

  /**
   * @param initialized The initialized to set
   */
  public void setInitialized( boolean initialized ) {
    this.initialized = initialized;
  }

  /**
   * Gets running
   *
   * @return value of running
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * @param running The running to set
   */
  public void setRunning( boolean running ) {
    this.running = running;
  }

  /**
   * Gets statusDescription
   *
   * @return value of statusDescription
   */
  @Override public String getStatusDescription() {
    return statusDescription;
  }

  /**
   * @param statusDescription The statusDescription to set
   */
  public void setStatusDescription( String statusDescription ) {
    this.statusDescription = statusDescription;
  }

  /**
   * Gets active
   *
   * @return value of active
   */
  @Override public boolean isActive() {
    return active;
  }

  /**
   * @param active The active to set
   */
  public void setActive( boolean active ) {
    this.active = active;
  }

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  @Override public Date getExecutionStartDate() {
    return executionStartDate;
  }

  /**
   * @param executionStartDate The executionStartDate to set
   */
  public void setExecutionStartDate( Date executionStartDate ) {
    this.executionStartDate = executionStartDate;
  }

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  @Override public Date getExecutionEndDate() {
    return executionEndDate;
  }

  /**
   * @param executionEndDate The executionEndDate to set
   */
  public void setExecutionEndDate( Date executionEndDate ) {
    this.executionEndDate = executionEndDate;
  }

  /**
   * Gets workflowFinishedListeners
   *
   * @return value of workflowFinishedListeners
   */
  @Override public List<IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>>> getWorkflowFinishedListeners() {
    return workflowFinishedListeners;
  }

  /**
   * @param workflowFinishedListeners The workflowFinishedListeners to set
   */
  public void setWorkflowFinishedListeners(
    List<IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>>> workflowFinishedListeners ) {
    this.workflowFinishedListeners = workflowFinishedListeners;
  }

  /**
   * Gets workflowStartedListeners
   *
   * @return value of workflowStartedListeners
   */
  @Override public List<IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>>> getWorkflowStartedListeners() {
    return workflowStartedListeners;
  }

  /**
   * @param workflowStartedListeners The workflowStartedListeners to set
   */
  public void setWorkflowStartedListeners(
    List<IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>>> workflowStartedListeners ) {
    this.workflowStartedListeners = workflowStartedListeners;
  }

  /**
   * Gets actionListeners
   *
   * @return value of actionListeners
   */
  @Override public List<IActionListener> getActionListeners() {
    return actionListeners;
  }

  /**
   * @param actionListeners The actionListeners to set
   */
  public void setActionListeners( List<IActionListener> actionListeners ) {
    this.actionListeners = actionListeners;
  }

  /**
   * Gets delegationListeners
   *
   * @return value of delegationListeners
   */
  public List<IDelegationListener> getDelegationListeners() {
    return delegationListeners;
  }

  /**
   * @param delegationListeners The delegationListeners to set
   */
  public void setDelegationListeners( List<IDelegationListener> delegationListeners ) {
    this.delegationListeners = delegationListeners;
  }

  /**
   * Gets activeActionPipeline
   *
   * @return value of activeActionPipeline
   */
  @Override public Map<ActionMeta, ActionPipeline> getActiveActionPipeline() {
    return activeActionPipeline;
  }

  /**
   * @param activeActionPipeline The activeActionPipeline to set
   */
  public void setActiveActionPipeline( Map<ActionMeta, ActionPipeline> activeActionPipeline ) {
    this.activeActionPipeline = activeActionPipeline;
  }

  /**
   * Gets activeActionWorkflows
   *
   * @return value of activeActionWorkflows
   */
  @Override public Map<ActionMeta, ActionWorkflow> getActiveActionWorkflows() {
    return activeActionWorkflows;
  }

  /**
   * @param activeActionWorkflows The activeActionWorkflows to set
   */
  public void setActiveActionWorkflows( Map<ActionMeta, ActionWorkflow> activeActionWorkflows ) {
    this.activeActionWorkflows = activeActionWorkflows;
  }

  /**
   * Gets namedParams
   *
   * @return value of namedParams
   */
  public INamedParameters getNamedParams() {
    return namedParams;
  }

  /**
   * @param namedParams The namedParams to set
   */
  public void setNamedParams( INamedParameters namedParams ) {
    this.namedParams = namedParams;
  }

  /**
   * Gets extensionDataMap
   *
   * @return value of extensionDataMap
   */
  @Override public Map<String, Object> getExtensionDataMap() {
    return extensionDataMap;
  }

  /**
   * @param extensionDataMap The extensionDataMap to set
   */
  public void setExtensionDataMap( Map<String, Object> extensionDataMap ) {
    this.extensionDataMap = extensionDataMap;
  }

  /**
   * Gets sourceRows
   *
   * @return value of sourceRows
   */
  public List<RowMetaAndData> getSourceRows() {
    return sourceRows;
  }

  /**
   * @param sourceRows The sourceRows to set
   */
  @Override public void setSourceRows( List<RowMetaAndData> sourceRows ) {
    this.sourceRows = sourceRows;
  }

  /**
   * Gets start action meta
   *
   * @return value of start action meta
   */
  public ActionMeta getStartActionMeta() {
    return startActionMeta;
  }

  /**
   * @param actionMeta The start action to set
   */
  @Override public void setStartActionMeta( ActionMeta actionMeta ) {
    this.startActionMeta = actionMeta;
  }

  /**
   * Gets workflowTracker
   *
   * @return value of workflowTracker
   */
  @Override public WorkflowTracker getWorkflowTracker() {
    return workflowTracker;
  }

  /**
   * @param workflowTracker The workflowTracker to set
   */
  public void setWorkflowTracker( WorkflowTracker workflowTracker ) {
    this.workflowTracker = workflowTracker;
  }

  /**
   * Gets parentWorkflow
   *
   * @return value of parentWorkflow
   */
  @Override public IWorkflowEngine<WorkflowMeta> getParentWorkflow() {
    return parentWorkflow;
  }

  /**
   * @param parentWorkflow The parentWorkflow to set
   */
  @Override public void setParentWorkflow( IWorkflowEngine<WorkflowMeta> parentWorkflow ) {
    this.parentWorkflow = parentWorkflow;
  }

  /**
   * Gets parentPipeline
   *
   * @return value of parentPipeline
   */
  @Override public IPipelineEngine getParentPipeline() {
    return parentPipeline;
  }

  /**
   * @param parentPipeline The parentPipeline to set
   */
  public void setParentPipeline( IPipelineEngine parentPipeline ) {
    this.parentPipeline = parentPipeline;
  }

  /**
   * Gets parentLoggingObject
   *
   * @return value of parentLoggingObject
   */
  public ILoggingObject getParentLoggingObject() {
    return parentLoggingObject;
  }

  /**
   * @param parentLoggingObject The parentLoggingObject to set
   */
  public void setParentLoggingObject( ILoggingObject parentLoggingObject ) {
    this.parentLoggingObject = parentLoggingObject;
  }

  /**
   * Gets actionResults
   *
   * @return value of actionResults
   */
  @Override public LinkedList<ActionResult> getActionResults() {
    return actionResults;
  }

  /**
   * Gets forcingSeparateLogging
   *
   * @return value of forcingSeparateLogging
   */
  @Override public boolean isForcingSeparateLogging() {
    return forcingSeparateLogging;
  }

  /**
   * @param forcingSeparateLogging The forcingSeparateLogging to set
   */
  @Override public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
    this.forcingSeparateLogging = forcingSeparateLogging;
  }

  /**
   * Gets gatheringMetrics
   *
   * @return value of gatheringMetrics
   */
  @Override public boolean isGatheringMetrics() {
    return gatheringMetrics;
  }

  /**
   * @param gatheringMetrics The gatheringMetrics to set
   */
  @Override public void setGatheringMetrics( boolean gatheringMetrics ) {
    this.gatheringMetrics = gatheringMetrics;
  }
}
