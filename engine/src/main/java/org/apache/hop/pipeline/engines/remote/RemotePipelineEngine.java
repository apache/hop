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

package org.apache.hop.pipeline.engines.remote;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.server.HopServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.IExecutionStoppedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineComponentRowsReceived;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.resource.ResourceUtil;
import org.apache.hop.resource.TopLevelResource;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.PrepareExecutionPipelineServlet;
import org.apache.hop.www.RegisterPackageServlet;
import org.apache.hop.www.RegisterPipelineServlet;
import org.apache.hop.www.HopServerPipelineStatus;
import org.apache.hop.www.SniffTransformServlet;
import org.apache.hop.www.StartExecutionPipelineServlet;
import org.apache.hop.www.WebResult;
import org.w3c.dom.Node;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

@PipelineEnginePlugin(
  id = "Remote",
  name = "Hop remote pipeline engine",
  description = "Executes your pipeline on a remote Hop server"
)
public class RemotePipelineEngine extends Variables implements IPipelineEngine<PipelineMeta> {

  /**
   * Constant specifying a filename containing XML to inject into a ZIP file created during resource export.
   */
  public static final String CONFIGURATION_IN_EXPORT_FILENAME = "__pipeline_execution_configuration__.xml";
  private final PipelineEngineCapabilities engineCapabilities;

  protected PipelineMeta subject;
  protected String pluginId;
  protected PipelineRunConfiguration pipelineRunConfiguration;
  protected boolean preparing;
  protected boolean readyToStart;
  protected boolean running;
  protected boolean finished;
  protected boolean stopped;
  protected boolean paused;
  protected boolean hasHaltedComponents;
  protected boolean preview;
  protected int errors;
  protected IHopMetadataProvider metadataProvider;
  protected ILogChannel logChannel;
  protected ILoggingObject loggingObject;
  protected EngineMetrics engineMetrics;
  protected Result previousResult;

  protected HopServer hopServer;

  protected ILoggingObject parent;
  protected IPipelineEngine parentPipeline;
  protected IWorkflowEngine<WorkflowMeta> parentWorkflow;
  protected LogLevel logLevel;
  protected boolean feedbackShown; // TODO factor out
  protected int feedbackSize; // TODO factor out

  protected String containerId;

  /**
   * A list of started listeners attached to the pipeline.
   */
  protected List<IExecutionStartedListener<IPipelineEngine<PipelineMeta>>> executionStartedListeners;

  /**
   * A list of finished listeners attached to the pipeline.
   */
  protected List<IExecutionFinishedListener<IPipelineEngine<PipelineMeta>>> executionFinishedListeners;

  /**
   * A list of stop-event listeners attached to the pipeline.
   */
  protected List<IExecutionStoppedListener<IPipelineEngine<PipelineMeta>>> executionStoppedListeners;

  /**
   * The active sub-pipelines.
   */
  protected Map<String, IPipelineEngine> activeSubPipelines;

  /**
   * The active subjobs
   */
  protected Map<String, IWorkflowEngine<WorkflowMeta>> activeSubWorkflows;

  protected Map<String, Object> extensionDataMap;

  protected int lastLogLineNr;
  protected Timer refreshTimer;

  /**
   * The named parameters.
   */
  protected INamedParameters namedParams = new NamedParameters();
  private String statusDescription;
  private ComponentExecutionStatus status;

  protected long serverPollDelay;
  protected long serverPollInterval;

  public RemotePipelineEngine() {
    super();
    logChannel = LogChannel.GENERAL;
    engineMetrics = new EngineMetrics();
    executionStartedListeners = Collections.synchronizedList( new ArrayList<>() );
    executionFinishedListeners = Collections.synchronizedList( new ArrayList<>() );
    executionStoppedListeners = Collections.synchronizedList( new ArrayList<>() );
    activeSubPipelines = new HashMap<>();
    activeSubWorkflows = new HashMap<>();
    engineCapabilities = new RemotePipelineEngineCapabilities();
    extensionDataMap = Collections.synchronizedMap( new HashMap<>() );
  }

  public RemotePipelineEngine( PipelineMeta subject ) {
    this();
    this.subject = subject;
  }

  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new RemotePipelineRunConfiguration();
  }

  @Override public void prepareExecution() throws HopException {
    try {
      IPipelineEngineRunConfiguration engineRunConfiguration = pipelineRunConfiguration.getEngineRunConfiguration();
      if ( !( engineRunConfiguration instanceof RemotePipelineRunConfiguration ) ) {
        throw new HopException( "The remote pipeline engine expects a remote pipeline configuration" );
      }
      RemotePipelineRunConfiguration remotePipelineRunConfiguration = (RemotePipelineRunConfiguration) pipelineRunConfiguration.getEngineRunConfiguration();

      String hopServerName = remotePipelineRunConfiguration.getHopServerName();
      if ( StringUtils.isEmpty( hopServerName ) ) {
        throw new HopException( "No remote Hop server was specified to run the pipeline on" );
      }
      String remoteRunConfigurationName = remotePipelineRunConfiguration.getRunConfigurationName();
      if ( StringUtils.isEmpty( remoteRunConfigurationName ) ) {
        throw new HopException( "No run configuration was specified to the remote pipeline with" );
      }
      if ( metadataProvider == null ) {
        throw new HopException( "The remote pipeline engine didn't receive a metadata to load hop server '" + hopServerName + "'" );
      }

      // Create a new log channel when we start the action
      // It's only now that we use it
      //
      this.logChannel = new LogChannel( this, subject );
      loggingObject = new LoggingObject( this );
      this.logChannel.setLogLevel( logLevel );

      logChannel.logBasic( "Executing this pipeline using the Remote Pipeline Engine with run configuration '" + pipelineRunConfiguration.getName() + "'" );

      serverPollDelay = Const.toLong( resolve( remotePipelineRunConfiguration.getServerPollDelay() ), 1000L );
      serverPollInterval = Const.toLong( resolve( remotePipelineRunConfiguration.getServerPollInterval() ), 2000L );

      hopServer = metadataProvider.getSerializer( HopServer.class ).load( hopServerName );
      if ( hopServer == null ) {
        throw new HopException( "Hop server '" + hopServerName + "' could not be found" );
      }

      PipelineExecutionConfiguration pipelineExecutionConfiguration = new PipelineExecutionConfiguration();
      pipelineExecutionConfiguration.setRunConfiguration( remoteRunConfigurationName );
      if ( logLevel != null ) {
        pipelineExecutionConfiguration.setLogLevel( logLevel );
      }
      if ( previousResult != null ) {
        pipelineExecutionConfiguration.setPreviousResult( previousResult );
      }

      sendToHopServer( subject, pipelineExecutionConfiguration, metadataProvider );

      setReadyToStart( true );
    } catch ( Exception e ) {
      throw new HopException( "Error preparing remote pipeline", e );
    }
  }

  /**
   * Send the pipeline for execution to a HopServer hop server.
   *
   * @param pipelineMeta           the pipeline meta-data
   * @param executionConfiguration the pipeline execution configuration
   * @throws HopException if any errors occur during the dispatch to the hop server
   */
  private void sendToHopServer( PipelineMeta pipelineMeta, PipelineExecutionConfiguration executionConfiguration, IHopMetadataProvider metadataProvider ) throws HopException {

    if ( hopServer == null ) {
      throw new HopException( "No remote server specified" );
    }
    if ( Utils.isEmpty( pipelineMeta.getName() ) ) {
      throw new HopException( "The pipeline needs a name to uniquely identify it by on the remote server." );
    }

    // Inject certain internal variables to make it more intuitive.
    //
    Map<String, String> vars = new HashMap<>();

    for ( String var : Const.INTERNAL_PIPELINE_VARIABLES ) {
      vars.put( var, getVariable( var ) );
    }
    for ( String var : Const.INTERNAL_WORKFLOW_VARIABLES ) {
      vars.put( var, getVariable( var ) );
    }
    // Overwrite with all the other variables we know off
    //
    for ( String var : getVariableNames() ) {
      vars.put( var, getVariable( var ) );
    }

    executionConfiguration.getVariablesMap().putAll( vars );

    hopServer.getLogChannel().setLogLevel( executionConfiguration.getLogLevel() );

    try {
      if ( executionConfiguration.isPassingExport() ) {

        // First export the workflow...
        //
        FileObject tempFile = HopVfs.createTempFile( "pipelineExport", HopVfs.Suffix.ZIP );

        // The executionConfiguration should not include external references here because all the resources should be
        // retrieved from the exported zip file
        // TODO: Serialize metadata objects to JSON and include it in the zip file
        //
        PipelineExecutionConfiguration clonedConfiguration = (PipelineExecutionConfiguration) executionConfiguration.clone();
        TopLevelResource topLevelResource = ResourceUtil.serializeResourceExportInterface( tempFile.getName().toString(), pipelineMeta, this,
          metadataProvider, clonedConfiguration.getXml(), CONFIGURATION_IN_EXPORT_FILENAME );

        // Send the zip file over to the hop server...
        //
        String result = hopServer.sendExport( this, topLevelResource.getArchiveName(), RegisterPackageServlet.TYPE_PIPELINE, topLevelResource.getBaseResourceName() );
        WebResult webResult = WebResult.fromXmlString( result );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error passing the exported pipeline to the remote server: "
            + Const.CR + webResult.getMessage() );
        }
        containerId = webResult.getId();
      } else {

        // Now send it off to the remote server...
        // Include the JSON of the whole content of the current metadata
        //
        SerializableMetadataProvider serializableMetadataProvider = new SerializableMetadataProvider( metadataProvider );
        String xml = new PipelineConfiguration( pipelineMeta, executionConfiguration, serializableMetadataProvider ).getXml();
        String reply = hopServer.sendXml( this, xml, RegisterPipelineServlet.CONTEXT_PATH + "/?xml=Y" );
        WebResult webResult = WebResult.fromXmlString( reply );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error posting the pipeline on the remote server: " + Const.CR
            + webResult.getMessage() );
        }
        containerId = webResult.getId();
      }

      // Prepare the pipeline
      //
      String reply = hopServer.execService( this, PrepareExecutionPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineMeta.getName(), "UTF-8" ) + "&xml=Y&id=" + containerId );
      WebResult webResult = WebResult.fromXmlString( reply );
      if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
        throw new HopException( "There was an error preparing the pipeline for execution on the remote server: " + Const.CR + webResult.getMessage() );
      }
      // Get the status right after preparation.
      //
      getPipelineStatus();
    } catch ( HopException ke ) {
      throw ke;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }


  @Override public void startThreads() throws HopException {
    try {
      // Start the pipeline
      //
      String reply = hopServer.execService( this, StartExecutionPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( subject.getName(), "UTF-8" ) + "&xml=Y&id=" + containerId );
      WebResult webResult = WebResult.fromXmlString( reply );
      if ( WebResult.STRING_OK.equals( webResult.getResult() ) ) {
        // Inform anyone that wants to know that the show has started
        //
        fireExecutionStartedListeners();

        // So the pipeline has been successfully started.
        // That doesn't mean that the execution itself is without error
        // To know that we need to monitor the execution remotely
        // We monitor this every 2 seconds after a 1 second delay (configurable)
        //
        TimerTask refreshTask = new TimerTask() {
          @Override public void run() {
            getPipelineStatus();
          }
        };
        refreshTimer = new Timer();
        refreshTimer.schedule( refreshTask, serverPollDelay, serverPollInterval );

        readyToStart = false;
        running = true;
      } else {
        throw new HopException( "Error starting pipeline on hop server '" + hopServer.getName() + "' with object ID '" + containerId + "' : " + webResult.getMessage() );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to start pipeline on server '" + hopServer.getName() + "'", e );
    }
  }

  private synchronized void getPipelineStatus() throws RuntimeException {
    try {
      HopServerPipelineStatus pipelineStatus = hopServer.getPipelineStatus( this, subject.getName(), containerId, lastLogLineNr );
      synchronized ( engineMetrics ) {
        hasHaltedComponents = false;
        engineMetrics.setStartDate( pipelineStatus.getExecutionStartDate() );
        engineMetrics.setEndDate( pipelineStatus.getExecutionEndDate() );
        engineMetrics.getComponents().clear();
        engineMetrics.getComponentRunningMap().clear();
        engineMetrics.getComponentSpeedMap().clear();
        engineMetrics.getComponentMetricsMap().clear();

        for ( TransformStatus transformStatus : pipelineStatus.getTransformStatusList() ) {
          EngineComponent component = new EngineComponent( transformStatus.getTransformName(), transformStatus.getCopy() );
          component.setErrors( transformStatus.getErrors() );
          status = ComponentExecutionStatus.getStatusFromDescription( transformStatus.getStatusDescription() );
          statusDescription = status.getDescription();
          boolean running = status == ComponentExecutionStatus.STATUS_RUNNING;
          component.setRunning( running );
          boolean halted = status == ComponentExecutionStatus.STATUS_HALTED || status == ComponentExecutionStatus.STATUS_HALTING;
          if ( halted ) {
            hasHaltedComponents = true;
          }
          engineMetrics.setComponentStatus( component, transformStatus.getStatusDescription() );
          engineMetrics.setComponentRunning( component, running );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_READ, transformStatus.getLinesRead() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_WRITTEN, transformStatus.getLinesWritten() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_INPUT, transformStatus.getLinesInput() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_OUTPUT, transformStatus.getLinesOutput() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_REJECTED, transformStatus.getLinesRejected() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_UPDATED, transformStatus.getLinesUpdated() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_ERROR, transformStatus.getErrors() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_BUFFER_IN, transformStatus.getInputBufferSize() );
          engineMetrics.setComponentMetric( component, Pipeline.METRIC_BUFFER_OUT, transformStatus.getOutputBufferSize() );
          engineMetrics.setComponentSpeed( component, transformStatus.getSpeed() );
          engineMetrics.getComponents().add( component );
        }

        running = pipelineStatus.isRunning();
        finished = pipelineStatus.isFinished();
        stopped = pipelineStatus.isStopped();
        paused = pipelineStatus.isPaused();
        errors = (int) pipelineStatus.getNrTransformErrors();

        lastLogLineNr = pipelineStatus.getLastLoggingLineNr();

        // Also pass the remote log to this log channel as BASIC logging...
        // TODO: make this configurable and split up the log lines individually so we can do a better job of this.
        // Now it's a bit garbled
        //
        if ( StringUtils.isNotEmpty( pipelineStatus.getLoggingString() ) ) {
          logChannel.logBasic( pipelineStatus.getLoggingString() );
        }

        // If the pipeline is finished, cancel the timer task
        //
        if ( finished ) {
          firePipelineExecutionFinishedListeners();
          refreshTimer.cancel();
          logChannel.logBasic( "Execution finished on a remote pipeline engine with run configuration '" + pipelineRunConfiguration.getName() + "'" );
        }
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Error getting the status of pipeline '" + subject.getName() + "' on hop server '" + hopServer.getName() + "' with object ID '" + containerId + "'", e );
    }
  }

  @Override public String getStatusDescription() {
    return statusDescription;
  }

  @Override public void execute() throws HopException {
    prepareExecution();
    startThreads();
  }

  @Override public EngineMetrics getEngineMetrics( String componentName, int copyNr ) {
    EngineMetrics em = new EngineMetrics();
    em.setStartDate( engineMetrics.getStartDate() );
    em.setEndDate( engineMetrics.getEndDate() );
    for ( IEngineComponent component : engineMetrics.getComponents() ) {
      if ( component.getName().equalsIgnoreCase( componentName ) && component.getCopyNr() == copyNr ) {
        Boolean running = engineMetrics.getComponentRunningMap().get( component );
        if ( running != null ) {
          em.setComponentRunning( component, running );
        }
        String status = engineMetrics.getComponentStatusMap().get( component );
        if ( status != null ) {
          em.setComponentStatus( component, status );
        }
        String speed = engineMetrics.getComponentSpeedMap().get( component );
        if ( speed != null ) {
          em.setComponentSpeed( component, speed );
        }
      }
    }
    return em;
  }

  @Override public void cleanup() {
  }

  @Override public void waitUntilFinished() {
    while ( ( running || paused || readyToStart ) && !( stopped || finished ) ) {
      try {
        Thread.sleep( 1000 );
      } catch ( Exception e ) {
        // ignore
      }
    }
  }

  /**
   * Stop the pipeline on the server
   */
  @Override public void stopAll() {
    try {
      hopServer.stopPipeline( this, subject.getName(), containerId );
      getPipelineStatus();
    } catch ( Exception e ) {
      throw new RuntimeException( "Stopping of pipeline '" + subject.getName() + "' with ID " + containerId + " failed", e );
    }
  }

  @Override public boolean hasHaltedComponents() {
    return hasHaltedComponents;
  }

  @Override public void pauseExecution() {
    try {
      hopServer.pauseResumePipeline( this, subject.getName(), containerId );
      getPipelineStatus();
    } catch ( Exception e ) {
      throw new RuntimeException( "Pause/Resume of pipeline '" + subject.getName() + "' with ID " + containerId + " failed", e );
    }
  }

  @Override public void resumeExecution() {
    pauseExecution();
  }

  /**
   * Adds a pipeline started listener.
   *
   * @param executionStartedListener the pipeline started listener
   */
  public void addExecutionStartedListener( IExecutionStartedListener executionStartedListener ) {
    synchronized ( executionStartedListener ) {
      executionStartedListeners.add( executionStartedListener );
    }
  }

  /**
   * Adds a pipeline finished listener.
   *
   * @param executionFinishedListener the pipeline finished listener
   */
  public void addExecutionFinishedListener( IExecutionFinishedListener executionFinishedListener ) {
    synchronized ( executionFinishedListener ) {
      executionFinishedListeners.add( executionFinishedListener );
    }
  }

  @Override public String getComponentLogText( String componentName, int copyNr ) {
    return ""; // TODO implement this
  }

  @Override public List<IEngineComponent> getComponents() {
    return engineMetrics.getComponents();
  }

  @Override public List<IEngineComponent> getComponentCopies( String name ) {
    List<IEngineComponent> copies = new ArrayList<>();
    for ( IEngineComponent component : engineMetrics.getComponents() ) {
      if ( component.getName().equalsIgnoreCase( name ) ) {
        copies.add( component );
      }
    }
    return copies;
  }

  @Override public IEngineComponent findComponent( String name, int copyNr ) {
    for ( IEngineComponent component : engineMetrics.getComponents() ) {
      if ( component.getName().equalsIgnoreCase( name ) && component.getCopyNr() == copyNr ) {
        return component;
      }
    }
    return null;
  }

  @Override public Result getResult() {
    Result result = new Result();
    result.setNrErrors( errors );
    result.setResult( errors == 0 );

    for ( IEngineComponent component : engineMetrics.getComponents() ) {

      result.setNrErrors( result.getNrErrors() + component.getErrors() );

      // For every transform metric, take the maximum amount
      //
      Long read = engineMetrics.getComponentMetric( component, Pipeline.METRIC_READ );
      result.setNrLinesRead( Math.max( result.getNrLinesRead(), read == null ? 0 : read.longValue() ) );
      Long written = engineMetrics.getComponentMetric( component, Pipeline.METRIC_WRITTEN );
      result.setNrLinesWritten( Math.max( result.getNrLinesWritten(), written == null ? 0 : written.longValue() ) );
      Long input = engineMetrics.getComponentMetric( component, Pipeline.METRIC_INPUT );
      result.setNrLinesInput( Math.max( result.getNrLinesInput(), input == null ? 0 : input.longValue() ) );
      Long output = engineMetrics.getComponentMetric( component, Pipeline.METRIC_OUTPUT );
      result.setNrLinesOutput( Math.max( result.getNrLinesOutput(), output == null ? 0 : output.longValue() ) );
      Long updated = engineMetrics.getComponentMetric( component, Pipeline.METRIC_UPDATED );
      result.setNrLinesUpdated( Math.max( result.getNrLinesUpdated(), updated == null ? 0 : updated.longValue() ) );
      Long rejected = engineMetrics.getComponentMetric( component, Pipeline.METRIC_REJECTED );
      result.setNrLinesRejected( Math.max( result.getNrLinesRejected(), rejected == null ? 0 : rejected.longValue() ) );
    }

    result.setStopped( isStopped() );
    result.setLogChannelId( getLogChannelId() );

    return result;
  }

  @Override public void retrieveComponentOutput( IVariables variables, String componentName, int copyNr, int nrRows, IPipelineComponentRowsReceived rowsReceived ) throws HopException {
    try {
      Runnable runnable = () -> {
        try {
          String rowBufferXml = hopServer.sniffTransform( this, subject.getName(), componentName, containerId, "" + copyNr, nrRows, SniffTransformServlet.TYPE_OUTPUT );
          Node rowBufferNode = XmlHandler.getSubNode( XmlHandler.loadXmlString( rowBufferXml ), RowBuffer.XML_TAG );
          if ( rowBufferNode != null ) {
            RowBuffer rowBuffer = new RowBuffer( rowBufferNode );
            rowsReceived.rowsReceived( RemotePipelineEngine.this, rowBuffer );
          }
        } catch ( Exception e ) {
          throw new RuntimeException( "Unable to get output rows from transform '" + componentName + "' in pipeline '" + subject.getName() + "' on server '" + hopServer.getName(), e );
        }
      };
      new Thread( runnable ).start();
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  @Override public Date getExecutionStartDate() {
    return engineMetrics.getStartDate();
  }

  @Override public Date getExecutionEndDate() {
    return engineMetrics.getEndDate();
  }

  @Override public boolean isSafeModeEnabled() {
    return false; // TODO: implement
  }

  @Override public IRowSet findRowSet( String fromTransformName, int fromTransformCopy, String toTransformName, int toTransformCopy ) {
    return null; // TODO factor out
  }

  // Logging object methods...
  //

  @Override public String getObjectName() {
    return subject.getName();
  }

  @Override public String getFilename() {
    return subject.getFilename();
  }

  @Override public LoggingObjectType getObjectType() {
    return LoggingObjectType.PIPELINE;
  }

  @Override public String getObjectCopy() {
    return null;
  }

  @Override public Date getRegistrationDate() {
    return null;
  }

  @Override public boolean isGatheringMetrics() {
    return logChannel != null && logChannel.isGatheringMetrics();
  }

  @Override public void setGatheringMetrics( boolean gatheringMetrics ) {
    if ( logChannel != null ) {
      logChannel.setGatheringMetrics( gatheringMetrics );
    }
  }

  @Override public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
    if ( logChannel != null ) {
      logChannel.setForcingSeparateLogging( forcingSeparateLogging );
    }
  }

  @Override public boolean isForcingSeparateLogging() {
    return logChannel == null ? false : logChannel.isForcingSeparateLogging();
  }

  @Override public String getLogChannelId() {
    return logChannel.getLogChannelId();
  }

  ////////////

  public void addActiveSubPipeline( final String subPipelineName, IPipelineEngine subPipeline ) {
    activeSubPipelines.put( subPipelineName, subPipeline );
  }

  public IPipelineEngine getActiveSubPipeline( final String subPipelineName ) {
    return activeSubPipelines.get( subPipelineName );
  }


  public void addActiveSubWorkflow( final String subWorkflowName, IWorkflowEngine<WorkflowMeta> subWorkflow ) {
    activeSubWorkflows.put( subWorkflowName, subWorkflow );
  }

  public IWorkflowEngine<WorkflowMeta> getActiveSubWorkflow( final String subWorkflowName ) {
    return activeSubWorkflows.get( subWorkflowName );
  }

  @Override public void setInternalHopVariables( IVariables var ) {
    // TODO: get rid of this method.  Internal variables should always be available.
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
   * Gets executionStartedListeners
   *
   * @return value of executionStartedListeners
   */
  public List<IExecutionStartedListener<IPipelineEngine<PipelineMeta>>> getExecutionStartedListeners() {
    return executionStartedListeners;
  }

  /**
   * @param executionStartedListeners The executionStartedListeners to set
   */
  public void setExecutionStartedListeners( List<IExecutionStartedListener<IPipelineEngine<PipelineMeta>>> executionStartedListeners ) {
    this.executionStartedListeners = executionStartedListeners;
  }

  private void fireExecutionStartedListeners() throws HopException {
    synchronized ( executionStartedListeners ) {
      for ( IExecutionStartedListener<IPipelineEngine<PipelineMeta>> listener : executionStartedListeners ) {
        listener.started( this );
      }
    }
  }

  /**
   * Gets executionFinishedListeners
   *
   * @return value of executionFinishedListeners
   */
  public List<IExecutionFinishedListener<IPipelineEngine<PipelineMeta>>> getExecutionFinishedListeners() {
    return executionFinishedListeners;
  }

  /**
   * @param executionFinishedListeners The executionFinishedListeners to set
   */
  public void setExecutionFinishedListeners( List<IExecutionFinishedListener<IPipelineEngine<PipelineMeta>>> executionFinishedListeners ) {
    this.executionFinishedListeners = executionFinishedListeners;
  }


  @Override public void addExecutionStoppedListener( IExecutionStoppedListener<IPipelineEngine<PipelineMeta>> listener ) throws HopException {
    synchronized ( executionStoppedListeners ) {
      executionStoppedListeners.add( listener );
    }
  }

  @Override public void firePipelineExecutionStartedListeners() throws HopException {
    synchronized ( executionStartedListeners ) {
      for ( IExecutionStartedListener<IPipelineEngine<PipelineMeta>> listener : executionStartedListeners ) {
        listener.started( this );
      }
    }
  }

  @Override public void firePipelineExecutionFinishedListeners() throws HopException {
    synchronized ( executionFinishedListeners ) {
      for ( IExecutionFinishedListener<IPipelineEngine<PipelineMeta>> listener : executionFinishedListeners ) {
        listener.finished( this );
      }
    }
  }

  @Override public void firePipelineExecutionStoppedListeners() throws HopException {
    synchronized ( executionStoppedListeners ) {
      for ( IExecutionStoppedListener<IPipelineEngine<PipelineMeta>> listener : executionStoppedListeners ) {
        listener.stopped( this );
      }
    }
  }

  /**
   * Gets executionStoppedListeners
   *
   * @return value of executionStoppedListeners
   */
  public List<IExecutionStoppedListener<IPipelineEngine<PipelineMeta>>> getExecutionStoppedListeners() {
    return executionStoppedListeners;
  }

  /**
   * @param executionStoppedListeners The executionStoppedListeners to set
   */
  public void setExecutionStoppedListeners( List<IExecutionStoppedListener<IPipelineEngine<PipelineMeta>>> executionStoppedListeners ) {
    this.executionStoppedListeners = executionStoppedListeners;
  }

  /**
   * Gets errors
   *
   * @return value of errors
   */
  @Override public int getErrors() {
    return errors;
  }

  /**
   * @param errors The errors to set
   */
  public void setErrors( int errors ) {
    this.errors = errors;
  }

  /**
   * Gets pipelineRunConfiguration
   *
   * @return value of pipelineRunConfiguration
   */
  @Override public PipelineRunConfiguration getPipelineRunConfiguration() {
    return pipelineRunConfiguration;
  }

  /**
   * @param pipelineRunConfiguration The pipelineRunConfiguration to set
   */
  public void setPipelineRunConfiguration( PipelineRunConfiguration pipelineRunConfiguration ) {
    this.pipelineRunConfiguration = pipelineRunConfiguration;
  }

  /**
   * Gets subject
   *
   * @return value of subject
   */
  @Override public PipelineMeta getPipelineMeta() {
    return subject;
  }

  /**
   * @param subject The subject to set
   */
  @Override public void setPipelineMeta( PipelineMeta subject ) {
    this.subject = subject;
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  @Override public String getPluginId() {
    return pluginId;
  }

  /**
   * @param pluginId The pluginId to set
   */
  @Override public void setPluginId( String pluginId ) {
    this.pluginId = pluginId;
  }

  /**
   * Gets preparing
   *
   * @return value of preparing
   */
  @Override public boolean isPreparing() {
    return preparing;
  }

  /**
   * @param preparing The preparing to set
   */
  public void setPreparing( boolean preparing ) {
    this.preparing = preparing;
  }

  /**
   * Gets readyToStart
   *
   * @return value of readyToStart
   */
  @Override public boolean isReadyToStart() {
    return readyToStart;
  }

  /**
   * @param readyToStart The readyToStart to set
   */
  public void setReadyToStart( boolean readyToStart ) {
    this.readyToStart = readyToStart;
  }

  /**
   * Gets running
   *
   * @return value of running
   */
  @Override public boolean isRunning() {
    return running;
  }

  /**
   * @param running The running to set
   */
  public void setRunning( boolean running ) {
    this.running = running;
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
  public void setStopped( boolean stopped ) {
    this.stopped = stopped;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  @Override public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
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
   * @param log The logChannel to set
   */
  public void setLogChannel( ILogChannel log ) {
    this.logChannel = log;
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
   * Gets engineMetrics
   *
   * @return value of engineMetrics
   */
  @Override public EngineMetrics getEngineMetrics() {
    return engineMetrics;
  }

  /**
   * @param engineMetrics The engineMetrics to set
   */
  public void setEngineMetrics( EngineMetrics engineMetrics ) {
    this.engineMetrics = engineMetrics;
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
  public void setFinished( boolean finished ) {
    this.finished = finished;
  }

  /**
   * Gets paused
   *
   * @return value of paused
   */
  @Override public boolean isPaused() {
    return paused;
  }

  /**
   * @param paused The paused to set
   */
  public void setPaused( boolean paused ) {
    this.paused = paused;
  }

  /**
   * Gets parent
   *
   * @return value of parent
   */
  @Override public ILoggingObject getParent() {
    return parent;
  }

  /**
   * @param parent The parent to set
   */
  @Override public void setParent( ILoggingObject parent ) {
    this.parent = parent;

    this.logChannel = new LogChannel( this, parent );
    this.logLevel = logChannel.getLogLevel();
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
   * Gets preview
   *
   * @return value of preview
   */
  @Override public boolean isPreview() {
    return preview;
  }

  /**
   * @param preview The preview to set
   */
  @Override public void setPreview( boolean preview ) {
    this.preview = preview;
  }

  /**
   * Gets hasHaltedComponents
   *
   * @return value of hasHaltedComponents
   */
  public boolean isHasHaltedComponents() {
    return hasHaltedComponents;
  }

  /**
   * @param hasHaltedComponents The hasHaltedComponents to set
   */
  public void setHasHaltedComponents( boolean hasHaltedComponents ) {
    this.hasHaltedComponents = hasHaltedComponents;
  }

  /**
   * Gets feedbackShown
   *
   * @return value of feedbackShown
   */
  @Override public boolean isFeedbackShown() {
    return feedbackShown;
  }

  /**
   * @param feedbackShown The feedbackShown to set
   */
  public void setFeedbackShown( boolean feedbackShown ) {
    this.feedbackShown = feedbackShown;
  }

  /**
   * Gets feedbackSize
   *
   * @return value of feedbackSize
   */
  @Override public int getFeedbackSize() {
    return feedbackSize;
  }

  /**
   * @param feedbackSize The feedbackSize to set
   */
  public void setFeedbackSize( int feedbackSize ) {
    this.feedbackSize = feedbackSize;
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
   * Gets loggingObject
   *
   * @return value of loggingObject
   */
  public ILoggingObject getLoggingObject() {
    return loggingObject;
  }

  /**
   * @param loggingObject The loggingObject to set
   */
  public void setLoggingObject( ILoggingObject loggingObject ) {
    this.loggingObject = loggingObject;
  }

  /**
   * Gets previousResult
   *
   * @return value of previousResult
   */
  @Override public Result getPreviousResult() {
    return previousResult;
  }

  /**
   * @param previousResult The previousResult to set
   */
  public void setPreviousResult( Result previousResult ) {
    this.previousResult = previousResult;
  }

  /**
   * Gets activeSubPipelines
   *
   * @return value of activeSubPipelines
   */
  public Map<String, IPipelineEngine> getActiveSubPipelines() {
    return activeSubPipelines;
  }

  /**
   * @param activeSubPipelines The activeSubPipelines to set
   */
  public void setActiveSubPipelines( Map<String, IPipelineEngine> activeSubPipelines ) {
    this.activeSubPipelines = activeSubPipelines;
  }

  /**
   * Gets activeSubWorkflows
   *
   * @return value of activeSubWorkflows
   */
  public Map<String, IWorkflowEngine<WorkflowMeta>> getActiveSubWorkflows() {
    return activeSubWorkflows;
  }

  /**
   * @param activeSubWorkflows The activeSubWorkflows to set
   */
  public void setActiveSubWorkflows( Map<String, IWorkflowEngine<WorkflowMeta>> activeSubWorkflows ) {
    this.activeSubWorkflows = activeSubWorkflows;
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#addParameterDefinition(java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override
  public void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException {
    namedParams.addParameterDefinition( key, defValue, description );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDescription(java.lang.String)
   */
  @Override
  public String getParameterDescription( String key ) throws UnknownParamException {
    return namedParams.getParameterDescription( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDefault(java.lang.String)
   */
  @Override
  public String getParameterDefault( String key ) throws UnknownParamException {
    return namedParams.getParameterDefault( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterValue(java.lang.String)
   */
  @Override
  public String getParameterValue( String key ) throws UnknownParamException {
    return namedParams.getParameterValue( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#listParameters()
   */
  @Override
  public String[] listParameters() {
    return namedParams.listParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#setParameterValue(java.lang.String, java.lang.String)
   */
  @Override
  public void setParameterValue( String key, String value ) throws UnknownParamException {
    namedParams.setParameterValue( key, value );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#eraseParameters()
   */
  @Override
  public void removeAllParameters() {
    namedParams.removeAllParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#clearParameters()
   */
  @Override
  public void clearParameterValues() {
    namedParams.clearParameterValues();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#activateParameters()
   */
  @Override
  public void activateParameters(IVariables variables) {
    namedParams.activateParameters( variables );
  }

  @Override public void copyParametersFromDefinitions( INamedParameterDefinitions definitions ) {
    namedParams.copyParametersFromDefinitions( definitions );
  }

  /**
   * Gets engineCapabilities
   *
   * @return value of engineCapabilities
   */
  public PipelineEngineCapabilities getEngineCapabilities() {
    return engineCapabilities;
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
   * Gets containerId
   *
   * @return value of containerId
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * @param containerId The containerId to set
   */
  @Override public void setContainerId( String containerId ) {
    this.containerId = containerId;
  }

  /**
   * Gets status
   *
   * @return value of status
   */
  public ComponentExecutionStatus getStatus() {
    return status;
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
   * @param statusDescription The statusDescription to set
   */
  public void setStatusDescription( String statusDescription ) {
    this.statusDescription = statusDescription;
  }

  /**
   * @param status The status to set
   */
  public void setStatus( ComponentExecutionStatus status ) {
    this.status = status;
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
   * @param extensionDataMap The extensionDataMap to set
   */
  public void setExtensionDataMap( Map<String, Object> extensionDataMap ) {
    this.extensionDataMap = extensionDataMap;
  }
}
