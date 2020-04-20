package org.apache.hop.pipeline.engines.remote;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.IPipelineStoppedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineComponentRowsReceived;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.www.SlaveServerPipelineStatus;
import org.apache.hop.www.SniffTransformServlet;
import org.apache.hop.www.WebResult;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class RemotePipelineEngine extends Variables implements IPipelineEngine<PipelineMeta> {
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
  protected IMetaStore metaStore;
  protected MetaStoreFactory<SlaveServer> slaveFactory;
  protected ILogChannel logChannel;
  protected ILoggingObject loggingObject;
  protected String serverObjectId;
  protected EngineMetrics engineMetrics;
  protected Result previousResult;

  protected SlaveServer slaveServer;

  protected ILoggingObject parent;
  protected IPipelineEngine parentPipeline;
  protected Workflow parentWorkflow;
  protected LogLevel logLevel;
  protected boolean feedbackShown; // TODO factor out
  protected int feedbackSize; // TODO factor out

  /**
   * A list of started listeners attached to the pipeline.
   */
  protected List<IExecutionStartedListener<PipelineMeta>> executionStartedListeners;

  /**
   * A list of finished listeners attached to the pipeline.
   */
  protected List<IExecutionFinishedListener<PipelineMeta>> executionFinishedListeners;

  /**
   * A list of stop-event listeners attached to the pipeline.
   */
  protected List<IPipelineStoppedListener> pipelineStoppedListeners;

  /**
   * The active sub-pipelines.
   */
  private Map<String, IPipelineEngine> activeSubPipelines;

  /**
   * The active subjobs
   */
  private Map<String, Workflow> activeSubWorkflows;

  protected int lastLogLineNr;
  private Timer refreshTimer;

  public RemotePipelineEngine() {
    super();
    logChannel = LogChannel.GENERAL;
    engineMetrics = new EngineMetrics();
    executionStartedListeners = Collections.synchronizedList( new ArrayList<>() );
    executionFinishedListeners = Collections.synchronizedList( new ArrayList<>() );
    pipelineStoppedListeners = Collections.synchronizedList( new ArrayList<>() );
    activeSubPipelines = new HashMap<>();
    activeSubWorkflows = new HashMap<>();
  }

  public RemotePipelineEngine( PipelineMeta subject ) {
    this();
    this.subject = subject;

    loggingObject = new LoggingObject( this );

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

      String slaveServerName = remotePipelineRunConfiguration.getSlaveServerName();
      if ( StringUtils.isEmpty( slaveServerName ) ) {
        throw new HopException( "No remote Hop server was specified to run the pipeline on" );
      }
      String remoteRunConfigurationName = remotePipelineRunConfiguration.getRunConfigurationName();
      if ( StringUtils.isEmpty( remoteRunConfigurationName ) ) {
        throw new HopException( "No run configuration was specified to the remote pipeline with" );
      }

      if ( metaStore == null ) {
        throw new HopException( "The remote pipeline engine didn't receive a metastore to slave server '" + slaveServerName + "' from" );
      }

      slaveFactory = SlaveServer.createFactory( metaStore );
      slaveServer = slaveFactory.loadElement( slaveServerName );
      if ( slaveServer == null ) {
        throw new HopException( "Slave server '" + slaveServerName + "' could not be found" );
      }

      PipelineExecutionConfiguration pipelineExecutionConfiguration = new PipelineExecutionConfiguration();
      pipelineExecutionConfiguration.setExecutingRemotely( true );
      pipelineExecutionConfiguration.setRunConfiguration( remoteRunConfigurationName );
      pipelineExecutionConfiguration.setRemoteServer( slaveServer );
      if ( logLevel != null ) {
        pipelineExecutionConfiguration.setLogLevel( logLevel );
      }
      if ( previousResult != null ) {
        pipelineExecutionConfiguration.setPreviousResult( previousResult );
      }
      serverObjectId = Pipeline.sendToSlaveServer( subject, pipelineExecutionConfiguration, metaStore );

      setReadyToStart( true );
    } catch ( Exception e ) {
      throw new HopException( "Error preparing remote pipeline", e );
    }
  }

  @Override public void startThreads() throws HopException {
    try {
      WebResult webResult = slaveServer.startPipeline( subject.getName(), serverObjectId );
      if ( WebResult.STRING_OK.equals( webResult.getResult() ) ) {
        // Inform anyone that wants to know that the show has started
        //
        fireExecutionStartedListeners();

        // So the pipeline has been successfully started.
        // That doesn't mean that the execution itself is without error
        // To know that we need to monitor the execution remotely
        // We monitor this every 5 seconds after a 1 second delay (TODO: configure this in the run configuration)
        //
        TimerTask refreshTask = new TimerTask() {
          @Override public void run() {
            getPipelineStatus();
          }
        };
        refreshTimer = new Timer();
        refreshTimer.schedule( refreshTask, 1000L, 5000L );

      } else {
        throw new HopException( "Error starting pipeline on slave server '" + slaveServer.getName() + "' with object ID '" + serverObjectId + "' : " + webResult.getMessage() );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to start pipeline on server '" + slaveServer.getName() + "'", e );
    }
  }

  private synchronized void getPipelineStatus() throws RuntimeException {
    try {
      SlaveServerPipelineStatus pipelineStatus = slaveServer.getPipelineStatus( subject.getName(), serverObjectId, lastLogLineNr );
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
          BaseTransformData.TransformExecutionStatus status = BaseTransformData.TransformExecutionStatus.getStatusFromDescription( transformStatus.getStatusDescription() );
          boolean running = status.equals( BaseTransformData.TransformExecutionStatus.STATUS_RUNNING );
          component.setRunning( running );
          boolean halted = status.equals( BaseTransformData.TransformExecutionStatus.STATUS_HALTED ) || status.equals( BaseTransformData.TransformExecutionStatus.STATUS_HALTING );
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
        logChannel.logBasic( pipelineStatus.getLoggingString() );

        // If the pipeline is finished, cancel the timer task
        //
        if ( finished ) {
          fireExecutionFinishedListeners();
          refreshTimer.cancel();
        }
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Error getting the status of pipeline '" + subject.getName() + "' on slave server '" + slaveServer.getName() + "' with object ID '" + serverObjectId + "'", e );
    }
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
    try {
      slaveServer.cleanupPipeline( subject.getName(), serverObjectId );
    } catch ( Exception e ) {
      throw new RuntimeException( "Cleanup of pipeline '" + subject.getName() + "' with ID " + serverObjectId + " failed", e );
    }
  }

  @Override public void waitUntilFinished() {
    while ( running && !stopped ) {
      try {
        Thread.sleep( 100 );
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
      slaveServer.stopPipeline( subject.getName(), serverObjectId );
    } catch ( Exception e ) {
      throw new RuntimeException( "Stopping of pipeline '" + subject.getName() + "' with ID " + serverObjectId + " failed", e );
    }
  }

  @Override public boolean hasHaltedComponents() {
    return hasHaltedComponents;
  }

  @Override public void pauseExecution() {
    try {
      slaveServer.pauseResumePipeline( subject.getName(), serverObjectId );
    } catch ( Exception e ) {
      throw new RuntimeException( "Pause/Resume of pipeline '" + subject.getName() + "' with ID " + serverObjectId + " failed", e );
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

  @Override public void retrieveComponentOutput( String componentName, int copyNr, int nrRows, IPipelineComponentRowsReceived rowsReceived ) throws HopException {
    try {
      Runnable runnable = () -> {
        try {
          String rowBufferXml = slaveServer.sniffTransform( subject.getName(), componentName, serverObjectId, "" + copyNr, nrRows, SniffTransformServlet.TYPE_OUTPUT );
          Node rowBufferNode = XmlHandler.getSubNode( XmlHandler.loadXmlString( rowBufferXml ), RowBuffer.XML_TAG );
          if ( rowBufferNode != null ) {
            RowBuffer rowBuffer = new RowBuffer( rowBufferNode );
            rowsReceived.rowsReceived( RemotePipelineEngine.this, rowBuffer );
          }
        } catch ( Exception e ) {
          throw new RuntimeException( "Unable to get output rows from transform '" + componentName + "' in pipeline '" + subject.getName() + "' on server '" + slaveServer.getName(), e );
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

  @Override public String getContainerObjectId() {
    return serverObjectId;
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


  public void addActiveSubWorkflow( final String subWorkflowName, Workflow subWorkflow ) {
    activeSubWorkflows.put( subWorkflowName, subWorkflow );
  }

  public Workflow getActiveSubWorkflow( final String subWorkflowName ) {
    return activeSubWorkflows.get( subWorkflowName );
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
  @Override public Workflow getParentWorkflow() {
    return parentWorkflow;
  }

  /**
   * @param parentWorkflow The parentWorkflow to set
   */
  public void setParentWorkflow( Workflow parentWorkflow ) {
    this.parentWorkflow = parentWorkflow;
  }

  /**
   * Gets executionStartedListeners
   *
   * @return value of executionStartedListeners
   */
  public List<IExecutionStartedListener<PipelineMeta>> getExecutionStartedListeners() {
    return executionStartedListeners;
  }

  /**
   * @param executionStartedListeners The executionStartedListeners to set
   */
  public void setExecutionStartedListeners( List<IExecutionStartedListener<PipelineMeta>> executionStartedListeners ) {
    this.executionStartedListeners = executionStartedListeners;
  }

  private void fireExecutionStartedListeners() throws HopException {
    synchronized ( executionStartedListeners ) {
      for (IExecutionStartedListener<PipelineMeta> listener : executionStartedListeners) {
        listener.started( this );
      }
    }
  }

  /**
   * Gets executionFinishedListeners
   *
   * @return value of executionFinishedListeners
   */
  public List<IExecutionFinishedListener<PipelineMeta>> getExecutionFinishedListeners() {
    return executionFinishedListeners;
  }

  /**
   * @param executionFinishedListeners The executionFinishedListeners to set
   */
  public void setExecutionFinishedListeners( List<IExecutionFinishedListener<PipelineMeta>> executionFinishedListeners ) {
    this.executionFinishedListeners = executionFinishedListeners;
  }

  private void fireExecutionFinishedListeners() throws HopException {
    synchronized ( executionFinishedListeners ) {
      for (IExecutionFinishedListener<PipelineMeta> listener : executionFinishedListeners) {
        listener.finished( this );
      }
    }
  }

  /**
   * Gets pipelineStoppedListeners
   *
   * @return value of pipelineStoppedListeners
   */
  public List<IPipelineStoppedListener> getPipelineStoppedListeners() {
    return pipelineStoppedListeners;
  }

  /**
   * @param pipelineStoppedListeners The pipelineStoppedListeners to set
   */
  public void setPipelineStoppedListeners( List<IPipelineStoppedListener> pipelineStoppedListeners ) {
    this.pipelineStoppedListeners = pipelineStoppedListeners;
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
  @Override public PipelineMeta getSubject() {
    return subject;
  }

  /**
   * @param subject The subject to set
   */
  @Override public void setSubject( PipelineMeta subject ) {
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
   * Gets metaStore
   *
   * @return value of metaStore
   */
  @Override public IMetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * @param metaStore The metaStore to set
   */
  @Override public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  /**
   * Gets slaveFactory
   *
   * @return value of slaveFactory
   */
  public MetaStoreFactory<SlaveServer> getSlaveFactory() {
    return slaveFactory;
  }

  /**
   * @param slaveFactory The slaveFactory to set
   */
  public void setSlaveFactory( MetaStoreFactory<SlaveServer> slaveFactory ) {
    this.slaveFactory = slaveFactory;
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
   * Gets slaveServer
   *
   * @return value of slaveServer
   */
  public SlaveServer getSlaveServer() {
    return slaveServer;
  }

  /**
   * @param slaveServer The slaveServer to set
   */
  public void setSlaveServer( SlaveServer slaveServer ) {
    this.slaveServer = slaveServer;
  }

  /**
   * Gets serverObjectId
   *
   * @return value of serverObjectId
   */
  public String getServerObjectId() {
    return serverObjectId;
  }

  /**
   * @param serverObjectId The serverObjectId to set
   */
  public void setServerObjectId( String serverObjectId ) {
    this.serverObjectId = serverObjectId;
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
  public Map<String, Workflow> getActiveSubWorkflows() {
    return activeSubWorkflows;
  }

  /**
   * @param activeSubWorkflows The activeSubWorkflows to set
   */
  public void setActiveSubWorkflows( Map<String, Workflow> activeSubWorkflows ) {
    this.activeSubWorkflows = activeSubWorkflows;
  }
}
