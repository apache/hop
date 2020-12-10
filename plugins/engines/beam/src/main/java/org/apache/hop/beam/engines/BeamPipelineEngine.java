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

package org.apache.hop.beam.engines;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.hop.beam.metadata.RunnerType;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.IExecutionStoppedListener;
import org.apache.hop.pipeline.Pipeline;
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
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public abstract class BeamPipelineEngine extends Variables implements IPipelineEngine<PipelineMeta> {

  /**
   * Constant specifying a filename containing XML to inject into a ZIP file created during resource export.
   */
  private final PipelineEngineCapabilities engineCapabilities;

  protected PipelineMeta pipelineMeta;
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
  protected String containerId;
  protected EngineMetrics engineMetrics;
  protected Result previousResult;

  protected ILoggingObject parent;
  protected IPipelineEngine parentPipeline;
  protected IWorkflowEngine<WorkflowMeta> parentWorkflow;
  protected LogLevel logLevel;

  protected Date executionStartDate;
  protected Date executionEndDate;

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
   * The active sub workflows
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

  private HopPipelineMetaToBeamPipelineConverter converter;
  private org.apache.beam.sdk.Pipeline beamPipeline;

  private Thread beamThread;
  private PipelineResult beamPipelineResults;
  private IBeamPipelineEngineRunConfiguration beamEngineRunConfiguration;

  public BeamPipelineEngine() {
    super();
    logChannel = LogChannel.GENERAL;
    engineMetrics = new EngineMetrics();
    executionStartedListeners = Collections.synchronizedList( new ArrayList<>() );
    executionFinishedListeners = Collections.synchronizedList( new ArrayList<>() );
    executionStoppedListeners = Collections.synchronizedList( new ArrayList<>() );
    activeSubPipelines = new HashMap<>();
    activeSubWorkflows = new HashMap<>();
    engineCapabilities = new BeamPipelineEngineCapabilities();
    extensionDataMap = Collections.synchronizedMap( new HashMap<>() );
    statusDescription = "IDLE";
  }

  public BeamPipelineEngine( PipelineMeta pipelineMeta ) {
    this();
    this.pipelineMeta = pipelineMeta;
    this.metadataProvider = pipelineMeta.getMetadataProvider();
    this.loggingObject = new LoggingObject( this );
    this.logChannel = new LogChannel( this, pipelineMeta );
    this.logLevel = this.logChannel.getLogLevel();
  }

  public BeamPipelineEngine( PipelineMeta pipelineMeta, ILoggingObject parent, IVariables variables ) {
    this();
    this.pipelineMeta = pipelineMeta;
    this.loggingObject = new LoggingObject( this );
    setParent( parent );
    initializeFrom( variables );
    copyParametersFromDefinitions( pipelineMeta );
    activateParameters(this);
  }

  @Override public abstract IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration();

  public abstract void validatePipelineRunConfigurationClass( IPipelineEngineRunConfiguration engineRunConfiguration ) throws HopException;

  @Override public void prepareExecution() throws HopException {
    ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      executionStartDate = new Date();

      // Explain to various classes in the Beam API (@see org.apache.beam.sdk.io.FileSystems)
      // what the context classloader is.
      // Set it back when we're done here.
      //
      Thread.currentThread().setContextClassLoader( this.getClass().getClassLoader() );

      setPreparing( true );
      IPipelineEngineRunConfiguration engineRunConfiguration = pipelineRunConfiguration.getEngineRunConfiguration();
      validatePipelineRunConfigurationClass( engineRunConfiguration );
      if ( !( engineRunConfiguration instanceof IBeamPipelineEngineRunConfiguration ) ) {
        throw new HopException( "A beam pipeline needs a beam pipeline engine configuration to run, not '" + pipelineRunConfiguration.getName() + "'" );
      }
      if ( metadataProvider == null ) {
        throw new HopException( "The beam pipeline engine didn't receive a metadata" );
      }

      beamEngineRunConfiguration = (IBeamPipelineEngineRunConfiguration) engineRunConfiguration;

      converter = new HopPipelineMetaToBeamPipelineConverter( this, pipelineMeta, metadataProvider, beamEngineRunConfiguration );

      beamPipeline = converter.createPipeline();

      FileSystems.setDefaultPipelineOptions( beamPipeline.getOptions() );


      // Create a new log channel when we start the action
      // It's only now that we use it
      //
      logChannel.logBasic( "Executing this pipeline using the Beam Pipeline Engine with run configuration '" + pipelineRunConfiguration.getName() + "'" );

      PipelineExecutionConfiguration pipelineExecutionConfiguration = new PipelineExecutionConfiguration();
      pipelineExecutionConfiguration.setRunConfiguration( pipelineRunConfiguration.getName() );
      if ( logLevel != null ) {
        pipelineExecutionConfiguration.setLogLevel( logLevel );
      }
      if ( previousResult != null ) {
        pipelineExecutionConfiguration.setPreviousResult( previousResult );
      }

      setRunning( false );
      setReadyToStart( true );
    } catch ( Exception e ) {
      setRunning( false );
      setReadyToStart( false );
      setStopped( true );
      setErrors( getErrors() + 1 );
      setPaused( false );
      setPreparing( false );
      throw new HopException( "Error preparing remote pipeline", e );
    } finally {
      setPreparing( false );
      Thread.currentThread().setContextClassLoader( oldContextClassLoader );
    }
  }

  private PipelineResult executePipeline( org.apache.beam.sdk.Pipeline pipeline ) throws HopException {

    RunnerType runnerType = beamEngineRunConfiguration.getRunnerType();
    switch ( runnerType ) {
      case Direct:
        return DirectRunner.fromOptions( pipeline.getOptions() ).run( pipeline );
      case Flink:
        return FlinkRunner.fromOptions( pipeline.getOptions() ).run( pipeline );
      case DataFlow:
        return DataflowRunner.fromOptions( pipeline.getOptions() ).run( pipeline );
      case Spark:
        return SparkRunner.fromOptions( pipeline.getOptions() ).run( pipeline );
      default:
        throw new HopException( "Execution on runner '" + runnerType.name() + "' is not supported yet." );
    }
  }

  @Override public void startThreads() throws HopException {
    ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Explain to various classes in the Beam API (@see org.apache.beam.sdk.io.FileSystems)
      // what the context classloader is.
      // Set it back when we're done here.
      //
      Thread.currentThread().setContextClassLoader( this.getClass().getClassLoader() );

      setRunning( true );
      setReadyToStart( false );

      if ( beamEngineRunConfiguration.isRunningAsynchronous() ) {
        // Certain runners like Direct and DataFlow allow async execution
        //
        try {
          beamPipelineResults = executePipeline( beamPipeline );
        } catch ( Throwable e ) {
          // Reset the flags so the user can correct and retry
          //
          setRunning( false );
          setStopped( true );
          setPreparing( false );
          setPaused( false );
          setReadyToStart( false );
          setErrors( getErrors() + 1 );

          throw new HopException( "Error starting the Beam pipeline", e );
        }
        firePipelineExecutionStartedListeners();

      } else {
        // The running pipeline will block
        //
        try {
          beamThread = new Thread( () -> {

            try {
              beamPipelineResults = executePipeline( beamPipeline );
            } catch ( Throwable e ) {
              throw new RuntimeException( "Error starting the Beam pipeline", e );
            }

            try {
              firePipelineExecutionFinishedListeners();
            } catch ( HopException e ) {
              throw new RuntimeException( "Error firing pipeline finished listeners in a Beam pipeline engine", e );
            }
          } );
          beamThread.start();

          // Keep track of when this thread is done...
          //
          new Thread( () -> {
            try {
              beamThread.join();
              firePipelineExecutionFinishedListeners();
              populateEngineMetrics(); // get the final state
              if (refreshTimer!=null) {
                refreshTimer.cancel(); // no more needed
              }
              setRunning( false );
              executionEndDate = new Date();
            } catch ( Exception e ) {
              throw new RuntimeException( "Error post-processing a beam pipeline", e );
            }
          } ).start();

        } catch ( Exception e ) {
          // Reset the flags so the user can correct and retry
          //
          setRunning( false );
          setStopped( true );
          setPreparing( false );
          setPaused( false );
          setReadyToStart( false );
          setErrors( getErrors() + 1 );

          throw new HopException( "Unable to start Beam pipeline", e );
        }
      }

      // We have stuff running in the background, let's keep track of the progress regularly
      //
      refreshTimer = new Timer();
      refreshTimer.schedule( new TimerTask() {
        @Override public void run() {
          try {
            populateEngineMetrics();
          } catch ( Exception e ) {
            throw new RuntimeException( "Error refreshing engine metrics in the Beam pipeline engine", e );
          }
        }
      }, 0L, 1000L );

    } finally {
      Thread.currentThread().setContextClassLoader( oldContextClassLoader );
    }
  }

  /**
   * Grab the Beam pipeline results and convert it into engine metrics
   */
  protected synchronized void populateEngineMetrics() throws HopException {
    ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( this.getClass().getClassLoader() );

      EngineMetrics em = new EngineMetrics();
      evaluatePipelineStatus();

      em.setStartDate( getExecutionStartDate() );
      em.setEndDate( getExecutionEndDate() );

      if ( beamPipelineResults != null ) {
        Set<String> transformNames = new HashSet<>( Arrays.asList( pipelineMeta.getTransformNames() ) );
        Map<String, EngineComponent> componentsMap = new HashMap<>();
        MetricResults metrics = beamPipelineResults.metrics();
        MetricQueryResults allResults = metrics.queryMetrics( MetricsFilter.builder().build() );

        for ( MetricResult<Long> result : allResults.getCounters() ) {
          String metricsType = result.getName().getNamespace();
          String metricsName = result.getName().getName();
          long processed = result.getAttempted();

          // This is a transform executing in Beam
          //
          if ( transformNames.contains( metricsName ) ) {
            EngineComponent engineComponent = componentsMap.get( metricsName );
            if ( engineComponent == null ) {
              engineComponent = new EngineComponent( metricsName, 0 );
              componentsMap.put( metricsName, engineComponent );
            }
            if ( Pipeline.METRIC_NAME_READ.equalsIgnoreCase( metricsType ) ) {
              engineComponent.setLinesRead( processed );
              em.setComponentMetric( engineComponent, Pipeline.METRIC_READ, processed );
            } else if ( Pipeline.METRIC_NAME_WRITTEN.equalsIgnoreCase( metricsType ) ) {
              engineComponent.setLinesWritten( processed );
              em.setComponentMetric( engineComponent, Pipeline.METRIC_WRITTEN, processed );
            } else if ( Pipeline.METRIC_NAME_INPUT.equalsIgnoreCase( metricsType ) ) {
              engineComponent.setLinesInput( processed );
              em.setComponentMetric( engineComponent, Pipeline.METRIC_INPUT, processed );
            } else if ( Pipeline.METRIC_NAME_OUTPUT.equalsIgnoreCase( metricsType ) ) {
              engineComponent.setLinesOutput( processed );
              em.setComponentMetric( engineComponent, Pipeline.METRIC_OUTPUT, processed );
            } else if ( Pipeline.METRIC_NAME_INIT.equalsIgnoreCase( metricsType ) ) {
              em.setComponentMetric( engineComponent, Pipeline.METRIC_INIT, processed );
            } else if ( Pipeline.METRIC_NAME_FLUSH_BUFFER.equalsIgnoreCase( metricsType ) ) {
              em.setComponentMetric( engineComponent, Pipeline.METRIC_FLUSH_BUFFER, processed );
            }

            // Copy the execution start and end date from the pipeline
            //
            engineComponent.setExecutionStartDate( getExecutionStartDate() );
            engineComponent.setExecutionEndDate( getExecutionEndDate() );
            engineComponent.setExecutionDuration( calculateDuration( getExecutionStartDate(), getExecutionEndDate() ) );

            // Set the transform status to reflect the pipeline status.
            //
            switch ( beamPipelineResults.getState() ) {
              case DONE:
                engineComponent.setRunning( false );
                engineComponent.setStatus( ComponentExecutionStatus.STATUS_FINISHED );
                break;
              case CANCELLED:
              case FAILED:
              case STOPPED:
                engineComponent.setStopped( true );
                engineComponent.setRunning( false );
                engineComponent.setStatus( ComponentExecutionStatus.STATUS_STOPPED );
                break;
              case RUNNING:
                engineComponent.setRunning( true );
                engineComponent.setStopped( false );
                engineComponent.setStatus( ComponentExecutionStatus.STATUS_RUNNING );
                break;
              case UNKNOWN:
                break;
              case UPDATED:
                break;
              default:
                break;
            }
          }
        }

        em.getComponents().clear();
        em.getComponents().addAll( componentsMap.values() );
      }

      // Swap the engine metrics with the new value
      //
      synchronized ( engineMetrics ) {
        engineMetrics = em;
      }
    } finally {
      Thread.currentThread().setContextClassLoader( oldContextClassLoader );
    }
  }

  protected long calculateDuration( Date startTime, Date stopTime ) {
    long lapsed;
    if ( startTime != null && stopTime == null ) {
      Calendar cal = Calendar.getInstance();
      long now = cal.getTimeInMillis();
      long st = startTime.getTime();
      lapsed = now - st;
    } else if ( startTime != null && stopTime != null ) {
      lapsed = stopTime.getTime() - startTime.getTime();
    } else {
      lapsed = 0;
    }

    return lapsed;
  }

  protected synchronized void evaluatePipelineStatus() throws HopException {
    if ( beamPipelineResults == null || beamPipelineResults.getState()==null ) {
      statusDescription = "";
      return;
    }
    beamPipelineResults.getState().name();
    boolean cancelPipeline = false;
    switch ( beamPipelineResults.getState() ) {
      case DONE:
        if ( isRunning() ) {
          // First time we've hit this:
          setRunning( false );
          executionEndDate = new Date();
          if ( beamEngineRunConfiguration.isRunningAsynchronous() ) {
            firePipelineExecutionFinishedListeners();
          }
          logChannel.logBasic( "Beam pipeline execution has finished." );
        }
        setStatus( ComponentExecutionStatus.STATUS_FINISHED );
        break;
      case STOPPED:
      case CANCELLED:
        if ( !isStopped() ) {
          firePipelineExecutionStoppedListeners();
          if ( refreshTimer != null ) {
            refreshTimer.cancel();
          }
        }
        setStopped( true );
        setRunning( false );
        setStatus( ComponentExecutionStatus.STATUS_STOPPED );
        cancelPipeline = true;
        break;
      case FAILED:
        setStopped( true );
        setFinished( true );
        logChannel.logBasic( "Beam pipeline execution failed." );
        cancelPipeline = true;
        break;
      case UNKNOWN:
        break;
      case UPDATED:
      case RUNNING:
        setRunning( true );
        setStopped( false );
        break;
      default:
        break;
    }

    if ( cancelPipeline ) {
      try {
        beamPipelineResults.cancel();
        logChannel.logBasic( "Pipeline execution cancelled" );
      } catch ( Exception e ) {
        logChannel.logError( "Cancellation of pipeline failed", e );
      }
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
      if ( beamPipelineResults != null ) {
        beamPipelineResults.cancel();
        evaluatePipelineStatus();
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Stopping of pipeline '" + pipelineMeta.getName() + "' failed", e );
    }
  }

  @Override public boolean hasHaltedComponents() {
    return hasHaltedComponents;
  }

  @Override public void pauseExecution() {
    // Not supported
  }

  @Override public void resumeExecution() {
    // Not supported
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
      // We ignore CopyNr since it's the number of copies ever started in the Beam pipeline
      // So in essence the metrics are always just for "one" transform even though there might be hundreds of copies.
      //
      if ( component.getName().equalsIgnoreCase( name )) {
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
    throw new HopException( "Retrieving component output is not supported by the Beam pipeline engine" );
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
    return pipelineMeta.getName();
  }

  @Override public String getFilename() {
    return pipelineMeta.getFilename();
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
    return pipelineMeta;
  }

  /**
   * @param subject The subject to set
   */
  @Override public void setPipelineMeta( PipelineMeta subject ) {
    this.pipelineMeta = subject;
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
   * Gets serverObjectId
   *
   * @return value of serverObjectId
   */
  @Override
  public String getContainerId() {
    return containerId;
  }

  /**
   * @param containerId The serverObjectId to set
   */
  @Override
  public void setContainerId( String containerId ) {
    this.containerId = containerId;
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

  @Override
  public void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException {
    namedParams.addParameterDefinition( key, defValue, description );
  }

  @Override
  public String getParameterDescription( String key ) throws UnknownParamException {
    return namedParams.getParameterDescription( key );
  }

  @Override
  public String getParameterDefault( String key ) throws UnknownParamException {
    return namedParams.getParameterDefault( key );
  }

  @Override
  public String getParameterValue( String key ) throws UnknownParamException {
    return namedParams.getParameterValue( key );
  }

  @Override
  public String[] listParameters() {
    return namedParams.listParameters();
  }

  @Override
  public void setParameterValue( String key, String value ) throws UnknownParamException {
    namedParams.setParameterValue( key, value );
  }

  @Override
  public void removeAllParameters() {
    namedParams.removeAllParameters();
  }

  @Override
  public void clearParameterValues() {
    namedParams.clearParameterValues();
  }

  @Override public void copyParametersFromDefinitions( INamedParameterDefinitions definitions ) {
    namedParams.copyParametersFromDefinitions( definitions );
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

  @Override public boolean isFeedbackShown() {
    return false;
  }

  @Override public int getFeedbackSize() {
    return 0;
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
   * @param extensionDataMap The extensionDataMap to set
   */
  public void setExtensionDataMap( Map<String, Object> extensionDataMap ) {
    this.extensionDataMap = extensionDataMap;
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
   * Gets converter
   *
   * @return value of converter
   */
  public HopPipelineMetaToBeamPipelineConverter getConverter() {
    return converter;
  }

  /**
   * @param converter The converter to set
   */
  public void setConverter( HopPipelineMetaToBeamPipelineConverter converter ) {
    this.converter = converter;
  }

  /**
   * Gets beamPipeline
   *
   * @return value of beamPipeline
   */
  public org.apache.beam.sdk.Pipeline getBeamPipeline() {
    return beamPipeline;
  }

  /**
   * @param beamPipeline The beamPipeline to set
   */
  public void setBeamPipeline( org.apache.beam.sdk.Pipeline beamPipeline ) {
    this.beamPipeline = beamPipeline;
  }

  /**
   * Gets beamThread
   *
   * @return value of beamThread
   */
  public Thread getBeamThread() {
    return beamThread;
  }

  /**
   * @param beamThread The beamThread to set
   */
  public void setBeamThread( Thread beamThread ) {
    this.beamThread = beamThread;
  }

  /**
   * Gets beamPipelineResults
   *
   * @return value of beamPipelineResults
   */
  public PipelineResult getBeamPipelineResults() {
    return beamPipelineResults;
  }

  /**
   * @param beamPipelineResults The beamPipelineResults to set
   */
  public void setBeamPipelineResults( PipelineResult beamPipelineResults ) {
    this.beamPipelineResults = beamPipelineResults;
  }

  /**
   * Gets beamEngineRunConfiguration
   *
   * @return value of beamEngineRunConfiguration
   */
  public IBeamPipelineEngineRunConfiguration getBeamEngineRunConfiguration() {
    return beamEngineRunConfiguration;
  }

  /**
   * @param beamEngineRunConfiguration The beamEngineRunConfiguration to set
   */
  public void setBeamEngineRunConfiguration( IBeamPipelineEngineRunConfiguration beamEngineRunConfiguration ) {
    this.beamEngineRunConfiguration = beamEngineRunConfiguration;
  }
}
