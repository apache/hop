//CHECKSTYLE:FileLength:OFF
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

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.BlockingBatchingRowSet;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.Counter;
import org.apache.hop.core.IExecutor;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabaseTransaction;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopPipelineException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ChannelLogTable;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogStatus;
import org.apache.hop.core.logging.LoggingHierarchy;
import org.apache.hop.core.logging.LoggingMetric;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.Metrics;
import org.apache.hop.core.logging.MetricsLogTable;
import org.apache.hop.core.logging.MetricsRegistry;
import org.apache.hop.core.logging.PerformanceLogTable;
import org.apache.hop.core.logging.PipelineLogTable;
import org.apache.hop.core.logging.TransformLogTable;
import org.apache.hop.core.metrics.MetricsDuration;
import org.apache.hop.core.metrics.IMetricsSnapshot;
import org.apache.hop.core.metrics.MetricsUtil;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.IDelegationListener;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformListener;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.resource.ResourceUtil;
import org.apache.hop.resource.TopLevelResource;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineMetric;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.engine.IFinishedListener;
import org.apache.hop.pipeline.engine.IPipelineComponentRowsReceived;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;
import org.apache.hop.pipeline.performance.PerformanceSnapShot;
import org.apache.hop.pipeline.transform.BaseTransformData.TransformExecutionStatus;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.RunThread;
import org.apache.hop.pipeline.transform.TransformAdapter;
import org.apache.hop.pipeline.transform.TransformInitThread;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transforms.mappinginput.MappingInput;
import org.apache.hop.pipeline.transforms.mappingoutput.MappingOutput;
import org.apache.hop.www.PrepareExecutionPipelineServlet;
import org.apache.hop.www.RegisterPackageServlet;
import org.apache.hop.www.RegisterPipelineServlet;
import org.apache.hop.www.SocketRepository;
import org.apache.hop.www.StartExecutionPipelineServlet;
import org.apache.hop.www.WebResult;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hop.pipeline.Pipeline.BitMaskStatus.BIT_STATUS_SUM;
import static org.apache.hop.pipeline.Pipeline.BitMaskStatus.FINISHED;
import static org.apache.hop.pipeline.Pipeline.BitMaskStatus.INITIALIZING;
import static org.apache.hop.pipeline.Pipeline.BitMaskStatus.PAUSED;
import static org.apache.hop.pipeline.Pipeline.BitMaskStatus.PREPARING;
import static org.apache.hop.pipeline.Pipeline.BitMaskStatus.RUNNING;
import static org.apache.hop.pipeline.Pipeline.BitMaskStatus.STOPPED;

/**
 * This class represents the information and operations associated with the execution of a Pipeline. It loads,
 * instantiates, initializes, runs, and monitors the execution of the pipeline contained in the specified
 * PipelineMeta object.
 *
 * @author Matt
 * @since 07-04-2003
 */

public class Pipeline implements IVariables, INamedParams, IHasLogChannel, ILoggingObject,
  IExecutor, IExtensionData, IPipelineEngine<PipelineMeta> {

  public static final String METRIC_NAME_INPUT = "input";
  public static final String METRIC_NAME_OUTPUT = "output";
  public static final String METRIC_NAME_ERROR = "error";
  public static final String METRIC_NAME_READ = "read";
  public static final String METRIC_NAME_WRITTEN = "written";
  public static final String METRIC_NAME_UPDATED = "updated";
  public static final String METRIC_NAME_REJECTED = "rejected";
  public static final String METRIC_NAME_BUFFER_IN = "buffer_in";
  public static final String METRIC_NAME_BUFFER_OUT = "buffer_out";

  /**
   * The package name, used for internationalization of messages.
   */
  private static Class<?> PKG = Pipeline.class; // for i18n purposes, needed by Translator!!

  protected String pluginId;
  protected IPipelineEngineRunConfiguration pipelineEngineRunConfiguration;

  /**
   * The log channel interface.
   */
  protected ILogChannel log;

  /**
   * The log level.
   */
  protected LogLevel logLevel = LogLevel.BASIC;

  /**
   * The container object id.
   */
  protected String containerObjectId;

  /**
   * The log commit size.
   */
  protected int logCommitSize = 10;

  /**
   * The pipeline metadata to execute.
   */
  protected PipelineMeta pipelineMeta;

  /**
   * The MetaStore to use
   */
  protected IMetaStore metaStore;

  /**
   * The workflow that's launching this pipeline. This gives us access to the whole chain, including the parent
   * variables, etc.
   */
  private Workflow parentWorkflow;

  /**
   * The pipeline that is executing this pipeline in case of mappings.
   */
  private Pipeline parentPipeline;

  /**
   * The parent logging object interface (this could be a pipeline or a workflow).
   */
  private ILoggingObject parent;

  /**
   * The name of the mapping transform that executes this pipeline in case this is a mapping.
   */
  private String mappingTransformName;

  /**
   * Indicates that we want to do a topological sort of the transforms in a GUI.
   */
  private boolean sortingTransformsTopologically;

  /**
   * Indicates that we are running in preview mode...
   */
  private boolean preview;

  /**
   * The date objects for logging information about the pipeline such as start and end time, etc.
   */
  private Date startDate, endDate, currentDate, logDate, depDate;

  /**
   * The workflow start and end date.
   */
  private Date jobStartDate, jobEndDate;

  /**
   * The batch id.
   */
  private long batchId;

  /**
   * This is the batch ID that is passed from workflow to workflow to pipeline, if nothing is passed, it's the
   * pipeline's batch id.
   */
  private long passedBatchId;

  /**
   * The variable bindings for the pipeline.
   */
  private IVariables variables = new Variables();

  /**
   * A list of all the row sets.
   */
  public List<IRowSet> rowsets;

  /**
   * A list of all the transforms.
   */
  private List<TransformMetaDataCombi<ITransform, ITransformMeta, ITransformData>> transforms;

  /**
   * The class number.
   */
  public int class_nr;

  /**
   * Constant indicating a dispatch type of 1-to-1.
   */
  public static final int TYPE_DISP_1_1 = 1;

  /**
   * Constant indicating a dispatch type of 1-to-N.
   */
  public static final int TYPE_DISP_1_N = 2;

  /**
   * Constant indicating a dispatch type of N-to-1.
   */
  public static final int TYPE_DISP_N_1 = 3;

  /**
   * Constant indicating a dispatch type of N-to-N.
   */
  public static final int TYPE_DISP_N_N = 4;

  /**
   * Constant indicating a dispatch type of N-to-M.
   */
  public static final int TYPE_DISP_N_M = 5;

  /**
   * Constant indicating a pipeline status of Finished.
   */
  public static final String STRING_FINISHED = "Finished";

  /**
   * Constant indicating a pipeline status of Finished (with errors).
   */
  public static final String STRING_FINISHED_WITH_ERRORS = "Finished (with errors)";

  /**
   * Constant indicating a pipeline status of Running.
   */
  public static final String STRING_RUNNING = "Running";

  /**
   * Constant indicating a pipeline status of Paused.
   */
  public static final String STRING_PAUSED = "Paused";

  /**
   * Constant indicating a pipeline status of Preparing for execution.
   */
  public static final String STRING_PREPARING = "Preparing executing";

  /**
   * Constant indicating a pipeline status of Initializing.
   */
  public static final String STRING_INITIALIZING = "Initializing";

  /**
   * Constant indicating a pipeline status of Waiting.
   */
  public static final String STRING_WAITING = "Waiting";

  /**
   * Constant indicating a pipeline status of Stopped.
   */
  public static final String STRING_STOPPED = "Stopped";

  /**
   * Constant indicating a pipeline status of Stopped (with errors).
   */
  public static final String STRING_STOPPED_WITH_ERRORS = "Stopped (with errors)";

  /**
   * Constant indicating a pipeline status of Halting.
   */
  public static final String STRING_HALTING = "Halting";

  /**
   * Constant specifying a filename containing XML to inject into a ZIP file created during resource export.
   */
  public static final String CONFIGURATION_IN_EXPORT_FILENAME = "__job_execution_configuration__.xml";

  /**
   * Whether safe mode is enabled.
   */
  private boolean safeModeEnabled;

  /**
   * The transaction ID
   */
  private String transactionId;

  /**
   * Int value for storage pipeline statuses
   */
  private AtomicInteger status;

  /**
   * <p>This enum stores bit masks which are used to manipulate with
   * statuses over field {@link Pipeline#status}
   */
  enum BitMaskStatus {
    RUNNING( 1 ),
    INITIALIZING( 2 ),
    PREPARING( 4 ),
    STOPPED( 8 ),
    FINISHED( 16 ),
    PAUSED( 32 );

    private final int mask;
    //the sum of status masks
    public static final int BIT_STATUS_SUM = 63;

    BitMaskStatus( int mask ) {
      this.mask = mask;
    }

  }

  /**
   * The number of errors that have occurred during execution of the pipeline.
   */
  private AtomicInteger errors;

  /**
   * Whether the pipeline is ready to start.
   */
  private boolean readyToStart;

  /**
   * Transform performance snapshots.
   */
  private Map<String, List<PerformanceSnapShot>> transformPerformanceSnapShots;

  /**
   * The transform performance snapshot timer.
   */
  private Timer transformPerformanceSnapShotTimer;

  /**
   * A list of listeners attached to the pipeline.
   */
  private List<IExecutionListener<PipelineMeta>> executionListeners;

  /**
   * A list of stop-event listeners attached to the pipeline.
   */
  private List<IPipelineStoppedListener> pipelineStoppedListeners;

  /**
   * In case this pipeline starts to delegate work to a local pipeline or workflow
   */
  private List<IDelegationListener> delegationListeners;

  /**
   * The number of finished transforms.
   */
  private int nrOfFinishedTransforms;

  /**
   * The number of active transforms.
   */
  private int nrOfActiveTransforms;

  /**
   * The named parameters.
   */
  private INamedParams namedParams = new NamedParamsDefault();

  /**
   * The socket repository.
   */
  private SocketRepository socketRepository;

  /**
   * The pipeline log table database connection.
   */
  private Database pipelineLogTableDatabaseConnection;

  /**
   * The transform performance snapshot sequence number.
   */
  private AtomicInteger transformPerformanceSnapshotSeqNr;

  /**
   * The last written transform performance sequence number.
   */
  private int lastWrittenTransformPerformanceSequenceNr;

  /**
   * The last transform performance snapshot sequence number added.
   */
  private int lastTransformPerformanceSnapshotSeqNrAdded;

  /**
   * The active sub-pipelines.
   */
  private Map<String, Pipeline> activeSubPipelines;

  /**
   * The active subjobs
   */
  private Map<String, Workflow> activeSubjobs;

  /**
   * The transform performance snapshot size limit.
   */
  private int transformPerformanceSnapshotSizeLimit;

  /**
   * The servlet print writer.
   */
  private PrintWriter servletPrintWriter;

  /**
   * The pipeline finished blocking queue.
   */
  private ArrayBlockingQueue<Object> pipelineFinishedBlockingQueue;

  /**
   * The name of the executing server
   */
  private String executingServer;

  /**
   * The name of the executing user
   */
  private String executingUser;

  private Result previousResult;

  protected List<RowMetaAndData> resultRows;

  protected List<ResultFile> resultFiles;

  /**
   * The command line arguments for the pipeline.
   */
  protected String[] arguments;

  /**
   * A table of named counters.
   */
  protected Hashtable<String, Counter> counters;

  private HttpServletResponse servletResponse;

  private HttpServletRequest servletRequest;

  private Map<String, Object> extensionDataMap;

  protected int rowSetSize;

  /**
   * Whether the feedback is shown.
   */
  protected boolean feedbackShown;

  /**
   * The feedback size.
   */
  protected int feedbackSize;

  /**
   * Instantiates a new pipeline.
   */
  public Pipeline() {
    status = new AtomicInteger();

    executionListeners = Collections.synchronizedList( new ArrayList<>() );
    pipelineStoppedListeners = Collections.synchronizedList( new ArrayList<>() );
    delegationListeners = new ArrayList<>();

    // Get a valid transactionId in case we run database transactional.
    transactionId = calculateTransactionId();

    errors = new AtomicInteger( 0 );

    transformPerformanceSnapshotSeqNr = new AtomicInteger( 0 );
    lastWrittenTransformPerformanceSequenceNr = 0;

    activeSubPipelines = new ConcurrentHashMap<>();
    activeSubjobs = new HashMap<>();

    resultRows = new ArrayList<>();
    resultFiles = new ArrayList<>();
    counters = new Hashtable<>();

    extensionDataMap = new HashMap<>();

    rowSetSize = Const.ROWS_IN_ROWSET;
  }

  /**
   * Initializes a pipeline from pipeline meta-data defined in memory.
   *
   * @param pipelineMeta the pipeline meta-data to use.
   */
  public Pipeline( PipelineMeta pipelineMeta ) {
    this( pipelineMeta, null );
  }

  /**
   * Initializes a pipeline from pipeline meta-data defined in memory. Also take into account the parent log
   * channel interface (workflow or pipeline) for logging lineage purposes.
   *
   * @param pipelineMeta the pipeline meta-data to use.
   * @param parent    the parent workflow that is executing this pipeline
   */
  public Pipeline( PipelineMeta pipelineMeta, ILoggingObject parent ) {
    this();

    this.pipelineMeta = pipelineMeta;
    this.containerObjectId = pipelineMeta.getContainerObjectId();

    setParent( parent );

    initializeVariablesFrom( pipelineMeta );
    copyParametersFrom( pipelineMeta );
    pipelineMeta.activateParameters();

    // Get a valid transactionId in case we run database transactional.
    transactionId = calculateTransactionId();
  }

  @Override public PipelineMeta getSubject() {
    return pipelineMeta;
  }

  @Override public void setSubject( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * Sets the parent logging object.
   *
   * @param parent the new parent
   */
  public void setParent( ILoggingObject parent ) {
    this.parent = parent;

    this.log = new LogChannel( this, parent );
    this.logLevel = log.getLogLevel();

    if ( this.containerObjectId == null ) {
      this.containerObjectId = log.getContainerObjectId();
    }

    if ( log.isDetailed() ) {
      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.PipelineIsPreloaded" ) );
    }
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "Pipeline.Log.NumberOfTransformsToRun", String.valueOf( pipelineMeta.nrTransforms() ),
        String.valueOf( pipelineMeta.nrPipelineHops() ) ) );
    }

  }

  /**
   * Sets the default log commit size.
   */
  private void setDefaultLogCommitSize() {
    String propLogCommitSize = this.getVariable( "pentaho.log.commit.size" );
    if ( propLogCommitSize != null ) {
      // override the logCommit variable
      try {
        logCommitSize = Integer.parseInt( propLogCommitSize );
      } catch ( Exception ignored ) {
        logCommitSize = 10; // ignore parsing error and default to 10
      }
    }

  }

  /**
   * Gets the log channel interface for the pipeline.
   *
   * @return the log channel
   * @see IHasLogChannel#getLogChannel()
   */
  @Override
  public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Sets the log channel interface for the pipeline.
   *
   * @param log the new log channel interface
   */
  public void setLog( ILogChannel log ) {
    this.log = log;
  }

  /**
   * Gets the name of the pipeline.
   *
   * @return the pipeline name
   */
  public String getName() {
    if ( pipelineMeta == null ) {
      return null;
    }

    return pipelineMeta.getName();
  }

  /**
   * Instantiates a new pipeline using any of the provided parameters including the variable bindings, a name
   * and a filename. This contstructor loads the specified pipeline from a file.
   *
   * @param parent    the parent variable space and named params
   * @param name      the name of the pipeline
   * @param filename  the filename containing the pipeline definition
   * @param metaStore The MetaStore to use when referencing metadata objects
   * @throws HopException if any error occurs during loading, parsing, or creation of the pipeline
   */
  public <Parent extends IVariables & INamedParams> Pipeline( Parent parent, String name, String filename, IMetaStore metaStore ) throws HopException {
    this();

    this.metaStore = metaStore;

    try {
      pipelineMeta = new PipelineMeta( filename, metaStore, false, this );

      this.log = new LogChannel( pipelineMeta );

      pipelineMeta.initializeVariablesFrom( parent );
      initializeVariablesFrom( parent );
      // PDI-3064 do not erase parameters from meta!
      // instead of this - copy parameters to actual pipeline
      this.copyParametersFrom( parent );
      this.activateParameters();

      this.setDefaultLogCommitSize();

      // Get a valid transactionId in case we run database transactional.
      transactionId = calculateTransactionId();
    } catch ( HopException e ) {
      throw new HopException( BaseMessages.getString( PKG, "Pipeline.Exception.UnableToOpenPipeline", name ), e );
    }
  }

  /**
   * Executes the pipeline. This method will prepare the pipeline for execution and then start all the
   * threads associated with the pipeline and its transforms.
   *
   * @throws HopException if the pipeline could not be prepared (initialized)
   */
  public void execute() throws HopException {
    prepareExecution();
    startThreads();
  }

  /**
   * Prepares the pipeline for execution. This includes setting the arguments and parameters as well as preparing
   * and tracking the transforms and hops in the pipeline.
   *
   * @throws HopException in case the pipeline could not be prepared (initialized)
   */
  public void prepareExecution() throws HopException {
    setPreparing( true );
    startDate = null;
    setRunning( false );

    log.snap( Metrics.METRIC_PIPELINE_EXECUTION_START );
    log.snap( Metrics.METRIC_PIPELINE_INIT_START );

    ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.PipelinePrepareExecution.id, this );

    activateParameters();
    pipelineMeta.activateParameters();

    if ( pipelineMeta.getName() == null ) {
      if ( pipelineMeta.getFilename() != null ) {
        log.logBasic( BaseMessages.getString( PKG, "Pipeline.Log.DispacthingStartedForFilename", pipelineMeta
          .getFilename() ) );
      }
    } else {
      log.logBasic( BaseMessages.getString( PKG, "Pipeline.Log.DispacthingStartedForPipeline", pipelineMeta
        .getName() ) );
    }

    if ( isSafeModeEnabled() ) {
      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.SafeModeIsEnabled", pipelineMeta.getName() ) );
      }
    }

    // setInternalHopVariables(this); --> Let's not do this, when running
    // without file, for example remote, it spoils the fun

    // extra check to see if the servlet print writer has some value in case
    // folks want to test it locally...
    //
    if ( servletPrintWriter == null ) {
      String encoding = System.getProperty( "HOP_DEFAULT_SERVLET_ENCODING", null );
      if ( encoding == null ) {
        servletPrintWriter = new PrintWriter( new OutputStreamWriter( System.out ) );
      } else {
        try {
          servletPrintWriter = new PrintWriter( new OutputStreamWriter( System.out, encoding ) );
        } catch ( UnsupportedEncodingException ex ) {
          servletPrintWriter = new PrintWriter( new OutputStreamWriter( System.out ) );
        }
      }
    }

    // Keep track of all the row sets and allocated transforms
    //
    transforms = Collections.synchronizedList( new ArrayList<>() );
    rowsets = new ArrayList<>();

    List<TransformMeta> hopTransforms = pipelineMeta.getPipelineHopTransforms( false );

    if ( log.isDetailed() ) {
      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.FoundDefferentTransforms", String.valueOf( hopTransforms
        .size() ) ) );
      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.AllocatingRowsets" ) );
    }
    // First allocate all the rowsets required!
    // Note that a mapping doesn't receive ANY input or output rowsets...
    //
    for ( int i = 0; i < hopTransforms.size(); i++ ) {
      TransformMeta thisTransform = hopTransforms.get( i );
      if ( thisTransform.isMapping() ) {
        continue; // handled and allocated by the mapping transform itself.
      }

      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.AllocateingRowsetsForTransform", String.valueOf( i ),
          thisTransform.getName() ) );
      }

      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms( thisTransform );
      int nrTargets = nextTransforms.size();

      for ( int n = 0; n < nrTargets; n++ ) {
        // What's the next transform?
        TransformMeta nextTransform = nextTransforms.get( n );
        if ( nextTransform.isMapping() ) {
          continue; // handled and allocated by the mapping transform itself.
        }

        // How many times do we start the source transform?
        int thisCopies = thisTransform.getCopies();

        if ( thisCopies < 0 ) {
          // This can only happen if a variable is used that didn't resolve to a positive integer value
          //
          throw new HopException( BaseMessages.getString( PKG, "Pipeline.Log.TransformCopiesNotCorrectlyDefined", thisTransform
            .getName() ) );
        }

        // How many times do we start the target transform?
        int nextCopies = nextTransform.getCopies();

        // Are we re-partitioning?
        boolean repartitioning;
        if ( thisTransform.isPartitioned() ) {
          repartitioning = !thisTransform.getTransformPartitioningMeta().equals( nextTransform.getTransformPartitioningMeta() );
        } else {
          repartitioning = nextTransform.isPartitioned();
        }

        int nrCopies;
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.copiesInfo", String.valueOf( thisCopies ), String
            .valueOf( nextCopies ) ) );
        }
        int dispatchType;
        if ( thisCopies == 1 && nextCopies == 1 ) {
          dispatchType = TYPE_DISP_1_1;
          nrCopies = 1;
        } else if ( thisCopies == 1 && nextCopies > 1 ) {
          dispatchType = TYPE_DISP_1_N;
          nrCopies = nextCopies;
        } else if ( thisCopies > 1 && nextCopies == 1 ) {
          dispatchType = TYPE_DISP_N_1;
          nrCopies = thisCopies;
        } else if ( thisCopies == nextCopies && !repartitioning ) {
          dispatchType = TYPE_DISP_N_N;
          nrCopies = nextCopies;
        } else {
          // > 1!
          dispatchType = TYPE_DISP_N_M;
          nrCopies = nextCopies;
        } // Allocate a rowset for each destination transform

        // Allocate the rowsets
        //
        if ( dispatchType != TYPE_DISP_N_M ) {
          for ( int c = 0; c < nrCopies; c++ ) {
            IRowSet rowSet;
            switch ( pipelineMeta.getPipelineType() ) {
              case Normal:
                // This is a temporary patch until the batching rowset has proven
                // to be working in all situations.
                // Currently there are stalling problems when dealing with small
                // amounts of rows.
                //
                Boolean batchingRowSet =
                  ValueMetaString.convertStringToBoolean( System.getProperty( Const.HOP_BATCHING_ROWSET ) );
                if ( batchingRowSet != null && batchingRowSet.booleanValue() ) {
                  rowSet = new BlockingBatchingRowSet( rowSetSize );
                } else {
                  rowSet = new BlockingRowSet( rowSetSize );
                }
                break;

              case SingleThreaded:
                rowSet = new QueueRowSet();
                break;

              default:
                throw new HopException( "Unhandled pipeline type: " + pipelineMeta.getPipelineType() );
            }

            switch ( dispatchType ) {
              case TYPE_DISP_1_1:
                rowSet.setThreadNameFromToCopy( thisTransform.getName(), 0, nextTransform.getName(), 0 );
                break;
              case TYPE_DISP_1_N:
                rowSet.setThreadNameFromToCopy( thisTransform.getName(), 0, nextTransform.getName(), c );
                break;
              case TYPE_DISP_N_1:
                rowSet.setThreadNameFromToCopy( thisTransform.getName(), c, nextTransform.getName(), 0 );
                break;
              case TYPE_DISP_N_N:
                rowSet.setThreadNameFromToCopy( thisTransform.getName(), c, nextTransform.getName(), c );
                break;
              default:
                break;
            }
            rowsets.add( rowSet );
            if ( log.isDetailed() ) {
              log.logDetailed( BaseMessages.getString( PKG, "Pipeline.PipelineAllocatedNewRowset", rowSet
                .toString() ) );
            }
          }
        } else {
          // For each N source transforms we have M target transforms
          //
          // From each input transform we go to all output transforms.
          // This allows maximum flexibility for re-partitioning,
          // distribution...
          for ( int s = 0; s < thisCopies; s++ ) {
            for ( int t = 0; t < nextCopies; t++ ) {
              BlockingRowSet rowSet = new BlockingRowSet( rowSetSize );
              rowSet.setThreadNameFromToCopy( thisTransform.getName(), s, nextTransform.getName(), t );
              rowsets.add( rowSet );
              if ( log.isDetailed() ) {
                log.logDetailed( BaseMessages.getString( PKG, "Pipeline.PipelineAllocatedNewRowset", rowSet
                  .toString() ) );
              }
            }
          }
        }
      }
      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.AllocatedRowsets", String.valueOf( rowsets.size() ),
        String.valueOf( i ), thisTransform.getName() ) + " " );
    }

    if ( log.isDetailed() ) {
      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.AllocatingTransformsAndTransformData" ) );
    }

    // Allocate the transforms & the data...
    //
    for ( int i = 0; i < hopTransforms.size(); i++ ) {
      TransformMeta transformMeta = hopTransforms.get( i );
      String transformid = transformMeta.getTransformPluginId();

      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.PipelineIsToAllocateTransform", transformMeta.getName(),
          transformid ) );
      }

      // How many copies are launched of this transform?
      int nrCopies = transformMeta.getCopies();

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "Pipeline.Log.TransformHasNumberRowCopies", String.valueOf( nrCopies ) ) );
      }

      // At least run once...
      for ( int c = 0; c < nrCopies; c++ ) {
        // Make sure we haven't started it yet!
        if ( !hasTransformStarted( transformMeta.getName(), c ) ) {
          TransformMetaDataCombi combi = new TransformMetaDataCombi();

          combi.transformName = transformMeta.getName();
          combi.copy = c;

          // The meta-data
          combi.transformMeta = transformMeta;
          combi.meta = transformMeta.getTransformMetaInterface();

          // Allocate the transform data
          ITransformData data = combi.meta.getTransformData();
          combi.data = data;

          // Allocate the transform
          ITransform transform = combi.meta.createTransform( transformMeta, data, c, pipelineMeta, this );

          // Copy the variables of the pipeline to the transform...
          // don't share. Each copy of the transform has its own variables.
          //
          transform.initializeVariablesFrom( this );
          transform.setUsingThreadPriorityManagment( pipelineMeta.isUsingThreadPriorityManagment() );

          // Pass the metaStore to the transforms runtime
          //
          transform.setMetaStore( metaStore );

          // If the transform is partitioned, set the partitioning ID and some other
          // things as well...
          if ( transformMeta.isPartitioned() ) {
            List<String> partitionIDs = transformMeta.getTransformPartitioningMeta().getPartitionSchema().calculatePartitionIds();
            if ( partitionIDs != null && partitionIDs.size() > 0 ) {
              transform.setPartitionId( partitionIDs.get( c ) ); // Pass the partition ID
              // to the transform
            }
          }

          // Save the transform too
          combi.transform = transform;

          // Pass logging level and metrics gathering down to the transform level.
          // /
          if ( combi.transform instanceof ILoggingObject ) {
            ILogChannel logChannel = combi.transform.getLogChannel();
            logChannel.setLogLevel( logLevel );
            logChannel.setGatheringMetrics( log.isGatheringMetrics() );
          }

          // Add to the bunch...
          transforms.add( combi );

          if ( log.isDetailed() ) {
            log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.PipelineHasAllocatedANewTransform", transformMeta
              .getName(), String.valueOf( c ) ) );
          }
        }
      }
    }

    // Now we need to verify if certain rowsets are not meant to be for error
    // handling...
    // Loop over the transforms and for every transform verify the output rowsets
    // If a rowset is going to a target transform in the transforms error handling
    // metadata, set it to the errorRowSet.
    // The input rowsets are already in place, so the next transform just accepts the
    // rows.
    // Metadata wise we need to do the same trick in PipelineMeta
    //
    for ( int s = 0; s < transforms.size(); s++ ) {
      TransformMetaDataCombi combi = transforms.get( s );
      if ( combi.transformMeta.isDoingErrorHandling() ) {
        combi.transform.identifyErrorOutput();

      }
    }

    // Now (optionally) write start log record!
    // Make sure we synchronize appropriately to avoid duplicate batch IDs.
    //
    Object syncObject = this;
    if ( parentWorkflow != null ) {
      syncObject = parentWorkflow; // parallel execution in a workflow
    }
    if ( parentPipeline != null ) {
      syncObject = parentPipeline; // multiple sub-pipelines
    }
    synchronized ( syncObject ) {
      calculateBatchIdAndDateRange();
      beginProcessing();
    }

    // Set the partition-to-rowset mapping
    //
    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );

      TransformMeta transformMeta = sid.transformMeta;
      ITransform baseTransform = sid.transform;

      baseTransform.setPartitioned( transformMeta.isPartitioned() );

      // Now let's take a look at the source and target relation
      //
      // If this source transform is not partitioned, and the target transform is: it
      // means we need to re-partition the incoming data.
      // If both transforms are partitioned on the same method and schema, we don't
      // need to re-partition
      // If both transforms are partitioned on a different method or schema, we need
      // to re-partition as well.
      // If both transforms are not partitioned, we don't need to re-partition
      //
      boolean isThisPartitioned = transformMeta.isPartitioned();
      PartitionSchema thisPartitionSchema = null;
      if ( isThisPartitioned ) {
        thisPartitionSchema = transformMeta.getTransformPartitioningMeta().getPartitionSchema();
      }

      boolean isNextPartitioned = false;
      TransformPartitioningMeta nextTransformPartitioningMeta = null;
      PartitionSchema nextPartitionSchema = null;

      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms( transformMeta );
      int nrNext = nextTransforms.size();
      for ( int p = 0; p < nrNext; p++ ) {
        TransformMeta nextTransform = nextTransforms.get( p );
        if ( nextTransform.isPartitioned() ) {
          isNextPartitioned = true;
          nextTransformPartitioningMeta = nextTransform.getTransformPartitioningMeta();
          nextPartitionSchema = nextTransformPartitioningMeta.getPartitionSchema();
        }
      }

      baseTransform.setRepartitioning( TransformPartitioningMeta.PARTITIONING_METHOD_NONE );

      // If the next transform is partitioned differently, set re-partitioning, when
      // running locally.
      //
      if ( ( !isThisPartitioned && isNextPartitioned ) || ( isThisPartitioned && isNextPartitioned
        && !thisPartitionSchema.equals( nextPartitionSchema ) ) ) {
        baseTransform.setRepartitioning( nextTransformPartitioningMeta.getMethodType() );
      }

      // For partitioning to a set of remove transforms (repartitioning from a master
      // to a set or remote output transforms)
      //
      TransformPartitioningMeta targetTransformPartitioningMeta = baseTransform.getTransformMeta().getTargetTransformPartitioningMeta();
      if ( targetTransformPartitioningMeta != null ) {
        baseTransform.setRepartitioning( targetTransformPartitioningMeta.getMethodType() );
      }
    }

    setPreparing( false );
    setInitializing( true );

    // Do a topology sort... Over 150 transform (copies) things might be slowing down too much.
    //
    if ( isSortingTransformsTopologically() && transforms.size() < 150 ) {
      doTopologySortOfTransforms();
    }

    if ( log.isDetailed() ) {
      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.InitialisingTransforms", String.valueOf( transforms.size() ) ) );
    }

    TransformInitThread[] initThreads = new TransformInitThread[ transforms.size() ];
    Thread[] threads = new Thread[ transforms.size() ];

    // Initialize all the threads...
    //
    for ( int i = 0; i < transforms.size(); i++ ) {
      final TransformMetaDataCombi sid = transforms.get( i );

      // Do the init code in the background!
      // Init all transforms at once, but ALL transforms need to finish before we can
      // continue properly!
      //
      initThreads[ i ] = new TransformInitThread( sid, log );

      // Put it in a separate thread!
      //
      threads[ i ] = new Thread( initThreads[ i ] );
      threads[ i ].setName( "init of " + sid.transformName + "." + sid.copy + " (" + threads[ i ].getName() + ")" );

      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.TransformBeforeInitialize.id, initThreads[ i ] );

      threads[ i ].start();
    }

    for ( int i = 0; i < threads.length; i++ ) {
      try {
        threads[ i ].join();
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.TransformAfterInitialize.id, initThreads[ i ] );
      } catch ( Exception ex ) {
        log.logError( "Error with init thread: " + ex.getMessage(), ex.getMessage() );
        log.logError( Const.getStackTracker( ex ) );
      }
    }

    setInitializing( false );
    boolean ok = true;

    // All transform are initialized now: see if there was one that didn't do it
    // correctly!
    //
    for ( int i = 0; i < initThreads.length; i++ ) {
      TransformMetaDataCombi combi = initThreads[ i ].getCombi();
      if ( !initThreads[ i ].isOk() ) {
        log.logError( BaseMessages.getString( PKG, "Pipeline.Log.TransformFailedToInit", combi.transformName + "." + combi.copy ) );
        combi.data.setStatus( TransformExecutionStatus.STATUS_STOPPED );
        ok = false;
      } else {
        combi.data.setStatus( TransformExecutionStatus.STATUS_IDLE );
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.TransformInitialized", combi.transformName + "."
            + combi.copy ) );
        }
      }
    }

    if ( !ok ) {
      // Halt the other threads as well, signal end-of-the line to the outside
      // world...
      // Also explicitly call dispose() to clean up resources opened during
      // init();
      //
      for ( int i = 0; i < initThreads.length; i++ ) {
        TransformMetaDataCombi combi = initThreads[ i ].getCombi();

        // Dispose will overwrite the status, but we set it back right after
        // this.
        combi.transform.dispose();

        if ( initThreads[ i ].isOk() ) {
          combi.data.setStatus( BaseTransformData.TransformExecutionStatus.STATUS_HALTED );
        } else {
          combi.data.setStatus( TransformExecutionStatus.STATUS_STOPPED );
        }
      }

      // Just for safety, fire the pipeline finished listeners...
      try {
        firePipelineExecutionListeners();
      } catch ( HopException e ) {
        // listeners produces errors
        log.logError( BaseMessages.getString( PKG, "Pipeline.FinishListeners.Exception" ) );
        // we will not pass this exception up to prepareExecuton() entry point.
      } finally {
        // Flag the pipeline as finished even if exception was thrown
        setFinished( true );
      }

      // Pass along the log during preview. Otherwise it becomes hard to see
      // what went wrong.
      //
      if ( preview ) {
        String logText = HopLogStore.getAppender().getBuffer( getLogChannelId(), true ).toString();
        throw new HopException( BaseMessages.getString( PKG, "Pipeline.Log.FailToInitializeAtLeastOneTransform" ) + Const.CR
          + logText );
      } else {
        throw new HopException( BaseMessages.getString( PKG, "Pipeline.Log.FailToInitializeAtLeastOneTransform" )
          + Const.CR );
      }
    }

    log.snap( Metrics.METRIC_PIPELINE_INIT_STOP );

    setReadyToStart( true );
  }

  /**
   * Starts the threads prepared by prepareThreads(). Before you start the threads, you can add RowListeners to them.
   *
   * @throws HopException if there is a communication error with a remote output socket.
   */
  public void startThreads() throws HopException {
    // Now prepare to start all the threads...
    //
    nrOfFinishedTransforms = 0;
    nrOfActiveTransforms = 0;

    ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.PipelineStartThreads.id, this );

    firePipelineExecutionStartedListeners();

    for ( int i = 0; i < transforms.size(); i++ ) {
      final TransformMetaDataCombi sid = transforms.get( i );
      sid.transform.markStart();
      sid.transform.initBeforeStart();

      // also attach a Transform Listener to detect when we're done...
      //
      ITransformListener transformListener = new ITransformListener() {
        @Override
        public void transformActive( Pipeline pipeline, TransformMeta transformMeta, ITransform transform ) {
          nrOfActiveTransforms++;
          if ( nrOfActiveTransforms == 1 ) {
            // Pipeline goes from in-active to active...
            // PDI-5229 sync added
            synchronized ( executionListeners ) {
              for ( IExecutionListener listener : executionListeners ) {
                listener.becameActive( Pipeline.this );
              }
            }
          }
        }

        @Override
        public void transformFinished( Pipeline pipeline, TransformMeta transformMeta, ITransform transform ) {
          synchronized ( Pipeline.this ) {
            nrOfFinishedTransforms++;

            if ( nrOfFinishedTransforms >= transforms.size() ) {
              // Set the finished flag
              //
              setFinished( true );

              // Grab the performance statistics one last time (if enabled)
              //
              addTransformPerformanceSnapShot();

              try {
                firePipelineExecutionListeners();
              } catch ( Exception e ) {
                transform.setErrors( transform.getErrors() + 1L );
                log.logError( getName() + " : " + BaseMessages.getString( PKG,
                  "Pipeline.Log.UnexpectedErrorAtPipelineEnd" ), e );
              }
            }

            // If a transform fails with an error, we want to kill/stop the others
            // too...
            //
            if ( transform.getErrors() > 0 ) {

              log.logMinimal( BaseMessages.getString( PKG, "Pipeline.Log.PipelineDetectedErrors" ) );
              log.logMinimal( BaseMessages.getString( PKG, "Pipeline.Log.PipelineIsKillingTheOtherTransforms" ) );

              killAllNoWait();
            }
          }
        }
      };
      // Make sure this is called first!
      //
      if ( sid.transform instanceof BaseTransform ) {
        ( (BaseTransform) sid.transform ).getTransformListeners().add( 0, transformListener );
      } else {
        sid.transform.addTransformListener( transformListener );
      }
    }

    if ( pipelineMeta.isCapturingTransformPerformanceSnapShots() ) {
      transformPerformanceSnapshotSeqNr = new AtomicInteger( 0 );
      transformPerformanceSnapShots = new ConcurrentHashMap<>();

      // Calculate the maximum number of snapshots to be kept in memory
      //
      String limitString = environmentSubstitute( pipelineMeta.getTransformPerformanceCapturingSizeLimit() );
      if ( Utils.isEmpty( limitString ) ) {
        limitString = EnvUtil.getSystemProperty( Const.HOP_TRANSFORM_PERFORMANCE_SNAPSHOT_LIMIT );
      }
      transformPerformanceSnapshotSizeLimit = Const.toInt( limitString, 0 );

      // Set a timer to collect the performance data from the running threads...
      //
      transformPerformanceSnapShotTimer = new Timer( "transformPerformanceSnapShot Timer: " + pipelineMeta.getName() );
      TimerTask timerTask = new TimerTask() {
        @Override
        public void run() {
          if ( !isFinished() ) {
            addTransformPerformanceSnapShot();
          }
        }
      };
      transformPerformanceSnapShotTimer.schedule( timerTask, 100, pipelineMeta.getTransformPerformanceCapturingDelay() );
    }

    // Now start a thread to monitor the running pipeline...
    //
    setFinished( false );
    setPaused( false );
    setStopped( false );

    pipelineFinishedBlockingQueue = new ArrayBlockingQueue<>( 10 );

    IExecutionListener executionListener = new ExecutionAdapter<PipelineMeta>() {
      @Override
      public void finished( IPipelineEngine<PipelineMeta> pipeline ) {

        try {
          ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.PipelineFinish.id, pipeline );
        } catch ( HopException e ) {
          throw new RuntimeException( "Error calling extension point at end of pipeline", e );
        }

        // First of all, stop the performance snapshot timer if there is is
        // one...
        //
        if ( pipelineMeta.isCapturingTransformPerformanceSnapShots() && transformPerformanceSnapShotTimer != null ) {
          transformPerformanceSnapShotTimer.cancel();
        }

        setFinished( true );
        setRunning( false ); // no longer running

        log.snap( Metrics.METRIC_PIPELINE_EXECUTION_STOP );

        // If the user ran with metrics gathering enabled and a metrics logging table is configured, add another
        // listener...
        //
        MetricsLogTable metricsLogTable = pipelineMeta.getMetricsLogTable();
        if ( metricsLogTable.isDefined() ) {
          try {
            writeMetricsInformation();
          } catch ( Exception e ) {
            log.logError( "Error writing metrics information", e );
            errors.incrementAndGet();
          }
        }

        // Close the unique connections when running database transactionally.
        // This will commit or roll back the transaction based on the result of this pipeline.
        //
        if ( pipelineMeta.isUsingUniqueConnections() ) {
          pipeline.closeUniqueDatabaseConnections( getResult() );
        }

        // release unused vfs connections
        HopVFS.freeUnusedResources();
      }
    };
    // This should always be done first so that the other listeners achieve a clean state to start from (setFinished and
    // so on)
    //
    executionListeners.add( 0, executionListener );

    setRunning( true );

    switch ( pipelineMeta.getPipelineType() ) {
      case Normal:

        // Now start all the threads...
        //
        for ( int i = 0; i < transforms.size(); i++ ) {
          final TransformMetaDataCombi combi = transforms.get( i );
          RunThread runThread = new RunThread( combi );
          Thread thread = new Thread( runThread );
          thread.setName( getName() + " - " + combi.transformName );
          ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.TransformBeforeStart.id, combi );
          // Call an extension point at the end of the transform
          //
          combi.transform.addTransformListener( new TransformAdapter() {

            @Override
            public void transformFinished( Pipeline pipeline, TransformMeta transformMeta, ITransform transform ) {
              try {
                ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.TransformFinished.id, combi );
              } catch ( HopException e ) {
                throw new RuntimeException( "Unexpected error in calling extension point upon transform finish", e );
              }
            }

          } );

          thread.start();
        }
        break;

      case SingleThreaded:
        // Don't do anything, this needs to be handled by the pipeline
        // executor!
        //
        break;
      default:
        break;

    }

    ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.PipelineStart.id, this );

    if ( transforms.isEmpty() ) {
      firePipelineExecutionListeners();
    }

    if ( log.isDetailed() ) {
      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.PipelineHasAllocated", String.valueOf( transforms
        .size() ), String.valueOf( rowsets.size() ) ) );
    }
  }

  /**
   * Make attempt to fire all registered listeners if possible.
   *
   * @throws HopException if any errors occur during notification
   */
  protected void firePipelineExecutionListeners() throws HopException {
    // PDI-5229 sync added
    synchronized ( executionListeners ) {
      if ( executionListeners.size() == 0 ) {
        return;
      }
      // prevent Exception from one listener to block others execution
      List<HopException> badGuys = new ArrayList<>( executionListeners.size() );
      for ( IExecutionListener executionListener : executionListeners ) {
        try {
          executionListener.finished( this );
        } catch ( HopException e ) {
          badGuys.add( e );
        }
      }
      if ( pipelineFinishedBlockingQueue != null ) {
        // Signal for the the waitUntilFinished blocker...
        pipelineFinishedBlockingQueue.add( new Object() );
      }
      if ( !badGuys.isEmpty() ) {
        // FIFO
        throw new HopException( badGuys.get( 0 ) );
      }
    }
  }

  /**
   * Fires the start-event listeners (if any are registered).
   *
   * @throws HopException if any errors occur during notification
   */
  protected void firePipelineExecutionStartedListeners() throws HopException {
    // PDI-5229 sync added
    synchronized ( executionListeners ) {
      for ( IExecutionListener executionListener : executionListeners ) {
        executionListener.started( this );
      }
    }
  }

  /**
   * Adds a transform performance snapshot.
   */
  protected void addTransformPerformanceSnapShot() {

    if ( transformPerformanceSnapShots == null ) {
      return; // Race condition somewhere?
    }

    boolean pausedAndNotEmpty = isPaused() && !transformPerformanceSnapShots.isEmpty();
    boolean stoppedAndNotEmpty = isStopped() && !transformPerformanceSnapShots.isEmpty();

    if ( pipelineMeta.isCapturingTransformPerformanceSnapShots() && !pausedAndNotEmpty && !stoppedAndNotEmpty ) {
      // get the statistics from the transforms and keep them...
      //
      int seqNr = transformPerformanceSnapshotSeqNr.incrementAndGet();
      for ( int i = 0; i < transforms.size(); i++ ) {
        TransformMeta transformMeta = transforms.get( i ).transformMeta;
        ITransform transform = transforms.get( i ).transform;

        PerformanceSnapShot snapShot =
          new PerformanceSnapShot( seqNr, getBatchId(), new Date(), getName(), transformMeta.getName(), transform.getCopy(),
            transform.getLinesRead(), transform.getLinesWritten(), transform.getLinesInput(), transform.getLinesOutput(), transform
            .getLinesUpdated(), transform.getLinesRejected(), transform.getErrors() );

        synchronized ( transformPerformanceSnapShots ) {
          List<PerformanceSnapShot> snapShotList = transformPerformanceSnapShots.get( transform.toString() );
          PerformanceSnapShot previous;
          if ( snapShotList == null ) {
            snapShotList = new ArrayList<>();
            transformPerformanceSnapShots.put( transform.toString(), snapShotList );
            previous = null;
          } else {
            previous = snapShotList.get( snapShotList.size() - 1 ); // the last one...
          }
          // Make the difference...
          //
          snapShot.diff( previous, transform.rowsetInputSize(), transform.rowsetOutputSize() );
          snapShotList.add( snapShot );

          if ( transformPerformanceSnapshotSizeLimit > 0 && snapShotList.size() > transformPerformanceSnapshotSizeLimit ) {
            snapShotList.remove( 0 );
          }
        }
      }

      lastTransformPerformanceSnapshotSeqNrAdded = transformPerformanceSnapshotSeqNr.get();
    }
  }

  /**
   * This method performs any cleanup operations, typically called after the pipeline has finished.
   */
  public void cleanup() {
    // Close all open server sockets.
    // We can only close these after all processing has been confirmed to be finished.
    //
    if ( transforms == null ) {
      return;
    }

    for ( TransformMetaDataCombi combi : transforms ) {
      combi.transform.cleanup();
    }
  }

  /**
   * Waits until all RunThreads have finished.
   */
  public void waitUntilFinished() {
    try {
      if ( pipelineFinishedBlockingQueue == null ) {
        return;
      }
      boolean wait = true;
      while ( wait ) {
        wait = pipelineFinishedBlockingQueue.poll( 1, TimeUnit.DAYS ) == null;
        if ( wait ) {
          // poll returns immediately - this was hammering the CPU with poll checks. Added
          // a sleep to let the CPU breathe
          Thread.sleep( 1 );
        }
      }
    } catch ( InterruptedException e ) {
      throw new RuntimeException( "Waiting for pipeline to be finished interrupted!", e );
    }
  }

  /**
   * Gets the number of errors that have occurred during execution of the pipeline.
   *
   * @return the number of errors
   */
  public int getErrors() {
    int nrErrors = errors.get();

    if ( transforms == null ) {
      return nrErrors;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      if ( sid.transform.getErrors() != 0L ) {
        nrErrors += sid.transform.getErrors();
      }
    }
    if ( nrErrors > 0 ) {
      log.logError( BaseMessages.getString( PKG, "Pipeline.Log.PipelineErrorsDetected" ) );
    }

    return nrErrors;
  }


  /**
   * Checks if the pipeline is finished\.
   *
   * @return true if the pipeline is finished, false otherwise
   */
  public boolean isFinished() {
    int exist = status.get() & FINISHED.mask;
    return exist != 0;
  }

  protected void setFinished( boolean finished ) {
    status.updateAndGet( v -> finished ? v | FINISHED.mask : ( BIT_STATUS_SUM ^ FINISHED.mask ) & v );
  }

  public boolean isFinishedOrStopped() {
    return isFinished() || isStopped();
  }

  /**
   * Asks all transforms to stop but doesn't wait around for it to happen. This is a special method for use with mappings.
   */
  private void killAllNoWait() {
    if ( transforms == null ) {
      return;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform transform = sid.transform;

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "Pipeline.Log.LookingAtTransform" ) + transform.getTransformName() );
      }

      transform.stopAll();
      try {
        Thread.sleep( 20 );
      } catch ( Exception e ) {
        log.logError( BaseMessages.getString( PKG, "Pipeline.Log.PipelineErrors" ) + e.toString() );
        return;
      }
    }
  }

  /**
   * Finds the IRowSet between two transforms (or copies of transforms).
   *
   * @param from     the name of the "from" transform
   * @param fromcopy the copy number of the "from" transform
   * @param to       the name of the "to" transform
   * @param tocopy   the copy number of the "to" transform
   * @return the row set, or null if none found
   */
  public IRowSet findRowSet( String from, int fromcopy, String to, int tocopy ) {
    // Start with the pipeline.
    for ( int i = 0; i < rowsets.size(); i++ ) {
      IRowSet rs = rowsets.get( i );
      if ( rs.getOriginTransformName().equalsIgnoreCase( from ) && rs.getDestinationTransformName().equalsIgnoreCase( to ) && rs
        .getOriginTransformCopy() == fromcopy && rs.getDestinationTransformCopy() == tocopy ) {
        return rs;
      }
    }

    return null;
  }

  /**
   * Checks whether the specified transform (or transform copy) has started.
   *
   * @param sname the transform name
   * @param copy  the copy number
   * @return true the specified transform (or transform copy) has started, false otherwise
   */
  public boolean hasTransformStarted( String sname, int copy ) {
    // log.logDetailed("DIS: Checking wether of not ["+sname+"]."+cnr+" has started!");
    // log.logDetailed("DIS: hasTransformStarted() looking in "+threads.size()+" threads");
    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      boolean started = ( sid.transformName != null && sid.transformName.equalsIgnoreCase( sname ) ) && sid.copy == copy;
      if ( started ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Stops only input transforms so that all downstream transforms can finish processing rows that have already been input
   */
  public void safeStop() {
    if ( transforms == null ) {
      return;
    }
    transforms.stream()
      .filter( this::isInputTransform )
      .forEach( combi -> stopTransform( combi, true ) );

    notifyStoppedListeners();
  }

  private boolean isInputTransform( TransformMetaDataCombi combi ) {
    checkNotNull( combi );
    return pipelineMeta.findPreviousTransforms( combi.transformMeta, true ).size() == 0;
  }

  /**
   * Stops all transforms from running, and alerts any registered listeners.
   */
  public void stopAll() {
    if ( transforms == null ) {
      return;
    }
    transforms.forEach( combi -> stopTransform( combi, false ) );

    // if it is stopped it is not paused
    setPaused( false );
    setStopped( true );

    notifyStoppedListeners();
  }

  public void stopTransform( TransformMetaDataCombi combi, boolean safeStop ) {
    ITransform rt = combi.transform;
    rt.setStopped( true );
    rt.setSafeStopped( safeStop );
    rt.resumeRunning();

    try {
      rt.stopRunning();
    } catch ( Exception e ) {
      log.logError( "Something went wrong while trying to safe stop the pipeline: ", e );
    }
    combi.data.setStatus( BaseTransformData.TransformExecutionStatus.STATUS_STOPPED );
    if ( safeStop ) {
      rt.setOutputDone();
    }
  }

  public void notifyStoppedListeners() {
    // Fire the stopped listener...
    //
    synchronized ( pipelineStoppedListeners ) {
      for ( IPipelineStoppedListener listener : pipelineStoppedListeners ) {
        listener.pipelineStopped( this );
      }
    }
  }

  /**
   * Gets the number of transforms in this pipeline.
   *
   * @return the number of transforms
   */
  public int nrTransforms() {
    if ( transforms == null ) {
      return 0;
    }
    return transforms.size();
  }

  /**
   * Gets the number of active (i.e. not finished) transforms in this pipeline
   *
   * @return the number of active transforms
   */
  public int nrActiveTransforms() {
    if ( transforms == null ) {
      return 0;
    }

    int nr = 0;
    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      // without also considering a transform status of not finished,
      // the transform execution results grid shows empty while
      // the pipeline has transforms still running.
      // if ( sid.transform.isRunning() ) nr++;
      if ( sid.transform.isRunning() || sid.transform.getStatus() != BaseTransformData.TransformExecutionStatus.STATUS_FINISHED ) {
        nr++;
      }
    }
    return nr;
  }

  /**
   * Checks whether the pipeline transforms are running lookup.
   *
   * @return a boolean array associated with the transform list, indicating whether that transform is running a lookup.
   */
  public boolean[] getPipelineTransformIsRunningLookup() {
    if ( transforms == null ) {
      return null;
    }

    boolean[] tResult = new boolean[ transforms.size() ];
    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      tResult[ i ] = ( sid.transform.isRunning() || sid.transform.getStatus() != TransformExecutionStatus.STATUS_FINISHED );
    }
    return tResult;
  }

  /**
   * Checks the execution status of each transform in the pipelines.
   *
   * @return an array associated with the transform list, indicating the status of that transform.
   */
  public TransformExecutionStatus[] getPipelineTransformExecutionStatusLookup() {
    if ( transforms == null ) {
      return null;
    }

    // we need this snapshot for the PipelineGridDelegate refresh method to handle the
    // difference between a timed refresh and continual transform status updates
    int totalTransforms = transforms.size();
    TransformExecutionStatus[] tList = new TransformExecutionStatus[ totalTransforms ];
    for ( int i = 0; i < totalTransforms; i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      tList[ i ] = sid.transform.getStatus();
    }
    return tList;
  }

  /**
   * Gets the run thread for the transform at the specified index.
   *
   * @param i the index of the desired transform
   * @return a ITransform object corresponding to the run thread for the specified transform
   */
  public ITransform getRunThread( int i ) {
    if ( transforms == null ) {
      return null;
    }
    return transforms.get( i ).transform;
  }

  /**
   * Gets the run thread for the transform with the specified name and copy number.
   *
   * @param name the transform name
   * @param copy the copy number
   * @return a ITransform object corresponding to the run thread for the specified transform
   */
  public ITransform getRunThread( String name, int copy ) {
    if ( transforms == null ) {
      return null;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform transform = sid.transform;
      if ( transform.getTransformName().equalsIgnoreCase( name ) && transform.getCopy() == copy ) {
        return transform;
      }
    }

    return null;
  }

  /**
   * Calculate the batch id and date range for the pipeline.
   *
   * @throws HopPipelineException if there are any errors during calculation
   */
  public void calculateBatchIdAndDateRange() throws HopPipelineException {

    PipelineLogTable pipelineLogTable = pipelineMeta.getPipelineLogTable();

    currentDate = new Date();
    logDate = new Date();
    startDate = Const.MIN_DATE;
    endDate = currentDate;

    DatabaseMeta logConnection = pipelineLogTable.getDatabaseMeta();
    String logTable = environmentSubstitute( pipelineLogTable.getActualTableName() );
    String logSchema = environmentSubstitute( pipelineLogTable.getActualSchemaName() );

    try {
      if ( logConnection != null ) {

        String logSchemaAndTable = logConnection.getQuotedSchemaTableCombination( logSchema, logTable );
        if ( Utils.isEmpty( logTable ) ) {
          // It doesn't make sense to start database logging without a table
          // to log to.
          throw new HopPipelineException( BaseMessages.getString( PKG, "Pipeline.Exception.NoLogTableDefined" ) );
        }
        if ( Utils.isEmpty( pipelineMeta.getName() ) && logTable != null ) {
          throw new HopException( BaseMessages.getString( PKG, "Pipeline.Exception.NoPipelineNameAvailableForLogging" ) );
        }
        pipelineLogTableDatabaseConnection = new Database( this, logConnection );
        pipelineLogTableDatabaseConnection.shareVariablesWith( this );
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.OpeningLogConnection", "" + logConnection ) );
        }
        pipelineLogTableDatabaseConnection.connect();
        pipelineLogTableDatabaseConnection.setCommit( logCommitSize );

        // See if we have to add a batch id...
        // Do this first, before anything else to lock the complete table exclusively
        //
        if ( pipelineLogTable.isBatchIdUsed() ) {
          Long id_batch =
            logConnection.getNextBatchId( pipelineLogTableDatabaseConnection, logSchema, logTable, pipelineLogTable
              .getKeyField().getFieldName() );
          setBatchId( id_batch.longValue() );
        }

        //
        // Get the date range from the logging table: from the last end_date to now. (currentDate)
        //
        Object[] lastr =
          pipelineLogTableDatabaseConnection.getLastLogDate( logSchemaAndTable, pipelineMeta.getName(), false,
            LogStatus.END );
        if ( lastr != null && lastr.length > 0 ) {
          startDate = (Date) lastr[ 0 ];
          if ( log.isDetailed() ) {
            log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.StartDateFound" ) + startDate );
          }
        }

        //
        // OK, we have a date-range.
        // However, perhaps we need to look at a table before we make a final judgment?
        //
        if ( pipelineMeta.getMaxDateConnection() != null && pipelineMeta.getMaxDateTable() != null && pipelineMeta
          .getMaxDateTable().length() > 0 && pipelineMeta.getMaxDateField() != null && pipelineMeta.getMaxDateField()
          .length() > 0 ) {
          if ( log.isDetailed() ) {
            log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.LookingForMaxdateConnection", "" + pipelineMeta
              .getMaxDateConnection() ) );
          }
          DatabaseMeta maxcon = pipelineMeta.getMaxDateConnection();
          if ( maxcon != null ) {
            Database maxdb = new Database( this, maxcon );
            maxdb.shareVariablesWith( this );
            try {
              if ( log.isDetailed() ) {
                log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.OpeningMaximumDateConnection" ) );
              }
              maxdb.connect();
              maxdb.setCommit( logCommitSize );

              //
              // Determine the endDate by looking at a field in a table...
              //
              String sql = "SELECT MAX(" + pipelineMeta.getMaxDateField() + ") FROM " + pipelineMeta.getMaxDateTable();
              RowMetaAndData r1 = maxdb.getOneRow( sql );
              if ( r1 != null ) {
                // OK, we have a value, what's the offset?
                Date maxvalue = r1.getRowMeta().getDate( r1.getData(), 0 );
                if ( maxvalue != null ) {
                  if ( log.isDetailed() ) {
                    log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.LastDateFoundOnTheMaxdateConnection" )
                      + r1 );
                  }
                  endDate.setTime( (long) ( maxvalue.getTime() + ( pipelineMeta.getMaxDateOffset() * 1000 ) ) );
                }
              } else {
                if ( log.isDetailed() ) {
                  log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.NoLastDateFoundOnTheMaxdateConnection" ) );
                }
              }
            } catch ( HopException e ) {
              throw new HopPipelineException( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorConnectingToDatabase",
                "" + pipelineMeta.getMaxDateConnection() ), e );
            } finally {
              maxdb.disconnect();
            }
          } else {
            throw new HopPipelineException( BaseMessages.getString( PKG,
              "Pipeline.Exception.MaximumDateConnectionCouldNotBeFound", "" + pipelineMeta.getMaxDateConnection() ) );
          }
        }

        // Determine the last date of all dependend tables...
        // Get the maximum in depdate...
        if ( pipelineMeta.nrDependencies() > 0 ) {
          if ( log.isDetailed() ) {
            log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.CheckingForMaxDependencyDate" ) );
          }
          //
          // Maybe one of the tables where this pipeline is dependent on has changed?
          // If so we need to change the start-date!
          //
          depDate = Const.MIN_DATE;
          Date maxdepdate = Const.MIN_DATE;
          if ( lastr != null && lastr.length > 0 ) {
            Date dep = (Date) lastr[ 1 ]; // #1: last depdate
            if ( dep != null ) {
              maxdepdate = dep;
              depDate = dep;
            }
          }

          for ( int i = 0; i < pipelineMeta.nrDependencies(); i++ ) {
            PipelineDependency td = pipelineMeta.getDependency( i );
            DatabaseMeta depcon = td.getDatabase();
            if ( depcon != null ) {
              Database depdb = new Database( this, depcon );
              try {
                depdb.connect();
                depdb.setCommit( logCommitSize );

                String sql = "SELECT MAX(" + td.getFieldname() + ") FROM " + td.getTablename();
                RowMetaAndData r1 = depdb.getOneRow( sql );
                if ( r1 != null ) {
                  // OK, we have a row, get the result!
                  Date maxvalue = (Date) r1.getData()[ 0 ];
                  if ( maxvalue != null ) {
                    if ( log.isDetailed() ) {
                      log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.FoundDateFromTable", td.getTablename(),
                        "." + td.getFieldname(), " = " + maxvalue.toString() ) );
                    }
                    if ( maxvalue.getTime() > maxdepdate.getTime() ) {
                      maxdepdate = maxvalue;
                    }
                  } else {
                    throw new HopPipelineException( BaseMessages.getString( PKG,
                      "Pipeline.Exception.UnableToGetDependencyInfoFromDB", td.getDatabase().getName() + ".", td
                        .getTablename() + ".", td.getFieldname() ) );
                  }
                } else {
                  throw new HopPipelineException( BaseMessages.getString( PKG,
                    "Pipeline.Exception.UnableToGetDependencyInfoFromDB", td.getDatabase().getName() + ".", td
                      .getTablename() + ".", td.getFieldname() ) );
                }
              } catch ( HopException e ) {
                throw new HopPipelineException( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorInDatabase", "" + td
                  .getDatabase() ), e );
              } finally {
                depdb.disconnect();
              }
            } else {
              throw new HopPipelineException( BaseMessages.getString( PKG, "Pipeline.Exception.ConnectionCouldNotBeFound",
                "" + td.getDatabase() ) );
            }
            if ( log.isDetailed() ) {
              log.logDetailed( BaseMessages.getString( PKG, "Pipeline.Log.Maxdepdate" ) + ( XmlHandler.date2string(
                maxdepdate ) ) );
            }
          }

          // OK, so we now have the maximum depdate;
          // If it is larger, it means we have to read everything back in again.
          // Maybe something has changed that we need!
          //
          if ( maxdepdate.getTime() > depDate.getTime() ) {
            depDate = maxdepdate;
            startDate = Const.MIN_DATE;
          }
        } else {
          depDate = currentDate;
        }
      }

      // OK, now we have a date-range. See if we need to set a maximum!
      if ( pipelineMeta.getMaxDateDifference() > 0.0 && // Do we have a difference specified?
        startDate.getTime() > Const.MIN_DATE.getTime() ) { // Is the startdate > Minimum?
        // See if the end-date is larger then Start_date + DIFF?
        Date maxdesired = new Date( startDate.getTime() + ( (long) pipelineMeta.getMaxDateDifference() * 1000 ) );

        // If this is the case: lower the end-date. Pick up the next 'region' next time around.
        // We do this to limit the workload in a single update session (e.g. for large fact tables)
        //
        if ( endDate.compareTo( maxdesired ) > 0 ) {
          endDate = maxdesired;
        }
      }

    } catch ( HopException e ) {
      throw new HopPipelineException( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorCalculatingDateRange",
        logTable ), e );
    }

    // Be careful, We DO NOT close the pipeline log table database connection!!!
    // It's closed later in beginProcessing() to prevent excessive connect/disconnect repetitions.

  }

  /**
   * Begin processing. Also handle logging operations related to the start of the pipeline
   *
   * @throws HopPipelineException the kettle pipeline exception
   */
  public void beginProcessing() throws HopPipelineException {
    PipelineLogTable pipelineLogTable = pipelineMeta.getPipelineLogTable();
    int intervalInSeconds = Const.toInt( environmentSubstitute( pipelineLogTable.getLogInterval() ), -1 );

    try {
      String logTable = pipelineLogTable.getActualTableName();

      try {
        if ( pipelineLogTableDatabaseConnection != null && !Utils.isEmpty( logTable ) && !Utils.isEmpty( pipelineMeta.getName() ) ) {
          pipelineLogTableDatabaseConnection.writeLogRecord( pipelineLogTable, LogStatus.START, this, null );

          // Pass in a commit to release transaction locks and to allow a user to actually see the log record.
          //
          if ( !pipelineLogTableDatabaseConnection.isAutoCommit() ) {
            pipelineLogTableDatabaseConnection.commitLog( true, pipelineLogTable );
          }

          // If we need to do periodic logging, make sure to install a timer for this...
          //
          if ( intervalInSeconds > 0 ) {
            final Timer timer = new Timer( getName() + " - interval logging timer" );
            TimerTask timerTask = new TimerTask() {
              @Override
              public void run() {
                try {
                  endProcessing();
                } catch ( Exception e ) {
                  log.logError( BaseMessages.getString( PKG, "Pipeline.Exception.UnableToPerformIntervalLogging" ), e );
                  // Also stop the show...
                  //
                  errors.incrementAndGet();
                  stopAll();
                }
              }
            };
            timer.schedule( timerTask, intervalInSeconds * 1000, intervalInSeconds * 1000 );

            addExecutionListener( new ExecutionAdapter<PipelineMeta>() {
              @Override
              public void finished( IPipelineEngine<PipelineMeta> pipeline ) {
                timer.cancel();
              }
            } );
          }

          // Add a listener to make sure that the last record is also written when pipeline finishes...
          //
          addExecutionListener( new ExecutionAdapter<PipelineMeta>() {
            @Override
            public void finished( IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
              try {
                endProcessing();

                lastWrittenTransformPerformanceSequenceNr =
                  writeTransformPerformanceLogRecords( lastWrittenTransformPerformanceSequenceNr, LogStatus.END );

              } catch ( HopException e ) {
                throw new HopException( BaseMessages.getString( PKG,
                  "Pipeline.Exception.UnableToPerformLoggingAtPipelineEnd" ), e );
              }
            }
          } );

        }

        // If we need to write out the transform logging information, do so at the end of the pipeline too...
        //
        TransformLogTable transformLogTable = pipelineMeta.getTransformLogTable();
        if ( transformLogTable.isDefined() ) {
          addExecutionListener( new ExecutionAdapter<PipelineMeta>() {
            @Override
            public void finished( IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
              try {
                writeTransformLogInformation();
              } catch ( HopException e ) {
                throw new HopException( BaseMessages.getString( PKG,
                  "Pipeline.Exception.UnableToPerformLoggingAtPipelineEnd" ), e );
              }
            }
          } );
        }

        // If we need to write the log channel hierarchy and lineage information, add a listener for that too...
        //
        ChannelLogTable channelLogTable = pipelineMeta.getChannelLogTable();
        if ( channelLogTable.isDefined() ) {
          addExecutionListener( new ExecutionAdapter<PipelineMeta>() {
            @Override
            public void finished( IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
              try {
                writeLogChannelInformation();
              } catch ( HopException e ) {
                throw new HopException( BaseMessages.getString( PKG,
                  "Pipeline.Exception.UnableToPerformLoggingAtPipelineEnd" ), e );
              }
            }
          } );
        }

        // See if we need to write the transform performance records at intervals too...
        //
        PerformanceLogTable performanceLogTable = pipelineMeta.getPerformanceLogTable();
        int perfLogInterval = Const.toInt( environmentSubstitute( performanceLogTable.getLogInterval() ), -1 );
        if ( performanceLogTable.isDefined() && perfLogInterval > 0 ) {
          final Timer timer = new Timer( getName() + " - transform performance log interval timer" );
          TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
              try {
                lastWrittenTransformPerformanceSequenceNr =
                  writeTransformPerformanceLogRecords( lastWrittenTransformPerformanceSequenceNr, LogStatus.RUNNING );
              } catch ( Exception e ) {
                log.logError( BaseMessages.getString( PKG,
                  "Pipeline.Exception.UnableToPerformIntervalPerformanceLogging" ), e );
                // Also stop the show...
                //
                errors.incrementAndGet();
                stopAll();
              }
            }
          };
          timer.schedule( timerTask, perfLogInterval * 1000, perfLogInterval * 1000 );

          addExecutionListener( new ExecutionAdapter<PipelineMeta>() {
            @Override
            public void finished( IPipelineEngine<PipelineMeta> pipeline ) {
              timer.cancel();
            }
          } );
        }
      } catch ( HopException e ) {
        throw new HopPipelineException( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorWritingLogRecordToTable",
          logTable ), e );
      } finally {
        // If we use interval logging, we keep the connection open for performance reasons...
        //
        if ( pipelineLogTableDatabaseConnection != null && ( intervalInSeconds <= 0 ) ) {
          pipelineLogTableDatabaseConnection.disconnect();
          pipelineLogTableDatabaseConnection = null;
        }
      }
    } catch ( HopException e ) {
      throw new HopPipelineException( BaseMessages.getString( PKG,
        "Pipeline.Exception.UnableToBeginProcessingPipeline" ), e );
    }
  }

  /**
   * Writes log channel information to a channel logging table (if one has been configured).
   *
   * @throws HopException if any errors occur during logging
   */
  protected void writeLogChannelInformation() throws HopException {
    Database db = null;
    ChannelLogTable channelLogTable = pipelineMeta.getChannelLogTable();

    // PDI-7070: If parent pipeline or workflow has the same channel logging info, don't duplicate log entries
    Pipeline t = getParentPipeline();
    if ( t != null ) {
      if ( channelLogTable.equals( t.getPipelineMeta().getChannelLogTable() ) ) {
        return;
      }
    }

    Workflow j = getParentWorkflow();

    if ( j != null ) {
      if ( channelLogTable.equals( j.getWorkflowMeta().getChannelLogTable() ) ) {
        return;
      }
    }
    // end PDI-7070

    try {
      db = new Database( this, channelLogTable.getDatabaseMeta() );
      db.shareVariablesWith( this );
      db.connect();
      db.setCommit( logCommitSize );

      List<LoggingHierarchy> loggingHierarchyList = getLoggingHierarchy();
      for ( LoggingHierarchy loggingHierarchy : loggingHierarchyList ) {
        db.writeLogRecord( channelLogTable, LogStatus.START, loggingHierarchy, null );
      }

      // Also time-out the log records in here...
      //
      db.cleanupLogRecords( channelLogTable );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG,
        "Pipeline.Exception.UnableToWriteLogChannelInformationToLogTable" ), e );
    } finally {
      disconnectDb( db );
    }
  }

  /**
   * Writes transform information to a transform logging table (if one has been configured).
   *
   * @throws HopException if any errors occur during logging
   */
  protected void writeTransformLogInformation() throws HopException {
    Database db = null;
    TransformLogTable transformLogTable = getPipelineMeta().getTransformLogTable();
    try {
      db = createDataBase( transformLogTable.getDatabaseMeta() );
      db.shareVariablesWith( this );
      db.connect();
      db.setCommit( logCommitSize );

      for ( TransformMetaDataCombi combi : getTransforms() ) {
        db.writeLogRecord( transformLogTable, LogStatus.START, combi, null );
      }

      db.cleanupLogRecords( transformLogTable );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG,
        "Pipeline.Exception.UnableToWriteTransformMetaToLogTable" ), e );
    } finally {
      disconnectDb( db );
    }

  }

  protected Database createDataBase( DatabaseMeta meta ) {
    return new Database( this, meta );
  }

  protected synchronized void writeMetricsInformation() throws HopException {
    Preconditions.checkNotNull( log );
    List<MetricsDuration> metricsList =
      MetricsUtil.getDuration( log.getLogChannelId(), Metrics.METRIC_PLUGIN_REGISTRY_REGISTER_EXTENSIONS_START );
    if ( ( log.isDebug() ) && !metricsList.isEmpty() ) {
      log.logDebug( metricsList.get( 0 ).toString() );
    }

    metricsList =
      MetricsUtil.getDuration( log.getLogChannelId(), Metrics.METRIC_PLUGIN_REGISTRY_PLUGIN_REGISTRATION_START );
    if ( ( log != null ) && ( log.isDebug() ) && !metricsList.isEmpty() ) {
      log.logDebug( metricsList.get( 0 ).toString() );
    }

    long total = 0;
    metricsList =
      MetricsUtil.getDuration( log.getLogChannelId(), Metrics.METRIC_PLUGIN_REGISTRY_PLUGIN_TYPE_REGISTRATION_START );
    if ( ( log != null ) && ( log.isDebug() ) && metricsList != null && !metricsList.isEmpty() ) {
      for ( MetricsDuration duration : metricsList ) {
        total += duration.getDuration();
        log.logDebug( "   - " + duration.toString() + "  Total=" + total );
      }
    }

    Database db = null;
    MetricsLogTable metricsLogTable = pipelineMeta.getMetricsLogTable();
    try {
      db = new Database( this, metricsLogTable.getDatabaseMeta() );
      db.shareVariablesWith( this );
      db.connect();
      db.setCommit( logCommitSize );

      List<String> logChannelIds = LoggingRegistry.getInstance().getLogChannelChildren( getLogChannelId() );
      for ( String logChannelId : logChannelIds ) {
        Queue<IMetricsSnapshot> snapshotList =
          MetricsRegistry.getInstance().getSnapshotLists().get( logChannelId );
        if ( snapshotList != null ) {
          Iterator<IMetricsSnapshot> iterator = snapshotList.iterator();
          while ( iterator.hasNext() ) {
            IMetricsSnapshot snapshot = iterator.next();
            db.writeLogRecord( metricsLogTable, LogStatus.START, new LoggingMetric( batchId, snapshot ), null );
          }
        }

        Map<String, IMetricsSnapshot> snapshotMap =
          MetricsRegistry.getInstance().getSnapshotMaps().get( logChannelId );
        if ( snapshotMap != null ) {
          synchronized ( snapshotMap ) {
            Iterator<IMetricsSnapshot> iterator = snapshotMap.values().iterator();
            while ( iterator.hasNext() ) {
              IMetricsSnapshot snapshot = iterator.next();
              db.writeLogRecord( metricsLogTable, LogStatus.START, new LoggingMetric( batchId, snapshot ), null );
            }
          }
        }
      }

      // Also time-out the log records in here...
      //
      db.cleanupLogRecords( metricsLogTable );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG,
        "Pipeline.Exception.UnableToWriteMetricsInformationToLogTable" ), e );
    } finally {
      disconnectDb( db );
    }
  }

  private void disconnectDb( Database db ) throws HopDatabaseException {
    if ( db == null ) {
      return;
    }
    if ( !db.isAutoCommit() ) {
      db.commit( true );
    }
    db.disconnect();
  }

  /**
   * Gets the result of the pipeline. The Result object contains such measures as the number of errors, number of
   * lines read/written/input/output/updated/rejected, etc.
   *
   * @return the Result object containing resulting measures from execution of the pipeline
   */
  public Result getResult() {
    if ( transforms == null ) {
      return null;
    }

    Result result = new Result();
    result.setNrErrors( errors.longValue() );
    result.setResult( errors.longValue() == 0 );
    PipelineLogTable pipelineLogTable = pipelineMeta.getPipelineLogTable();

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform transform = sid.transform;

      result.setNrErrors( result.getNrErrors() + sid.transform.getErrors() );
      result.getResultFiles().putAll( transform.getResultFiles() );

      if ( transform.isSafeStopped() ) {
        result.setSafeStop( transform.isSafeStopped() );
      }

      if ( transform.getTransformName().equals( pipelineLogTable.getSubjectString( PipelineLogTable.ID.LINES_READ ) ) ) {
        result.setNrLinesRead( result.getNrLinesRead() + transform.getLinesRead() );
      }
      if ( transform.getTransformName().equals( pipelineLogTable.getSubjectString( PipelineLogTable.ID.LINES_INPUT ) ) ) {
        result.setNrLinesInput( result.getNrLinesInput() + transform.getLinesInput() );
      }
      if ( transform.getTransformName().equals( pipelineLogTable.getSubjectString( PipelineLogTable.ID.LINES_WRITTEN ) ) ) {
        result.setNrLinesWritten( result.getNrLinesWritten() + transform.getLinesWritten() );
      }
      if ( transform.getTransformName().equals( pipelineLogTable.getSubjectString( PipelineLogTable.ID.LINES_OUTPUT ) ) ) {
        result.setNrLinesOutput( result.getNrLinesOutput() + transform.getLinesOutput() );
      }
      if ( transform.getTransformName().equals( pipelineLogTable.getSubjectString( PipelineLogTable.ID.LINES_UPDATED ) ) ) {
        result.setNrLinesUpdated( result.getNrLinesUpdated() + transform.getLinesUpdated() );
      }
      if ( transform.getTransformName().equals( pipelineLogTable.getSubjectString( PipelineLogTable.ID.LINES_REJECTED ) ) ) {
        result.setNrLinesRejected( result.getNrLinesRejected() + transform.getLinesRejected() );
      }
    }

    result.setRows( resultRows );
    if ( !Utils.isEmpty( resultFiles ) ) {
      result.setResultFiles( new HashMap<String, ResultFile>() );
      for ( ResultFile resultFile : resultFiles ) {
        result.getResultFiles().put( resultFile.toString(), resultFile );
      }
    }
    result.setStopped( isStopped() );
    result.setLogChannelId( log.getLogChannelId() );

    return result;
  }

  /**
   * End processing. Also handle any logging operations associated with the end of a pipeline
   *
   * @return true if all end processing is successful, false otherwise
   * @throws HopException if any errors occur during processing
   */
  private synchronized boolean endProcessing() throws HopException {
    LogStatus status;

    if ( isFinished() ) {
      if ( isStopped() ) {
        status = LogStatus.STOP;
      } else {
        status = LogStatus.END;
      }
    } else if ( isPaused() ) {
      status = LogStatus.PAUSED;
    } else {
      status = LogStatus.RUNNING;
    }

    PipelineLogTable pipelineLogTable = pipelineMeta.getPipelineLogTable();
    int intervalInSeconds = Const.toInt( environmentSubstitute( pipelineLogTable.getLogInterval() ), -1 );

    logDate = new Date();

    // OK, we have some logging to do...
    //
    DatabaseMeta logcon = pipelineMeta.getPipelineLogTable().getDatabaseMeta();
    String logTable = pipelineMeta.getPipelineLogTable().getActualTableName();
    if ( logcon != null ) {
      Database ldb = null;

      try {
        // Let's not reconnect/disconnect all the time for performance reasons!
        //
        if ( pipelineLogTableDatabaseConnection == null ) {
          ldb = new Database( this, logcon );
          ldb.shareVariablesWith( this );
          ldb.connect();
          ldb.setCommit( logCommitSize );
          pipelineLogTableDatabaseConnection = ldb;
        } else {
          ldb = pipelineLogTableDatabaseConnection;
        }

        // Write to the standard pipeline log table...
        //
        if ( !Utils.isEmpty( logTable ) ) {
          ldb.writeLogRecord( pipelineLogTable, status, this, null );
        }

        // Also time-out the log records in here...
        //
        if ( status.equals( LogStatus.END ) || status.equals( LogStatus.STOP ) ) {
          ldb.cleanupLogRecords( pipelineLogTable );
        }

        // Commit the operations to prevent locking issues
        //
        if ( !ldb.isAutoCommit() ) {
          ldb.commitLog( true, pipelineMeta.getPipelineLogTable() );
        }
      } catch ( HopDatabaseException e ) {
        // PDI-9790 error write to log db is transaction error
        log.logError( BaseMessages.getString( PKG, "Database.Error.WriteLogTable", logTable ), e );
        errors.incrementAndGet();
        // end PDI-9790
      } catch ( Exception e ) {
        throw new HopException( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorWritingLogRecordToTable",
          pipelineMeta.getPipelineLogTable().getActualTableName() ), e );
      } finally {
        if ( intervalInSeconds <= 0 || ( status.equals( LogStatus.END ) || status.equals( LogStatus.STOP ) ) ) {
          ldb.disconnect();
          pipelineLogTableDatabaseConnection = null; // disconnected
        }
      }
    }
    return true;
  }

  /**
   * Write transform performance log records.
   *
   * @param startSequenceNr the start sequence numberr
   * @param status          the logging status. If this is End, perform cleanup
   * @return the new sequence number
   * @throws HopException if any errors occur during logging
   */
  private int writeTransformPerformanceLogRecords( int startSequenceNr, LogStatus status ) throws HopException {
    int lastSeqNr = 0;
    Database ldb = null;
    PerformanceLogTable performanceLogTable = pipelineMeta.getPerformanceLogTable();

    if ( !performanceLogTable.isDefined() || !pipelineMeta.isCapturingTransformPerformanceSnapShots()
      || transformPerformanceSnapShots == null || transformPerformanceSnapShots.isEmpty() ) {
      return 0; // nothing to do here!
    }

    try {
      ldb = new Database( this, performanceLogTable.getDatabaseMeta() );
      ldb.shareVariablesWith( this );
      ldb.connect();
      ldb.setCommit( logCommitSize );

      // Write to the transform performance log table...
      //
      IRowMeta rowMeta = performanceLogTable.getLogRecord( LogStatus.START, null, null ).getRowMeta();
      ldb.prepareInsert( rowMeta, performanceLogTable.getActualSchemaName(), performanceLogTable.getActualTableName() );

      synchronized ( transformPerformanceSnapShots ) {
        Iterator<List<PerformanceSnapShot>> iterator = transformPerformanceSnapShots.values().iterator();
        while ( iterator.hasNext() ) {
          List<PerformanceSnapShot> snapshots = iterator.next();
          synchronized ( snapshots ) {
            Iterator<PerformanceSnapShot> snapshotsIterator = snapshots.iterator();
            while ( snapshotsIterator.hasNext() ) {
              PerformanceSnapShot snapshot = snapshotsIterator.next();
              if ( snapshot.getSeqNr() >= startSequenceNr && snapshot
                .getSeqNr() <= lastTransformPerformanceSnapshotSeqNrAdded ) {

                RowMetaAndData row = performanceLogTable.getLogRecord( LogStatus.START, snapshot, null );

                ldb.setValuesInsert( row.getRowMeta(), row.getData() );
                ldb.insertRow( true );
              }
              lastSeqNr = snapshot.getSeqNr();
            }
          }
        }
      }

      ldb.insertFinished( true );

      // Finally, see if the log table needs cleaning up...
      //
      if ( status.equals( LogStatus.END ) ) {
        ldb.cleanupLogRecords( performanceLogTable );
      }

    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG,
        "Pipeline.Exception.ErrorWritingTransformPerformanceLogRecordToTable" ), e );
    } finally {
      if ( ldb != null ) {
        ldb.disconnect();
      }
    }

    return lastSeqNr + 1;
  }

  /**
   * Close unique database connections. If there are errors in the Result, perform a rollback
   *
   * @param result the result of the pipeline execution
   */
  @Override
  public void closeUniqueDatabaseConnections( Result result ) {

    // Don't close any connections if the parent workflow is using the same transaction
    //
    if ( parentWorkflow != null && transactionId != null && parentWorkflow.getTransactionId() != null && transactionId.equals(
      parentWorkflow.getTransactionId() ) ) {
      return;
    }

    // Don't close any connections if the parent pipeline is using the same transaction
    //
    if ( parentPipeline != null && parentPipeline.getPipelineMeta().isUsingUniqueConnections() && transactionId != null
      && parentPipeline.getTransactionId() != null && transactionId.equals( parentPipeline.getTransactionId() ) ) {
      return;
    }

    // First we get all the database connections ...
    //
    DatabaseConnectionMap map = DatabaseConnectionMap.getInstance();
    synchronized ( map ) {
      List<Database> databaseList = new ArrayList<>( map.getMap().values() );
      for ( Database database : databaseList ) {
        if ( database.getConnectionGroup().equals( getTransactionId() ) ) {
          try {
            // This database connection belongs to this pipeline.
            // Let's roll it back if there is an error...
            //
            if ( result.getNrErrors() > 0 ) {
              try {
                database.rollback( true );
                log.logBasic( BaseMessages.getString( PKG, "Pipeline.Exception.TransactionsRolledBackOnConnection",
                  database.toString() ) );
              } catch ( Exception e ) {
                throw new HopDatabaseException( BaseMessages.getString( PKG,
                  "Pipeline.Exception.ErrorRollingBackUniqueConnection", database.toString() ), e );
              }
            } else {
              try {
                database.commit( true );
                log.logBasic( BaseMessages.getString( PKG, "Pipeline.Exception.TransactionsCommittedOnConnection", database
                  .toString() ) );
              } catch ( Exception e ) {
                throw new HopDatabaseException( BaseMessages.getString( PKG,
                  "Pipeline.Exception.ErrorCommittingUniqueConnection", database.toString() ), e );
              }
            }
          } catch ( Exception e ) {
            log.logError( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorHandlingPipelineTransaction",
              database.toString() ), e );
            result.setNrErrors( result.getNrErrors() + 1 );
          } finally {
            try {
              // This database connection belongs to this pipeline.
              database.closeConnectionOnly();
            } catch ( Exception e ) {
              log.logError( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorHandlingPipelineTransaction",
                database.toString() ), e );
              result.setNrErrors( result.getNrErrors() + 1 );
            } finally {
              // Remove the database from the list...
              //
              map.removeConnection( database.getConnectionGroup(), database.getPartitionId(), database );
            }
          }
        }
      }

      // Who else needs to be informed of the rollback or commit?
      //
      List<IDatabaseTransaction> transactionListeners = map.getTransactionListeners( getTransactionId() );
      if ( result.getNrErrors() > 0 ) {
        for ( IDatabaseTransaction listener : transactionListeners ) {
          try {
            listener.rollback();
          } catch ( Exception e ) {
            log.logError( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorHandlingTransactionListenerRollback" ),
              e );
            result.setNrErrors( result.getNrErrors() + 1 );
          }
        }
      } else {
        for ( IDatabaseTransaction listener : transactionListeners ) {
          try {
            listener.commit();
          } catch ( Exception e ) {
            log.logError( BaseMessages.getString( PKG, "Pipeline.Exception.ErrorHandlingTransactionListenerCommit" ), e );
            result.setNrErrors( result.getNrErrors() + 1 );
          }
        }
      }

    }
  }

  /**
   * Find the run thread for the transform with the specified name.
   *
   * @param transformName the transform name
   * @return a ITransform object corresponding to the run thread for the specified transform
   */
  public ITransform findRunThread( String transformName ) {
    if ( transforms == null ) {
      return null;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform transform = sid.transform;
      if ( transform.getTransformName().equalsIgnoreCase( transformName ) ) {
        return transform;
      }
    }
    return null;
  }

  /**
   * Find the base transforms for the transform with the specified name.
   *
   * @param transformName the transform name
   * @return the list of base transforms for the specified transform
   */
  public List<ITransform> findBaseTransforms( String transformName ) {
    List<ITransform> baseTransforms = new ArrayList<>();

    if ( transforms == null ) {
      return baseTransforms;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform iTransform = sid.transform;
      if ( iTransform.getTransformName().equalsIgnoreCase( transformName ) ) {
        baseTransforms.add( iTransform );
      }
    }
    return baseTransforms;
  }

  /**
   * Find the executing transform copy for the transform with the specified name and copy number
   *
   * @param transformName the transform name
   * @param copyNr
   * @return the executing transform found or null if no copy could be found.
   */
  public ITransform findTransformInterface( String transformName, int copyNr ) {
    if ( transforms == null ) {
      return null;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform iTransform = sid.transform;
      if ( iTransform.getTransformName().equalsIgnoreCase( transformName ) && sid.copy == copyNr ) {
        return iTransform;
      }
    }
    return null;
  }

  /**
   * Find the available executing transform copies for the transform with the specified name
   *
   * @param transformName the transform name
   * @return the list of executing transform copies found or null if no transforms are available yet (incorrect usage)
   */
  public List<ITransform> findTransformInterfaces( String transformName ) {
    if ( transforms == null ) {
      return null;
    }

    List<ITransform> list = new ArrayList<>();

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform iTransform = sid.transform;
      if ( iTransform.getTransformName().equalsIgnoreCase( transformName ) ) {
        list.add( iTransform );
      }
    }
    return list;
  }

  /**
   * Find the data interface for the transform with the specified name.
   *
   * @param name the transform name
   * @return the transform data interface
   */
  public ITransformData findDataInterface( String name ) {
    if ( transforms == null ) {
      return null;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      ITransform rt = sid.transform;
      if ( rt.getTransformName().equalsIgnoreCase( name ) ) {
        return sid.data;
      }
    }
    return null;
  }

  /**
   * Gets the start date/time object for the pipeline.
   *
   * @return Returns the startDate.
   */
  public Date getStartDate() {
    return startDate;
  }

  /**
   * Gets the end date/time object for the pipeline.
   *
   * @return Returns the endDate.
   */
  public Date getEndDate() {
    return endDate;
  }

  /**
   * Gets sortingTransformsTopologically
   *
   * @return value of sortingTransformsTopologically
   */
  public boolean isSortingTransformsTopologically() {
    return sortingTransformsTopologically;
  }

  /**
   * @param sortingTransformsTopologically The sortingTransformsTopologically to set
   */
  public void setSortingTransformsTopologically( boolean sortingTransformsTopologically ) {
    this.sortingTransformsTopologically = sortingTransformsTopologically;
  }

  /**
   * Gets the meta-data for the pipeline.
   *
   * @return Returns the pipeline meta-data
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * Sets the meta-data for the pipeline.
   *
   * @param pipelineMeta The pipeline meta-data to set.
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * Gets the current date/time object.
   *
   * @return the current date
   */
  public Date getCurrentDate() {
    return currentDate;
  }

  /**
   * Gets the dependency date for the pipeline. A pipeline can have a list of dependency fields. If any of
   * these fields have a maximum date higher than the dependency date of the last run, the date range is set to to (-oo,
   * now). The use-case is the incremental population of Slowly Changing Dimensions (SCD).
   *
   * @return Returns the dependency date
   */
  public Date getDepDate() {
    return depDate;
  }

  /**
   * Gets the date the pipeline was logged.
   *
   * @return the log date
   */
  public Date getLogDate() {
    return logDate;
  }

  /**
   * Gets the rowsets for the pipeline.
   *
   * @return a list of rowsets
   */
  public List<IRowSet> getRowsets() {
    return rowsets;
  }

  /**
   * Gets a list of transforms in the pipeline.
   *
   * @return a list of the transforms in the pipeline
   */
  public List<TransformMetaDataCombi<ITransform, ITransformMeta, ITransformData>> getTransforms() {
    return transforms;
  }

  protected void setTransforms( List<TransformMetaDataCombi<ITransform, ITransformMeta, ITransformData>> transforms ) {
    this.transforms = transforms;
  }

  /**
   * Gets a string representation of the pipeline.
   *
   * @return the string representation of the pipeline
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    if ( pipelineMeta == null || pipelineMeta.getName() == null ) {
      return getClass().getSimpleName();
    }

    // See if there is a parent pipeline. If so, print the name of the parent here as well...
    //
    StringBuilder string = new StringBuilder( 50 );

    // If we're running as a mapping, we get a reference to the calling (parent) pipeline as well...
    //
    if ( getParentPipeline() != null ) {
      string.append( '[' ).append( getParentPipeline().toString() ).append( ']' ).append( '.' );
    }

    // When we run a mapping we also set a mapping transform name in there...
    //
    if ( !Utils.isEmpty( mappingTransformName ) ) {
      string.append( '[' ).append( mappingTransformName ).append( ']' ).append( '.' );
    }

    string.append( pipelineMeta.getName() );

    return string.toString();
  }

  /**
   * Gets the mapping inputs for each transform in the pipeline.
   *
   * @return an array of MappingInputs
   */
  public MappingInput[] findMappingInput() {
    if ( transforms == null ) {
      return null;
    }

    List<MappingInput> list = new ArrayList<>();

    // Look in threads and find the MappingInput transform thread...
    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi smdc = transforms.get( i );
      ITransform transform = smdc.transform;
      if ( transform.getTransformPluginId().equalsIgnoreCase( "MappingInput" ) ) {
        list.add( (MappingInput) transform );
      }
    }
    return list.toArray( new MappingInput[ list.size() ] );
  }

  /**
   * Gets the mapping outputs for each transform in the pipeline.
   *
   * @return an array of MappingOutputs
   */
  public MappingOutput[] findMappingOutput() {
    List<MappingOutput> list = new ArrayList<>();

    if ( transforms != null ) {
      // Look in threads and find the MappingInput transform thread...
      for ( int i = 0; i < transforms.size(); i++ ) {
        TransformMetaDataCombi smdc = transforms.get( i );
        ITransform transform = smdc.transform;
        if ( transform.getTransformPluginId().equalsIgnoreCase( "MappingOutput" ) ) {
          list.add( (MappingOutput) transform );
        }
      }
    }
    return list.toArray( new MappingOutput[ list.size() ] );
  }

  /**
   * Find the ITransform (thread) by looking it up using the name.
   *
   * @param transformName The name of the transform to look for
   * @param copy     the copy number of the transform to look for
   * @return the ITransform or null if nothing was found.
   */
  public ITransform getTransformInterface( String transformName, int copy ) {
    if ( transforms == null ) {
      return null;
    }

    // Now start all the threads...
    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      if ( sid.transformName.equalsIgnoreCase( transformName ) && sid.copy == copy ) {
        return sid.transform;
      }
    }

    return null;
  }

  /**
   * Turn on safe mode during running: the pipeline will run slower but with more checking enabled.
   *
   * @param safeModeEnabled true for safe mode
   */
  public void setSafeModeEnabled( boolean safeModeEnabled ) {
    this.safeModeEnabled = safeModeEnabled;
  }

  /**
   * Checks whether safe mode is enabled.
   *
   * @return Returns true if the safe mode is enabled: the pipeline will run slower but with more checking enabled
   */
  public boolean isSafeModeEnabled() {
    return safeModeEnabled;
  }

  /**
   * This adds a row producer to the pipeline that just got set up. It is preferable to run this BEFORE execute()
   * but after prepareExecution()
   *
   * @param transformName The transform to produce rows for
   * @param copynr   The copynr of the transform to produce row for (normally 0 unless you have multiple copies running)
   * @return the row producer
   * @throws HopException in case the thread/transform to produce rows for could not be found.
   * @see Pipeline#execute()
   * @see Pipeline#prepareExecution()
   */
  public RowProducer addRowProducer( String transformName, int copynr ) throws HopException {
    ITransform iTransform = getTransformInterface( transformName, copynr );
    if ( iTransform == null ) {
      throw new HopException( "Unable to find thread with name " + transformName + " and copy number " + copynr );
    }

    // We are going to add an extra IRowSet to this iTransform.
    IRowSet rowSet;
    switch ( pipelineMeta.getPipelineType() ) {
      case Normal:
        rowSet = new BlockingRowSet( rowSetSize );
        break;
      case SingleThreaded:
        rowSet = new QueueRowSet();
        break;
      default:
        throw new HopException( "Unhandled pipeline type: " + pipelineMeta.getPipelineType() );
    }

    // Add this rowset to the list of active rowsets for the selected transform
    iTransform.addRowSetToInputRowSets( rowSet );

    return new RowProducer( iTransform, rowSet );
  }

  /**
   * Gets the parent workflow, or null if there is no parent.
   *
   * @return the parent workflow, or null if there is no parent
   */
  public Workflow getParentWorkflow() {
    return parentWorkflow;
  }

  /**
   * Sets the parent workflow for the pipeline.
   *
   * @param parentWorkflow The parent workflow to set
   */
  public void setParentWorkflow( Workflow parentWorkflow ) {
    this.logLevel = parentWorkflow.getLogLevel();
    this.log.setLogLevel( logLevel );
    this.parentWorkflow = parentWorkflow;

    transactionId = calculateTransactionId();
  }

  /**
   * Finds the ITransformData (currently) associated with the specified transform.
   *
   * @param transformName The name of the transform to look for
   * @param transformcopy The copy number (0 based) of the transform
   * @return The ITransformData or null if non found.
   */
  public ITransformData getTransformDataInterface( String transformName, int transformcopy ) {
    if ( transforms == null ) {
      return null;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      if ( sid.transformName.equals( transformName ) && sid.copy == transformcopy ) {
        return sid.data;
      }
    }
    return null;
  }

  /**
   * Checks whether the pipeline has any components that are halted.
   *
   * @return true if one or more components are halted, false otherwise
   */
  @Override
  public boolean hasHaltedComponents() {
    // not yet 100% sure of this, if there are no transforms... or none halted?
    if ( transforms == null ) {
      return false;
    }

    for ( int i = 0; i < transforms.size(); i++ ) {
      TransformMetaDataCombi sid = transforms.get( i );
      if ( sid.data.getStatus() == TransformExecutionStatus.STATUS_HALTED ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the workflow start date.
   *
   * @return the workflow start date
   */
  public Date getJobStartDate() {
    return jobStartDate;
  }

  /**
   * Gets the workflow end date.
   *
   * @return the workflow end date
   */
  public Date getJobEndDate() {
    return jobEndDate;
  }

  /**
   * Sets the workflow end date.
   *
   * @param jobEndDate the jobEndDate to set
   */
  public void setJobEndDate( Date jobEndDate ) {
    this.jobEndDate = jobEndDate;
  }

  /**
   * Sets the workflow start date.
   *
   * @param jobStartDate the jobStartDate to set
   */
  public void setJobStartDate( Date jobStartDate ) {
    this.jobStartDate = jobStartDate;
  }

  /**
   * Get the batch ID that is passed from the parent workflow to the pipeline. If nothing is passed, it's the
   * pipeline's batch ID
   *
   * @return the parent workflow's batch ID, or the pipeline's batch ID if there is no parent workflow
   */
  public long getPassedBatchId() {
    return passedBatchId;
  }

  /**
   * Sets the passed batch ID of the pipeline from the batch ID of the parent workflow.
   *
   * @param jobBatchId the jobBatchId to set
   */
  public void setPassedBatchId( long jobBatchId ) {
    this.passedBatchId = jobBatchId;
  }

  /**
   * Gets the batch ID of the pipeline.
   *
   * @return the batch ID of the pipeline
   */
  public long getBatchId() {
    return batchId;
  }

  /**
   * Sets the batch ID of the pipeline.
   *
   * @param batchId the batch ID to set
   */
  public void setBatchId( long batchId ) {
    this.batchId = batchId;
  }

  /**
   * Gets the status of the pipeline (Halting, Finished, Paused, etc.)
   *
   * @return the status of the pipeline
   */
  public String getStatus() {
    String message;

    if ( isRunning() ) {
      if ( isStopped() ) {
        message = STRING_HALTING;
      } else {
        if ( isPaused() ) {
          message = STRING_PAUSED;
        } else {
          message = STRING_RUNNING;
        }
      }
    } else if ( isFinished() ) {
      message = STRING_FINISHED;
      if ( getResult().getNrErrors() > 0 ) {
        message += " (with errors)";
      }
    } else if ( isStopped() ) {
      message = STRING_STOPPED;
    } else if ( isPreparing() ) {
      message = STRING_PREPARING;
    } else if ( isInitializing() ) {
      message = STRING_INITIALIZING;
    } else {
      message = STRING_WAITING;
    }

    return message;
  }

  /**
   * Checks whether the pipeline is initializing.
   *
   * @return true if the pipeline is initializing, false otherwise
   */
  public boolean isInitializing() {
    int exist = status.get() & INITIALIZING.mask;
    return exist != 0;
  }

  /**
   * Sets whether the pipeline is initializing.
   *
   * @param initializing true if the pipeline is initializing, false otherwise
   */
  public void setInitializing( boolean initializing ) {
    status.updateAndGet( v -> initializing ? v | INITIALIZING.mask : ( BIT_STATUS_SUM ^ INITIALIZING.mask ) & v );
  }

  /**
   * Checks whether the pipeline is preparing for execution.
   *
   * @return true if the pipeline is preparing for execution, false otherwise
   */
  public boolean isPreparing() {
    int exist = status.get() & PREPARING.mask;
    return exist != 0;
  }

  /**
   * Sets whether the pipeline is preparing for execution.
   *
   * @param preparing true if the pipeline is preparing for execution, false otherwise
   */
  public void setPreparing( boolean preparing ) {
    status.updateAndGet( v -> preparing ? v | PREPARING.mask : ( BIT_STATUS_SUM ^ PREPARING.mask ) & v );
  }

  /**
   * Checks whether the pipeline is running.
   *
   * @return true if the pipeline is running, false otherwise
   */
  public boolean isRunning() {
    int exist = status.get() & RUNNING.mask;
    return exist != 0;
  }

  /**
   * Sets whether the pipeline is running.
   *
   * @param running true if the pipeline is running, false otherwise
   */
  public void setRunning( boolean running ) {
    status.updateAndGet( v -> running ? v | RUNNING.mask : ( BIT_STATUS_SUM ^ RUNNING.mask ) & v );
  }


  /**
   * Send the pipeline for execution to a HopServer slave server.
   *
   * @param pipelineMeta              the pipeline meta-data
   * @param executionConfiguration the pipeline execution configuration
   * @return The HopServer object ID on the server.
   * @throws HopException if any errors occur during the dispatch to the slave server
   */
  public static String sendToSlaveServer( PipelineMeta pipelineMeta, PipelineExecutionConfiguration executionConfiguration,
                                          IMetaStore metaStore ) throws HopException {
    String carteObjectId;
    SlaveServer slaveServer = executionConfiguration.getRemoteServer();

    if ( slaveServer == null ) {
      throw new HopException( "No slave server specified" );
    }
    if ( Utils.isEmpty( pipelineMeta.getName() ) ) {
      throw new HopException( "The pipeline needs a name to uniquely identify it by on the remote server." );
    }

    // Inject certain internal variables to make it more intuitive.
    //
    Map<String, String> vars = new HashMap<>();

    for ( String var : Const.INTERNAL_PIPELINE_VARIABLES ) {
      vars.put( var, pipelineMeta.getVariable( var ) );
    }
    for ( String var : Const.INTERNAL_WORKFLOW_VARIABLES ) {
      vars.put( var, pipelineMeta.getVariable( var ) );
    }

    executionConfiguration.getVariablesMap().putAll( vars );
    slaveServer.injectVariables( executionConfiguration.getVariablesMap() );

    slaveServer.getLogChannel().setLogLevel( executionConfiguration.getLogLevel() );

    try {
      if ( executionConfiguration.isPassingExport() ) {

        // First export the workflow...
        //
        FileObject tempFile = HopVFS.createTempFile( "pipelineExport", HopVFS.Suffix.ZIP, pipelineMeta );

        // The executionConfiguration should not include external references here because all the resources should be
        // retrieved from the exported zip file
        // TODO: Serialize metastore objects to JSON (Kettle Beam project) and include it in the zip file
        //
        PipelineExecutionConfiguration clonedConfiguration = (PipelineExecutionConfiguration) executionConfiguration.clone();
        TopLevelResource topLevelResource =
          ResourceUtil.serializeResourceExportInterface( tempFile.getName().toString(), pipelineMeta, pipelineMeta,
            metaStore, clonedConfiguration.getXml(), CONFIGURATION_IN_EXPORT_FILENAME );

        // Send the zip file over to the slave server...
        //
        String result = slaveServer.sendExport(
          topLevelResource.getArchiveName(),
          RegisterPackageServlet.TYPE_PIPELINE,
          topLevelResource.getBaseResourceName() );
        WebResult webResult = WebResult.fromXMLString( result );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error passing the exported pipeline to the remote server: "
            + Const.CR + webResult.getMessage() );
        }
        carteObjectId = webResult.getId();
      } else {

        // Now send it off to the remote server...
        //
        String xml = new PipelineConfiguration( pipelineMeta, executionConfiguration ).getXml();
        String reply = slaveServer.sendXML( xml, RegisterPipelineServlet.CONTEXT_PATH + "/?xml=Y" );
        WebResult webResult = WebResult.fromXMLString( reply );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error posting the pipeline on the remote server: " + Const.CR
            + webResult.getMessage() );
        }
        carteObjectId = webResult.getId();
      }

      // Prepare the pipeline
      //
      String reply =
        slaveServer.execService( PrepareExecutionPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineMeta
          .getName(), "UTF-8" ) + "&xml=Y&id=" + carteObjectId );
      WebResult webResult = WebResult.fromXMLString( reply );
      if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
        throw new HopException( "There was an error preparing the pipeline for excution on the remote server: "
          + Const.CR + webResult.getMessage() );
      }

      // Start the pipeline
      //
      reply =
        slaveServer.execService( StartExecutionPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineMeta
          .getName(), "UTF-8" ) + "&xml=Y&id=" + carteObjectId );
      webResult = WebResult.fromXMLString( reply );

      if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
        throw new HopException( "There was an error starting the pipeline on the remote server: " + Const.CR
          + webResult.getMessage() );
      }

      return carteObjectId;
    } catch ( HopException ke ) {
      throw ke;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  /**
   * Checks whether the pipeline is ready to start (i.e. execution preparation was successful)
   *
   * @return true if the pipeline was prepared for execution successfully, false otherwise
   * @see Pipeline#prepareExecution()
   */
  public boolean isReadyToStart() {
    return readyToStart;
  }

  protected void setReadyToStart( boolean ready ) {
    readyToStart = ready;
  }


  /**
   * Sets the internal kettle variables.
   *
   * @param var the new internal kettle variables
   */
  public void setInternalHopVariables( IVariables var ) {
    boolean hasFilename = pipelineMeta != null && !Utils.isEmpty( pipelineMeta.getFilename() );
    if ( hasFilename ) { // we have a finename that's defined.
      try {
        FileObject fileObject = HopVFS.getFileObject( pipelineMeta.getFilename(), var );
        FileName fileName = fileObject.getName();

        // The filename of the pipeline
        variables.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME, fileName.getBaseName() );

        // The directory of the pipeline
        FileName fileDir = fileName.getParent();
        variables.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, fileDir.getURI() );
      } catch ( HopFileException e ) {
        variables.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "" );
        variables.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME, "" );
      }
    } else {
      variables.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "" );
      variables.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME, "" );
    }

    // The name of the pipeline
    variables.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_NAME, Const.NVL( pipelineMeta.getName(), "" ) );

    // Here we don't clear the definition of the workflow specific parameters, as they may come in handy.
    // A pipeline can be called from a workflow and may inherit the workflow internal variables
    // but the other around is not possible.

    setInternalEntryCurrentDirectory( hasFilename );

  }

  protected void setInternalEntryCurrentDirectory( boolean hasFilename ) {
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, variables.getVariable(
      hasFilename
        ? Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY
        : Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }


  /**
   * Copies variables from a given variable space to this pipeline.
   *
   * @param variables the variable space
   * @see IVariables#copyVariablesFrom(IVariables)
   */
  @Override
  public void copyVariablesFrom( IVariables variables ) {
    this.variables.copyVariablesFrom( variables );
  }

  /**
   * Substitutes any variable values into the given string, and returns the resolved string.
   *
   * @param aString the string to resolve against environment variables
   * @return the string after variables have been resolved/susbstituted
   * @see IVariables#environmentSubstitute(java.lang.String)
   */
  @Override
  public String environmentSubstitute( String aString ) {
    return variables.environmentSubstitute( aString );
  }

  /**
   * Substitutes any variable values into each of the given strings, and returns an array containing the resolved
   * string(s).
   *
   * @param aString an array of strings to resolve against environment variables
   * @return the array of strings after variables have been resolved/susbstituted
   * @see IVariables#environmentSubstitute(java.lang.String[])
   */
  @Override
  public String[] environmentSubstitute( String[] aString ) {
    return variables.environmentSubstitute( aString );
  }

  @Override
  public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData )
    throws HopValueException {
    return variables.fieldSubstitute( aString, rowMeta, rowData );
  }

  /**
   * Gets the parent variable space.
   *
   * @return the parent variable space
   * @see IVariables#getParentVariableSpace()
   */
  @Override
  public IVariables getParentVariableSpace() {
    return variables.getParentVariableSpace();
  }

  /**
   * Sets the parent variable space.
   *
   * @param parent the new parent variable space
   * @see IVariables#setParentVariableSpace(
   *IVariables)
   */
  @Override
  public void setParentVariableSpace( IVariables parent ) {
    variables.setParentVariableSpace( parent );
  }

  /**
   * Gets the value of the specified variable, or returns a default value if no such variable exists.
   *
   * @param variableName the variable name
   * @param defaultValue the default value
   * @return the value of the specified variable, or returns a default value if no such variable exists
   * @see IVariables#getVariable(java.lang.String, java.lang.String)
   */
  @Override
  public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  /**
   * Gets the value of the specified variable, or returns a default value if no such variable exists.
   *
   * @param variableName the variable name
   * @return the value of the specified variable, or returns a default value if no such variable exists
   * @see IVariables#getVariable(java.lang.String)
   */
  @Override
  public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  /**
   * Returns a boolean representation of the specified variable after performing any necessary substitution. Truth
   * values include case-insensitive versions of "Y", "YES", "TRUE" or "1".
   *
   * @param variableName the variable name
   * @param defaultValue the default value
   * @return a boolean representation of the specified variable after performing any necessary substitution
   * @see IVariables#getBooleanValueOfVariable(java.lang.String, boolean)
   */
  @Override
  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaString.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  /**
   * Sets the values of the pipeline's variables to the values from the parent variables.
   *
   * @param parent the parent
   * @see IVariables#initializeVariablesFrom(
   *IVariables)
   */
  @Override
  public void initializeVariablesFrom( IVariables parent ) {
    variables.initializeVariablesFrom( parent );
  }

  /**
   * Gets a list of variable names for the pipeline.
   *
   * @return a list of variable names
   * @see IVariables#listVariables()
   */
  @Override
  public String[] listVariables() {
    return variables.listVariables();
  }

  /**
   * Sets the value of the specified variable to the specified value.
   *
   * @param variableName  the variable name
   * @param variableValue the variable value
   * @see IVariables#setVariable(java.lang.String, java.lang.String)
   */
  @Override
  public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  /**
   * Shares a variable space from another variable space. This means that the object should take over the space used as
   * argument.
   *
   * @param variables the variable space
   * @see IVariables#shareVariablesWith(IVariables)
   */
  @Override
  public void shareVariablesWith( IVariables variables ) {
    this.variables = variables;
  }

  /**
   * Injects variables using the given Map. The behavior should be that the properties object will be stored and at the
   * time the IVariables is initialized (or upon calling this method if the space is already initialized). After
   * injecting the link of the properties object should be removed.
   *
   * @param prop the property map
   * @see IVariables#injectVariables(java.util.Map)
   */
  @Override
  public void injectVariables( Map<String, String> prop ) {
    variables.injectVariables( prop );
  }

  /**
   * Pauses the pipeline (pause all transforms).
   */
  public void pauseRunning() {
    setPaused( true );
    for ( TransformMetaDataCombi combi : transforms ) {
      combi.transform.pauseRunning();
    }
  }

  /**
   * Resumes running the pipeline after a pause (resume all transforms).
   */
  public void resumeRunning() {
    for ( TransformMetaDataCombi combi : transforms ) {
      combi.transform.resumeRunning();
    }
    setPaused( false );
  }

  /**
   * Checks whether the pipeline is being previewed.
   *
   * @return true if the pipeline is being previewed, false otherwise
   */
  public boolean isPreview() {
    return preview;
  }

  /**
   * Sets whether the pipeline is being previewed.
   *
   * @param preview true if the pipeline is being previewed, false otherwise
   */
  public void setPreview( boolean preview ) {
    this.preview = preview;
  }

  /**
   * Gets a named list (map) of transform performance snapshots.
   *
   * @return a named list (map) of transform performance snapshots
   */
  public Map<String, List<PerformanceSnapShot>> getTransformPerformanceSnapShots() {
    return transformPerformanceSnapShots;
  }

  /**
   * Sets the named list (map) of transform performance snapshots.
   *
   * @param transformPerformanceSnapShots a named list (map) of transform performance snapshots to set
   */
  public void setTransformPerformanceSnapShots( Map<String, List<PerformanceSnapShot>> transformPerformanceSnapShots ) {
    this.transformPerformanceSnapShots = transformPerformanceSnapShots;
  }

  /**
   * Gets a list of the pipeline listeners. Please do not attempt to modify this list externally. Returned list is
   * mutable only for backward compatibility purposes.
   *
   * @return the executionListeners
   */
  public List<IExecutionListener<PipelineMeta>> getExecutionListeners() {
    return executionListeners;
  }

  /**
   * Sets the list of pipeline listeners.
   *
   * @param executionListeners the executionListeners to set
   */
  public void setExecutionListeners( List<IExecutionListener<PipelineMeta>> executionListeners ) {
    this.executionListeners = Collections.synchronizedList( executionListeners );
  }

  /**
   * Adds a pipeline listener.
   *
   * @param executionListener the pipeline listener
   */
  public void addExecutionListener( IExecutionListener executionListener ) {
    // PDI-5229 sync added
    synchronized ( executionListeners ) {
      executionListeners.add( executionListener );
    }
  }

  /**
   * Sets the list of stop-event listeners for the pipeline.
   *
   * @param pipelineStoppedListeners the list of stop-event listeners to set
   */
  public void setPipelineStoppedListeners( List<IPipelineStoppedListener> pipelineStoppedListeners ) {
    this.pipelineStoppedListeners = Collections.synchronizedList( pipelineStoppedListeners );
  }

  /**
   * Gets the list of stop-event listeners for the pipeline. This is not concurrent safe. Please note this is
   * mutable implementation only for backward compatibility reasons.
   *
   * @return the list of stop-event listeners
   */
  public List<IPipelineStoppedListener> getPipelineStoppedListeners() {
    return pipelineStoppedListeners;
  }

  /**
   * Adds a stop-event listener to the pipeline.
   *
   * @param pipelineStoppedListener the stop-event listener to add
   */
  public void addPipelineStoppedListener( IPipelineStoppedListener pipelineStoppedListener ) {
    pipelineStoppedListeners.add( pipelineStoppedListener );
  }

  /**
   * Checks if the pipeline is paused.
   *
   * @return true if the pipeline is paused, false otherwise
   */
  public boolean isPaused() {
    int exist = status.get() & PAUSED.mask;
    return exist != 0;
  }

  public void setPaused( boolean paused ) {
    status.updateAndGet( v -> paused ? v | PAUSED.mask : ( BIT_STATUS_SUM ^ PAUSED.mask ) & v );
  }

  /**
   * Checks if the pipeline is stopped.
   *
   * @return true if the pipeline is stopped, false otherwise
   */
  public boolean isStopped() {
    int exist = status.get() & STOPPED.mask;
    return exist != 0;
  }

  public void setStopped( boolean stopped ) {
    status.updateAndGet( v -> stopped ? v | STOPPED.mask : ( BIT_STATUS_SUM ^ STOPPED.mask ) & v );
  }

  /**
   * Adds a parameter definition to this pipeline.
   *
   * @param key         the name of the parameter
   * @param defValue    the default value for the parameter
   * @param description the description of the parameter
   * @throws DuplicateParamException the duplicate param exception
   * @see INamedParams#addParameterDefinition(java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override
  public void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException {
    namedParams.addParameterDefinition( key, defValue, description );
  }

  /**
   * Gets the default value of the specified parameter.
   *
   * @param key the name of the parameter
   * @return the default value of the parameter
   * @throws UnknownParamException if the parameter does not exist
   * @see INamedParams#getParameterDefault(java.lang.String)
   */
  @Override
  public String getParameterDefault( String key ) throws UnknownParamException {
    return namedParams.getParameterDefault( key );
  }

  /**
   * Gets the description of the specified parameter.
   *
   * @param key the name of the parameter
   * @return the parameter description
   * @throws UnknownParamException if the parameter does not exist
   * @see INamedParams#getParameterDescription(java.lang.String)
   */
  @Override
  public String getParameterDescription( String key ) throws UnknownParamException {
    return namedParams.getParameterDescription( key );
  }

  /**
   * Gets the value of the specified parameter.
   *
   * @param key the name of the parameter
   * @return the parameter value
   * @throws UnknownParamException if the parameter does not exist
   * @see INamedParams#getParameterValue(java.lang.String)
   */
  @Override
  public String getParameterValue( String key ) throws UnknownParamException {
    return namedParams.getParameterValue( key );
  }

  /**
   * Gets a list of the parameters for the pipeline.
   *
   * @return an array of strings containing the names of all parameters for the pipeline
   * @see INamedParams#listParameters()
   */
  @Override
  public String[] listParameters() {
    return namedParams.listParameters();
  }

  /**
   * Sets the value for the specified parameter.
   *
   * @param key   the name of the parameter
   * @param value the name of the value
   * @throws UnknownParamException if the parameter does not exist
   * @see INamedParams#setParameterValue(java.lang.String, java.lang.String)
   */
  @Override
  public void setParameterValue( String key, String value ) throws UnknownParamException {
    namedParams.setParameterValue( key, value );
  }

  /**
   * Remove all parameters.
   *
   * @see INamedParams#eraseParameters()
   */
  @Override
  public void eraseParameters() {
    namedParams.eraseParameters();
  }

  /**
   * Clear the values of all parameters.
   *
   * @see INamedParams#clearParameters()
   */
  @Override
  public void clearParameters() {
    namedParams.clearParameters();
  }

  /**
   * Activates all parameters by setting their values. If no values already exist, the method will attempt to set the
   * parameter to the default value. If no default value exists, the method will set the value of the parameter to the
   * empty string ("").
   *
   * @see INamedParams#activateParameters()
   */
  @Override
  public void activateParameters() {
    String[] keys = listParameters();

    for ( String key : keys ) {
      String value;
      try {
        value = getParameterValue( key );
      } catch ( UnknownParamException e ) {
        value = "";
      }

      String defValue;
      try {
        defValue = getParameterDefault( key );
      } catch ( UnknownParamException e ) {
        defValue = "";
      }

      if ( Utils.isEmpty( value ) ) {
        setVariable( key, Const.NVL( defValue, "" ) );
      } else {
        setVariable( key, Const.NVL( value, "" ) );
      }
    }
  }

  /**
   * Copy parameters from a INamedParams object.
   *
   * @param params the INamedParams object from which to copy the parameters
   * @see INamedParams#copyParametersFrom(INamedParams)
   */
  @Override
  public void copyParametersFrom( INamedParams params ) {
    namedParams.copyParametersFrom( params );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#mergeParametersWith(org.apache.hop.core.parameters.INamedParams,
   * boolean replace)
   */
  @Override
  public void mergeParametersWith( INamedParams params, boolean replace ) {
    namedParams.mergeParametersWith( params, replace );
  }

  /**
   * Gets the parent pipeline, which is null if no parent pipeline exists.
   *
   * @return a reference to the parent pipeline's Pipeline object, or null if no parent pipeline exists
   */
  public Pipeline getParentPipeline() {
    return parentPipeline;
  }

  /**
   * Sets the parent pipeline.
   *
   * @param parentPipeline the parentPipeline to set
   */
  public void setParentPipeline( Pipeline parentPipeline ) {
    this.logLevel = parentPipeline.getLogLevel();
    this.log.setLogLevel( logLevel );
    this.parentPipeline = parentPipeline;

    transactionId = calculateTransactionId();
  }

  /**
   * Gets the mapping transform name.
   *
   * @return the name of the mapping transform that created this pipeline
   */
  public String getMappingTransformName() {
    return mappingTransformName;
  }

  /**
   * Sets the mapping transform name.
   *
   * @param mappingTransformName the name of the mapping transform that created this pipeline
   */
  public void setMappingTransformName( String mappingTransformName ) {
    this.mappingTransformName = mappingTransformName;
  }

  /**
   * Sets the socket repository.
   *
   * @param socketRepository the new socket repository
   */
  public void setSocketRepository( SocketRepository socketRepository ) {
    this.socketRepository = socketRepository;
  }

  /**
   * Gets the socket repository.
   *
   * @return the socket repository
   */
  public SocketRepository getSocketRepository() {
    return socketRepository;
  }

  /**
   * Gets the object name.
   *
   * @return the object name
   * @see ILoggingObject#getObjectName()
   */
  @Override
  public String getObjectName() {
    return getName();
  }

  /**
   * Gets the object copy. For Pipeline, this always returns null
   *
   * @return null
   * @see ILoggingObject#getObjectCopy()
   */
  @Override
  public String getObjectCopy() {
    return null;
  }

  /**
   * Gets the filename of the pipeline, or null if no filename exists
   *
   * @return the filename
   * @see ILoggingObject#getFilename()
   */
  @Override
  public String getFilename() {
    if ( pipelineMeta == null ) {
      return null;
    }
    return pipelineMeta.getFilename();
  }

  /**
   * Gets the log channel ID.
   *
   * @return the log channel ID
   * @see ILoggingObject#getLogChannelId()
   */
  @Override
  public String getLogChannelId() {
    return log.getLogChannelId();
  }


  /**
   * Gets the object type. For Pipeline, this always returns LoggingObjectType.PIPELINE
   *
   * @return the object type
   * @see ILoggingObject#getObjectType()
   */
  @Override
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.PIPELINE;
  }

  /**
   * Gets the parent logging object interface.
   *
   * @return the parent
   * @see ILoggingObject#getParent()
   */
  @Override
  public ILoggingObject getParent() {
    return parent;
  }

  /**
   * Gets the log level.
   *
   * @return the log level
   * @see ILoggingObject#getLogLevel()
   */
  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * Sets the log level.
   *
   * @param logLevel the new log level
   */
  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
    log.setLogLevel( logLevel );
  }

  /**
   * Gets the logging hierarchy.
   *
   * @return the logging hierarchy
   */
  public List<LoggingHierarchy> getLoggingHierarchy() {
    List<LoggingHierarchy> hierarchy = new ArrayList<>();
    List<String> childIds = LoggingRegistry.getInstance().getLogChannelChildren( getLogChannelId() );
    for ( String childId : childIds ) {
      ILoggingObject loggingObject = LoggingRegistry.getInstance().getLoggingObject( childId );
      if ( loggingObject != null ) {
        hierarchy.add( new LoggingHierarchy( getLogChannelId(), batchId, loggingObject ) );
      }
    }

    return hierarchy;
  }

  /**
   * Use:
   * {@link #addActiveSubPipeline(String, Pipeline),
   * {@link #getActiveSubPipeline(String)},
   * {@link #removeActiveSubPipeline(String)}
   * <p>
   * instead
   */
  @Deprecated
  public Map<String, Pipeline> getActiveSubPipelines() {
    return activeSubPipelines;
  }

  public void addActiveSubPipeline( final String subPipelineName, Pipeline subPipeline ) {
    activeSubPipelines.put( subPipelineName, subPipeline );
  }

  public Pipeline removeActiveSubPipeline( final String subPipelineName ) {
    return activeSubPipelines.remove( subPipelineName );
  }

  public Pipeline getActiveSubPipeline( final String subPipelineName ) {
    return activeSubPipelines.get( subPipelineName );
  }

  /**
   * Gets the active sub-workflows.
   *
   * @return a map (by name) of the active sub-workflows
   */
  public Map<String, Workflow> getActiveSubjobs() {
    return activeSubjobs;
  }

  /**
   * Gets the container object ID.
   *
   * @return the HopServer object ID
   */
  @Override
  public String getContainerObjectId() {
    return containerObjectId;
  }

  /**
   * Sets the container object ID.
   *
   * @param containerObjectId the HopServer object ID to set
   */
  public void setContainerObjectId( String containerObjectId ) {
    this.containerObjectId = containerObjectId;
  }

  /**
   * Gets the registration date. For Pipeline, this always returns null
   *
   * @return null
   */
  @Override
  public Date getRegistrationDate() {
    return null;
  }

  /**
   * Sets the servlet print writer.
   *
   * @param servletPrintWriter the new servlet print writer
   */
  public void setServletPrintWriter( PrintWriter servletPrintWriter ) {
    this.servletPrintWriter = servletPrintWriter;
  }

  /**
   * Gets the servlet print writer.
   *
   * @return the servlet print writer
   */
  public PrintWriter getServletPrintWriter() {
    return servletPrintWriter;
  }

  /**
   * Gets the name of the executing server.
   *
   * @return the executingServer
   */
  @Override
  public String getExecutingServer() {
    if ( executingServer == null ) {
      setExecutingServer( Const.getHostname() );
    }
    return executingServer;
  }

  /**
   * Sets the name of the executing server.
   *
   * @param executingServer the executingServer to set
   */
  @Override
  public void setExecutingServer( String executingServer ) {
    this.executingServer = executingServer;
  }

  /**
   * Gets the name of the executing user.
   *
   * @return the executingUser
   */
  @Override
  public String getExecutingUser() {
    return executingUser;
  }

  /**
   * Sets the name of the executing user.
   *
   * @param executingUser the executingUser to set
   */
  @Override
  public void setExecutingUser( String executingUser ) {
    this.executingUser = executingUser;
  }

  @Override
  public boolean isGatheringMetrics() {
    return log != null && log.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    if ( log != null ) {
      log.setGatheringMetrics( gatheringMetrics );
    }
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return log != null && log.isForcingSeparateLogging();
  }

  @Override
  public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
    if ( log != null ) {
      log.setForcingSeparateLogging( forcingSeparateLogging );
    }
  }

  public List<ResultFile> getResultFiles() {
    return resultFiles;
  }

  public void setResultFiles( List<ResultFile> resultFiles ) {
    this.resultFiles = resultFiles;
  }

  public List<RowMetaAndData> getResultRows() {
    return resultRows;
  }

  public void setResultRows( List<RowMetaAndData> resultRows ) {
    this.resultRows = resultRows;
  }

  public Result getPreviousResult() {
    return previousResult;
  }

  public void setPreviousResult( Result previousResult ) {
    this.previousResult = previousResult;
  }

  public Hashtable<String, Counter> getCounters() {
    return counters;
  }

  public void setCounters( Hashtable<String, Counter> counters ) {
    this.counters = counters;
  }

  /**
   * Clear the error in the pipeline, clear all the rows from all the row sets, to make sure the pipeline
   * can continue with other data. This is intended for use when running single threaded.
   */
  public void clearError() {
    setStopped( false );
    errors.set( 0 );
    setFinished( false );
    for ( TransformMetaDataCombi combi : transforms ) {
      ITransform<ITransformMeta, ITransformData> transform = combi.transform;
      for ( IRowSet rowSet : transform.getInputRowSets() ) {
        rowSet.clear();
      }
      transform.setStopped( false );
    }
  }

  /**
   * Gets the transaction ID for the pipeline.
   *
   * @return the transactionId
   */
  public String getTransactionId() {
    return transactionId;
  }

  /**
   * Sets the transaction ID for the pipeline.
   *
   * @param transactionId the transactionId to set
   */
  public void setTransactionId( String transactionId ) {
    this.transactionId = transactionId;
  }

  /**
   * Calculates the transaction ID for the pipeline.
   *
   * @return the calculated transaction ID for the pipeline.
   */
  public String calculateTransactionId() {
    if ( getPipelineMeta() != null && getPipelineMeta().isUsingUniqueConnections() ) {
      if ( parentWorkflow != null && parentWorkflow.getTransactionId() != null ) {
        return parentWorkflow.getTransactionId();
      } else if ( parentPipeline != null && parentPipeline.getPipelineMeta().isUsingUniqueConnections() ) {
        return parentPipeline.getTransactionId();
      } else {
        return DatabaseConnectionMap.getInstance().getNextTransactionId();
      }
    } else {
      return Thread.currentThread().getName();
    }
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
    if ( pipelineMeta != null ) {
      pipelineMeta.setMetaStore( metaStore );
    }
  }

  /**
   * Sets encoding of HttpServletResponse according to System encoding.Check if system encoding is null or an empty and
   * set it to HttpServletResponse when not and writes error to log if null. Throw IllegalArgumentException if input
   * parameter is null.
   *
   * @param response the HttpServletResponse to set encoding, mayn't be null
   */
  public void setServletReponse( HttpServletResponse response ) {
    if ( response == null ) {
      throw new IllegalArgumentException( "HttpServletResponse cannot be null " );
    }
    String encoding = System.getProperty( "HOP_DEFAULT_SERVLET_ENCODING", null );
    // true if encoding is null or an empty (also for the next kin of strings: " ")
    if ( !StringUtils.isBlank( encoding ) ) {
      try {
        response.setCharacterEncoding( encoding.trim() );
        response.setContentType( "text/html; charset=" + encoding );
      } catch ( Exception ex ) {
        LogChannel.GENERAL.logError( "Unable to encode data with encoding : '" + encoding + "'", ex );
      }
    }
    this.servletResponse = response;
  }

  public HttpServletResponse getServletResponse() {
    return servletResponse;
  }

  public void setServletRequest( HttpServletRequest request ) {
    this.servletRequest = request;
  }

  public HttpServletRequest getServletRequest() {
    return servletRequest;
  }

  public List<IDelegationListener> getDelegationListeners() {
    return delegationListeners;
  }

  public void setDelegationListeners( List<IDelegationListener> delegationListeners ) {
    this.delegationListeners = delegationListeners;
  }

  public void addDelegationListener( IDelegationListener delegationListener ) {
    delegationListeners.add( delegationListener );
  }

  public synchronized void doTopologySortOfTransforms() {
    // The bubble sort algorithm in contrast to the QuickSort or MergeSort
    // algorithms
    // does indeed cover all possibilities.
    // Sorting larger pipelines with hundreds of transforms might be too slow
    // though.
    // We should consider caching PipelineMeta.findPrevious() results in that case.
    //
    pipelineMeta.clearCaches();

    //
    // Cocktail sort (bi-directional bubble sort)
    //
    // Original sort was taking 3ms for 30 transforms
    // cocktail sort takes about 8ms for the same 30, but it works :)
    //
    int transformsMinSize = 0;
    int transformsSize = transforms.size();

    // Noticed a problem with an immediate shrinking iteration window
    // trapping rows that need to be sorted.
    // This threshold buys us some time to get the sorting close before
    // starting to decrease the window size.
    //
    // TODO: this could become much smarter by tracking row movement
    // and reacting to that each outer iteration verses
    // using a threshold.
    //
    // After this many iterations enable trimming inner iteration
    // window on no change being detected.
    //
    int windowShrinkThreshold = (int) Math.round( transformsSize * 0.75 );

    // give ourselves some room to sort big lists. the window threshold should
    // stop us before reaching this anyway.
    //
    int totalIterations = transformsSize * 2;

    boolean isBefore = false;
    boolean forwardChange = false;
    boolean backwardChange = false;

    boolean lastForwardChange = true;
    boolean keepSortingForward = true;

    TransformMetaDataCombi one = null;
    TransformMetaDataCombi two = null;

    for ( int x = 0; x < totalIterations; x++ ) {

      // Go forward through the list
      //
      if ( keepSortingForward ) {
        for ( int y = transformsMinSize; y < transformsSize - 1; y++ ) {
          one = transforms.get( y );
          two = transforms.get( y + 1 );

          if ( one.transformMeta.equals( two.transformMeta ) ) {
            isBefore = one.copy > two.copy;
          } else {
            isBefore = pipelineMeta.findPrevious( one.transformMeta, two.transformMeta );
          }
          if ( isBefore ) {
            // two was found to be positioned BEFORE one so we need to
            // switch them...
            //
            transforms.set( y, two );
            transforms.set( y + 1, one );
            forwardChange = true;

          }
        }
      }

      // Go backward through the list
      //
      for ( int z = transformsSize - 1; z > transformsMinSize; z-- ) {
        one = transforms.get( z );
        two = transforms.get( z - 1 );

        if ( one.transformMeta.equals( two.transformMeta ) ) {
          isBefore = one.copy > two.copy;
        } else {
          isBefore = pipelineMeta.findPrevious( one.transformMeta, two.transformMeta );
        }
        if ( !isBefore ) {
          // two was found NOT to be positioned BEFORE one so we need to
          // switch them...
          //
          transforms.set( z, two );
          transforms.set( z - 1, one );
          backwardChange = true;
        }
      }

      // Shrink transformsSize(max) if there was no forward change
      //
      if ( x > windowShrinkThreshold && !forwardChange ) {

        // should we keep going? check the window size
        //
        transformsSize--;
        if ( transformsSize <= transformsMinSize ) {
          break;
        }
      }

      // shrink transformsMinSize(min) if there was no backward change
      //
      if ( x > windowShrinkThreshold && !backwardChange ) {

        // should we keep going? check the window size
        //
        transformsMinSize++;
        if ( transformsMinSize >= transformsSize ) {
          break;
        }
      }

      // End of both forward and backward traversal.
      // Time to see if we should keep going.
      //
      if ( !forwardChange && !backwardChange ) {
        break;
      }

      //
      // if we are past the first iteration and there has been no change twice,
      // quit doing it!
      //
      if ( keepSortingForward && x > 0 && !lastForwardChange && !forwardChange ) {
        keepSortingForward = false;
      }
      lastForwardChange = forwardChange;
      forwardChange = false;
      backwardChange = false;

    } // finished sorting
  }

  @Override
  public Map<String, Object> getExtensionDataMap() {
    return extensionDataMap;
  }

  // TODO: i18n
  public static final IEngineMetric METRIC_INPUT = new EngineMetric( METRIC_NAME_INPUT, "Input", "The number of rows read from physical I/O", "010", true );
  public static final IEngineMetric METRIC_OUTPUT = new EngineMetric( METRIC_NAME_OUTPUT, "Output", "The number of rows written to physical I/O", "020", true );
  public static final IEngineMetric METRIC_READ = new EngineMetric( METRIC_NAME_READ, "Read", "The number of rows read from other transforms", "030", true );
  public static final IEngineMetric METRIC_WRITTEN = new EngineMetric( METRIC_NAME_WRITTEN, "Written", "The number of rows written to other transforms", "040", true );
  public static final IEngineMetric METRIC_UPDATED = new EngineMetric( METRIC_NAME_UPDATED, "Updated", "The number of rows updated", "050", true );
  public static final IEngineMetric METRIC_REJECTED = new EngineMetric( METRIC_NAME_REJECTED, "Rejected", "The number of rows rejected by a transform", "060", true );
  public static final IEngineMetric METRIC_ERROR = new EngineMetric( METRIC_NAME_ERROR, "Errors", "The number of errors", "070", true );
  public static final IEngineMetric METRIC_BUFFER_IN = new EngineMetric( METRIC_NAME_BUFFER_IN, "Buffers Input", "The number of rows in the transforms input buffers", "080", true );
  public static final IEngineMetric METRIC_BUFFER_OUT = new EngineMetric( METRIC_NAME_BUFFER_OUT, "Buffers Output", "The number of rows in the transforms output buffers", "090", true );

  public EngineMetrics getEngineMetrics() {
    return getEngineMetrics( null, -1 );
  }

  public synchronized EngineMetrics getEngineMetrics(String componentName, int copyNr) {
    EngineMetrics metrics = new EngineMetrics();
    metrics.setStartDate( getStartDate() );
    metrics.setEndDate( getEndDate() );
    if ( transforms != null ) {
      synchronized ( transforms ) {
        for ( TransformMetaDataCombi<ITransform, ITransformMeta, ITransformData> combi : transforms ) {
          ITransform<ITransformMeta, ITransformData> transform = combi.transform;

          boolean collect = true;
          if ( copyNr >= 0 ) {
            collect = collect && copyNr == combi.copy;
          }
          if ( componentName != null ) {
            collect = collect && componentName.equalsIgnoreCase( combi.transformName );
          }

          if ( collect ) {

            metrics.addComponent( combi.transform );

            metrics.setComponentMetric( combi.transform, METRIC_INPUT, combi.transform.getLinesInput() );
            metrics.setComponentMetric( combi.transform, METRIC_OUTPUT, combi.transform.getLinesOutput() );
            metrics.setComponentMetric( combi.transform, METRIC_READ, combi.transform.getLinesRead() );
            metrics.setComponentMetric( combi.transform, METRIC_WRITTEN, combi.transform.getLinesWritten() );
            metrics.setComponentMetric( combi.transform, METRIC_UPDATED, combi.transform.getLinesUpdated() );
            metrics.setComponentMetric( combi.transform, METRIC_REJECTED, combi.transform.getLinesRejected() );
            metrics.setComponentMetric( combi.transform, METRIC_ERROR, combi.transform.getErrors() );

            long inputBufferSize = 0;
            for ( IRowSet rowSet : transform.getInputRowSets() ) {
              inputBufferSize += rowSet.size();
            }
            metrics.setComponentMetric( combi.transform, METRIC_BUFFER_IN, inputBufferSize );
            long outputBufferSize = 0;
            for ( IRowSet rowSet : transform.getOutputRowSets() ) {
              outputBufferSize += rowSet.size();
            }
            metrics.setComponentMetric( combi.transform, METRIC_BUFFER_OUT, outputBufferSize );

            TransformStatus transformStatus = new TransformStatus( combi.transform );
            metrics.setComponentSpeed( combi.transform, transformStatus.getSpeed() );
            metrics.setComponentStatus( combi.transform, combi.transform.getStatus().getDescription() );
            metrics.setComponentRunning( combi.transform, combi.transform.isRunning() );
          }
        }
      }
    }

    // Also pass on the performance snapshot data...
    //
    if (transformPerformanceSnapShots!=null) {
      for (String componentString : transformPerformanceSnapShots.keySet()) {
        String snapshotName = componentString;
        int snapshotCopyNr = 0;
        int lastDot = componentString.lastIndexOf( '.' );
        if (lastDot>0) {
          componentString.substring( 0, lastDot );
          snapshotCopyNr = Const.toInt(componentString.substring( lastDot+1 ), 0);
        }
        boolean collect = true;
        if (componentName!=null) {
          collect=collect && componentName.equalsIgnoreCase( componentString );
        }
        if (copyNr>=0) {
          collect = collect && snapshotCopyNr==copyNr;
        }

        if (collect) {
          IEngineComponent component = findComponent( snapshotName, snapshotCopyNr );
          if (component!=null) {
            List<PerformanceSnapShot> snapShots = transformPerformanceSnapShots.get( componentString );
            metrics.getComponentPerformanceSnapshots().put( component, snapShots );
          }
        }
      }
    }

    return metrics;
  }

  @Override
  public String getComponentLogText( String componentName, int copyNr ) {
    ITransform transform = findTransformInterface( componentName, copyNr );
    if ( transform == null ) {
      return null;
    }
    StringBuffer logBuffer = HopLogStore.getAppender().getBuffer( transform.getLogChannel().getLogChannelId(), false );
    if ( logBuffer == null ) {
      return null;
    }
    return logBuffer.toString();
  }

  @Override public List<IEngineComponent> getComponents() {
    List<IEngineComponent> list = new ArrayList<>();
    for ( TransformMetaDataCombi transform : transforms ) {
      list.add( transform.transform );
    }
    return list;
  }

  @Override public IEngineComponent findComponent( String name, int copyNr ) {
    return findTransformInterface( name, copyNr );
  }

  @Override public List<IEngineComponent> getComponentCopies( String name ) {
    List<IEngineComponent> list = new ArrayList<>();
    if (transforms!=null) {
      for ( TransformMetaDataCombi transform : transforms ) {
        if ( transform.transformName.equalsIgnoreCase( name ) ) {
          list.add( transform.transform );
        }
      }
    }
    return list;
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

  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new EmptyPipelineRunConfiguration();
  }

  /**
   * Gets pipelineEngineRunConfiguration
   *
   * @return value of pipelineEngineRunConfiguration
   */
  public IPipelineEngineRunConfiguration getPipelineEngineRunConfiguration() {
    return pipelineEngineRunConfiguration;
  }

  /**
   * @param pipelineEngineRunConfiguration The pipelineEngineRunConfiguration to set
   */
  public void setPipelineEngineRunConfiguration( IPipelineEngineRunConfiguration pipelineEngineRunConfiguration ) {
    this.pipelineEngineRunConfiguration = pipelineEngineRunConfiguration;
  }

  public void retrieveComponentOutput( String componentName, int copyNr, int nrRows, IPipelineComponentRowsReceived rowsReceived ) throws HopException {
    ITransform iTransform = findTransformInterface( componentName, copyNr );
    if ( iTransform ==null) {
      throw new HopException( "Unable to find transform '"+componentName+"', copy "+copyNr+" to retrieve output rows from" );
    }
    RowBuffer rowBuffer = new RowBuffer( pipelineMeta.getTransformFields( componentName ) );
    iTransform.addRowListener( new RowAdapter() {
      @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        if (rowBuffer.getBuffer().size()<nrRows) {
          rowBuffer.getBuffer().add( row );
          if ( rowBuffer.getBuffer().size() >= nrRows ) {
            try {
              rowsReceived.rowsReceived( Pipeline.this, rowBuffer );
            } catch ( HopException e ) {
              throw new HopTransformException( "Error recieving rows from '" + componentName + " copy " + copyNr, e );
            }
          }
        }
      }
    } );
  }

  @Override public void addFinishedListener( IFinishedListener listener ) throws HopException {
    addExecutionListener( new ExecutionAdapter() {
      @Override public void finished( IPipelineEngine engine ) throws HopException {
        listener.finished( engine );
      }
    } );
  }

  /**
   * Gets rowSetSize
   *
   * @return value of rowSetSize
   */
  public int getRowSetSize() {
    return rowSetSize;
  }

  /**
   * @param rowSetSize The rowSetSize to set
   */
  public void setRowSetSize( int rowSetSize ) {
    this.rowSetSize = rowSetSize;
  }

  /**
   * Gets feedbackShown
   *
   * @return value of feedbackShown
   */
  public boolean isFeedbackShown() {
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
  public int getFeedbackSize() {
    return feedbackSize;
  }

  /**
   * @param feedbackSize The feedbackSize to set
   */
  public void setFeedbackSize( int feedbackSize ) {
    this.feedbackSize = feedbackSize;
  }
}
