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

package org.apache.hop.workflow;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IExecutor;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingBuffer;
import org.apache.hop.core.logging.LoggingHierarchy;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.Metrics;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.resource.ResourceUtil;
import org.apache.hop.resource.TopLevelResource;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.actions.special.ActionSpecial;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.www.RegisterPackageServlet;
import org.apache.hop.www.RegisterWorkflowServlet;
import org.apache.hop.www.SocketRepository;
import org.apache.hop.www.StartWorkflowServlet;
import org.apache.hop.www.WebResult;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class executes a workflow as defined by a WorkflowMeta object.
 * <p>
 * The definition of a PDI workflow is represented by a WorkflowMeta object. It is typically loaded from a .hwf file,
 * or it is generated dynamically. The declared parameters of the workflow definition are then queried using
 * listParameters() and assigned values using calls to setParameterValue(..).
 *
 * @author Matt Casters
 * @since 07-apr-2003
 */
public class Workflow extends Thread implements IVariables, INamedParams, IHasLogChannel, ILoggingObject,
  IExecutor, IExtensionData {
  private static Class<?> PKG = Workflow.class; // for i18n purposes, needed by Translator!!

  public static final String CONFIGURATION_IN_EXPORT_FILENAME = "__job_execution_configuration__.xml";

  private ILogChannel log;

  private LogLevel logLevel = DefaultLogLevel.getLogLevel();

  private String containerObjectId;

  private WorkflowMeta workflowMeta;

  private int logCommitSize = 10;

  private AtomicInteger errors;

  private IVariables variables = new Variables();

  /**
   * The workflow that's launching this (sub-) workflow. This gives us access to the whole chain, including the parent variables,
   * etc.
   */
  protected Workflow parentWorkflow;

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
  private final LinkedList<ActionResult> actionResults = new LinkedList<ActionResult>();

  private Date executionStartDate;

  private Date executionEndDate;

  private long batchId;

  /**
   * This is the batch ID that is passed from workflow to workflow to pipeline, if nothing is passed, it's the workflow's batch
   * id
   */
  private long passedBatchId;

  /**
   * The rows that were passed onto this workflow by a previous pipeline. These rows are passed onto the first workflow
   * entry in this workflow (on the result object)
   */
  private List<RowMetaAndData> sourceRows;

  /**
   * The result of the workflow, after execution.
   */
  private Result result;

  private boolean interactive;

  private List<IWorkflowListener> jobListeners;

  private List<IActionListener> jobEntryListeners;

  private List<IDelegationListener> delegationListeners;

  private Map<ActionCopy, ActionPipeline> activeJobEntryPipeline;

  private Map<ActionCopy, ActionWorkflow> activeJobEntryWorkflows;

  /**
   * Parameters of the workflow.
   */
  private INamedParams namedParams = new NamedParamsDefault();

  private SocketRepository socketRepository;

  private int maxJobEntriesLogged;

  private ActionCopy startActionCopy;
  private Result startJobEntryResult;

  private String executingServer;

  private String executingUser;

  private Map<String, Object> extensionDataMap;

  /**
   * Int value for storage workflow statuses
   */
  private AtomicInteger status;

  /**
   * <p>
   * This enum stores bit masks which are used to manipulate with statuses over field {@link Workflow#status}
   */
  enum BitMaskStatus {
    ACTIVE( 1 ), INITIALIZED( 2 ), STOPPED( 4 ), FINISHED( 8 );

    private final int mask;
    // the sum of status masks
    public static final int BIT_STATUS_SUM = 63;

    BitMaskStatus( int mask ) {
      this.mask = mask;
    }
  }

  public Workflow( String name, String file, String[] args ) {
    this();
    workflowMeta = new WorkflowMeta();

    if ( name != null ) {
      setName( name + " (" + super.getName() + ")" );
    }
    workflowMeta.setName( name );
    workflowMeta.setFilename( file );

    init();
    this.log = new LogChannel( this );
  }

  public void init() {
    status = new AtomicInteger();

    jobListeners = new ArrayList<IWorkflowListener>();
    jobEntryListeners = new ArrayList<IActionListener>();
    delegationListeners = new ArrayList<IDelegationListener>();

    // these 2 maps are being modified concurrently and must be thread-safe
    activeJobEntryPipeline = new ConcurrentHashMap<ActionCopy, ActionPipeline>();
    activeJobEntryWorkflows = new ConcurrentHashMap<ActionCopy, ActionWorkflow>();

    extensionDataMap = new HashMap<String, Object>();

    workflowTracker = new WorkflowTracker( workflowMeta );
    synchronized ( actionResults ) {
      actionResults.clear();
    }
    errors = new AtomicInteger( 0 );
    batchId = -1;
    passedBatchId = -1;
    maxJobEntriesLogged = Const.toInt( EnvUtil.getSystemProperty( Const.HOP_MAX_ACTIONS_LOGGED ), 1000 );

    result = null;
    startActionCopy = null;
    startJobEntryResult = null;

    this.setDefaultLogCommitSize();
  }

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

  public Workflow( WorkflowMeta workflowMeta ) {
    this( workflowMeta, null );
  }

  public Workflow( WorkflowMeta workflowMeta, ILoggingObject parentLogging ) {
    this.workflowMeta = workflowMeta;
    this.containerObjectId = workflowMeta.getContainerObjectId();
    this.parentLoggingObject = parentLogging;

    init();

    workflowTracker = new WorkflowTracker( workflowMeta );

    this.log = new LogChannel( this, parentLogging );
    this.logLevel = log.getLogLevel();

    if ( this.containerObjectId == null ) {
      this.containerObjectId = log.getContainerObjectId();
    }
  }

  public Workflow() {
    init();
    this.log = new LogChannel( this );
    this.logLevel = log.getLogLevel();
  }

  /**
   * Gets the name property of the WorkflowMeta property.
   *
   * @return String name for the WorkflowMeta
   */
  @Override
  public String toString() {
    if ( workflowMeta == null || Utils.isEmpty( workflowMeta.getName() ) ) {
      return getName();
    } else {
      return workflowMeta.getName();
    }
  }

  public static final Workflow createJobWithNewClassLoader() throws HopException {
    try {
      // Load the class.
      Class<?> jobClass = Const.createNewClassLoader().loadClass( Workflow.class.getName() );

      // create the class
      // Try to instantiate this one...
      Workflow workflow = (Workflow) jobClass.getDeclaredConstructor().newInstance();

      // Done!
      return workflow;
    } catch ( Exception e ) {
      String message = BaseMessages.getString( PKG, "Job.Log.ErrorAllocatingNewWorkflow", e.toString() );
      throw new HopException( message, e );
    }
  }

  public String getJobname() {
    if ( workflowMeta == null ) {
      return null;
    }
    return workflowMeta.getName();
  }

  /**
   * Threads main loop: called by Thread.start();
   */
  @Override public void run() {

    ExecutorService heartbeat = null; // this workflow's heartbeat scheduled executor

    try {
      setStopped( false );
      setFinished( false );
      setInitialized( true );

      // Create a new variable name space as we want workflows to have their own set of variables.
      // initialize from parentWorkflow or null
      //
      variables.initializeVariablesFrom( parentWorkflow );
      setInternalHopVariables( variables );
      copyParametersFrom( workflowMeta );
      activateParameters();

      // Run the workflow
      //
      fireJobStartListeners();

      heartbeat = startHeartbeat( getHeartbeatIntervalInSeconds() );

      result = execute();
    } catch ( Throwable je ) {
      log.logError( BaseMessages.getString( PKG, "Job.Log.ErrorExecWorkflow", je.getMessage() ), je );
      // log.logError(Const.getStackTracker(je));
      //
      // we don't have result object because execute() threw a curve-ball.
      // So we create a new error object.
      //
      result = new Result();
      result.setNrErrors( 1L );
      result.setResult( false );
      addErrors( 1 ); // This can be before actual execution

      emergencyWriteJobTracker( result );

      setActive( false );
      setFinished( true );
      setStopped( false );
    } finally {
      try {
        shutdownHeartbeat( heartbeat );

        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobFinish.id, this );
        log.logDebug( BaseMessages.getString( PKG, "Job.Log.DisposeEmbeddedMetastore" ) );

        fireJobFinishListeners();

        // release unused vfs connections
        HopVfs.freeUnusedResources();

      } catch ( HopException e ) {
        result.setNrErrors( 1 );
        result.setResult( false );
        log.logError( BaseMessages.getString( PKG, "Job.Log.ErrorExecWorkflow", e.getMessage() ), e );

        emergencyWriteJobTracker( result );
      }
    }
  }

  private void emergencyWriteJobTracker( Result res ) {
    ActionResult jerFinalResult =
      new ActionResult( res, this.getLogChannelId(), BaseMessages.getString( PKG, "Job.Comment.JobFinished" ), null,
        null, 0, null );
    WorkflowTracker finalTrack = new WorkflowTracker( this.getWorkflowMeta(), jerFinalResult );
    // workflowTracker is up to date too.
    this.workflowTracker.addWorkflowTracker( finalTrack );
  }

  /**
   * Execute a workflow without previous results. This is a action point (not recursive)<br>
   * <br>
   *
   * @return the result of the execution
   * @throws HopException
   */
  private Result execute() throws HopException {
    try {
      log.snap( Metrics.METRIC_JOB_START );

      setFinished( false );
      setStopped( false );
      HopEnvironment.setExecutionInformation( this );

      log.logMinimal( BaseMessages.getString( PKG, "Job.Comment.JobStarted" ) );

      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobStart.id, this );

      // Start the tracking...
      ActionResult jerStart =
        new ActionResult( null, null, BaseMessages.getString( PKG, "Job.Comment.JobStarted" ), BaseMessages
          .getString( PKG, "Job.Reason.Started" ), null, 0, null );
      workflowTracker.addWorkflowTracker( new WorkflowTracker( workflowMeta, jerStart ) );

      setActive( true );
      // Where do we start?
      ActionCopy startpoint;

      // synchronize this to a parent workflow if needed.
      //
      Object syncObject = this;
      if ( parentWorkflow != null ) {
        syncObject = parentWorkflow; // parallel execution in a workflow
      }

      synchronized ( syncObject ) {
        beginProcessing();
      }

      Result res = null;

      if ( startActionCopy == null ) {
        startpoint = workflowMeta.findAction( WorkflowMeta.STRING_SPECIAL_START, 0 );
      } else {
        startpoint = startActionCopy;
        res = startJobEntryResult;
      }
      if ( startpoint == null ) {
        throw new HopWorkflowException( BaseMessages.getString( PKG, "Job.Log.CounldNotFindStartingPoint" ) );
      }

      ActionResult jerEnd = null;

      if ( startpoint.isStart() ) {
        // Perform optional looping in the special Start action...
        //
        // long iteration = 0;

        boolean isFirst = true;
        ActionSpecial jes = (ActionSpecial) startpoint.getEntry();
        while ( ( jes.isRepeat() || isFirst ) && !isStopped() ) {
          isFirst = false;
          res = execute( 0, null, startpoint, null, BaseMessages.getString( PKG, "Job.Reason.Started" ) );
        }
        jerEnd =
          new ActionResult( res, jes.getLogChannelId(), BaseMessages.getString( PKG, "Job.Comment.JobFinished" ),
            BaseMessages.getString( PKG, "Job.Reason.Finished" ), null, 0, null );
      } else {
        res = execute( 0, res, startpoint, null, BaseMessages.getString( PKG, "Job.Reason.Started" ) );
        jerEnd =
          new ActionResult( res, startpoint.getEntry().getLogChannel().getLogChannelId(), BaseMessages.getString(
            PKG, "Job.Comment.JobFinished" ), BaseMessages.getString( PKG, "Job.Reason.Finished" ), null, 0, null );
      }
      // Save this result...
      workflowTracker.addWorkflowTracker( new WorkflowTracker( workflowMeta, jerEnd ) );
      log.logMinimal( BaseMessages.getString( PKG, "Job.Comment.JobFinished" ) );

      setActive( false );
      if ( !isStopped() ) {
        setFinished( true );
      }
      return res;
    } finally {
      log.snap( Metrics.METRIC_JOB_STOP );
    }
  }

  /**
   * Execute a workflow with previous results passed in.<br>
   * <br>
   * Execute called by ActionWorkflow: don't clear the actionResults.
   *
   * @param nr     The action number
   * @param result the result of the previous execution
   * @return Result of the workflow execution
   * @throws HopWorkflowException
   */
  public Result execute( int nr, Result result ) throws HopException {
    setFinished( false );
    setActive( true );
    setInitialized( true );
    HopEnvironment.setExecutionInformation( this );

    // Where do we start?
    ActionCopy startpoint;

    // Perhaps there is already a list of input rows available?
    if ( getSourceRows() != null ) {
      result.setRows( getSourceRows() );
    }

    startpoint = workflowMeta.findAction( WorkflowMeta.STRING_SPECIAL_START, 0 );
    if ( startpoint == null ) {
      throw new HopWorkflowException( BaseMessages.getString( PKG, "Job.Log.CounldNotFindStartingPoint" ) );
    }

    ActionSpecial jes = (ActionSpecial) startpoint.getEntry();
    Result res;
    do {
      res = execute( nr, result, startpoint, null, BaseMessages.getString( PKG, "Job.Reason.StartOfJobentry" ) );
      setActive( false );
    } while ( jes.isRepeat() && !isStopped() );
    return res;
  }

  /**
   * Sets the finished flag.<b> Then launch all the workflow listeners and call the jobFinished method for each.<br>
   *
   * @see IWorkflowListener#jobFinished(Workflow)
   */
  public void fireJobFinishListeners() throws HopException {
    synchronized ( jobListeners ) {
      for ( IWorkflowListener jobListener : jobListeners ) {
        jobListener.jobFinished( this );
      }
    }
  }

  /**
   * Call all the jobStarted method for each listener.<br>
   *
   * @see IWorkflowListener#jobStarted(Workflow)
   */
  public void fireJobStartListeners() throws HopException {
    synchronized ( jobListeners ) {
      for ( IWorkflowListener jobListener : jobListeners ) {
        jobListener.jobStarted( this );
      }
    }
  }

  /**
   * Execute a action recursively and move to the next action automatically.<br>
   * Uses a back-tracking algorithm.<br>
   *
   * @param nr
   * @param prev_result
   * @param actionCopy
   * @param previous
   * @param reason
   * @return
   * @throws HopException
   */
  private Result execute( final int nr, Result prev_result, final ActionCopy actionCopy, ActionCopy previous,
                          String reason ) throws HopException {
    Result res = null;

    if ( isStopped() ) {
      res = new Result( nr );
      res.stopped = true;
      return res;
    }

    // if we didn't have a previous result, create one, otherwise, copy the content...
    //
    final Result newResult;
    Result prevResult = null;
    if ( prev_result != null ) {
      prevResult = prev_result.clone();
    } else {
      prevResult = new Result();
    }

    WorkflowExecutionExtension extension = new WorkflowExecutionExtension( this, prevResult, actionCopy, true );
    ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobBeforeJobEntryExecution.id, extension );

    if ( extension.result != null ) {
      prevResult = extension.result;
    }

    if ( !extension.executeEntry ) {
      newResult = prevResult;
    } else {
      if ( log.isDetailed() ) {
        log.logDetailed( "exec(" + nr + ", " + ( prev_result != null ? prev_result.getNrErrors() : 0 ) + ", "
          + ( actionCopy != null ? actionCopy.toString() : "null" ) + ")" );
      }

      // Which entry is next?
      IAction jobEntry = actionCopy.getEntry();
      jobEntry.getLogChannel().setLogLevel( logLevel );

      // Track the fact that we are going to launch the next action...
      ActionResult jerBefore =
        new ActionResult( null, null, BaseMessages.getString( PKG, "Job.Comment.JobStarted" ), reason, actionCopy
          .getName(), actionCopy.getNr(), environmentSubstitute( actionCopy.getEntry().getFilename() ) );
      workflowTracker.addWorkflowTracker( new WorkflowTracker( workflowMeta, jerBefore ) );

      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader( jobEntry.getClass().getClassLoader() );
      // Execute this entry...
      IAction cloneJei = (IAction) jobEntry.clone();
      ( (IVariables) cloneJei ).copyVariablesFrom( this );
      cloneJei.setMetaStore( getWorkflowMeta().getMetaStore() );
      cloneJei.setParentWorkflow( this );
      cloneJei.setParentWorkflowMeta( this.getWorkflowMeta() );
      final long start = System.currentTimeMillis();

      cloneJei.getLogChannel().logDetailed( "Starting action" );
      for ( IActionListener jobEntryListener : jobEntryListeners ) {
        jobEntryListener.beforeExecution( this, actionCopy, cloneJei );
      }
      if ( interactive ) {
        if ( actionCopy.isPipeline() ) {
          getActiveJobEntryPipeline().put( actionCopy, (ActionPipeline) cloneJei );
        }
        if ( actionCopy.isJob() ) {
          getActiveJobEntryWorkflows().put( actionCopy, (ActionWorkflow) cloneJei );
        }
      }
      log.snap( Metrics.METRIC_JOBENTRY_START, cloneJei.toString() );
      newResult = cloneJei.execute( prevResult, nr );
      log.snap( Metrics.METRIC_JOBENTRY_STOP, cloneJei.toString() );

      final long end = System.currentTimeMillis();
      if ( interactive ) {
        if ( actionCopy.isPipeline() ) {
          getActiveJobEntryPipeline().remove( actionCopy );
        }
        if ( actionCopy.isJob() ) {
          getActiveJobEntryWorkflows().remove( actionCopy );
        }
      }

      if ( cloneJei instanceof ActionPipeline ) {
        String throughput = newResult.getReadWriteThroughput( (int) ( ( end - start ) / 1000 ) );
        if ( throughput != null ) {
          log.logMinimal( throughput );
        }
      }
      for ( IActionListener jobEntryListener : jobEntryListeners ) {
        jobEntryListener.afterExecution( this, actionCopy, cloneJei, newResult );
      }

      Thread.currentThread().setContextClassLoader( cl );
      addErrors( (int) newResult.getNrErrors() );

      // Also capture the logging text after the execution...
      //
      LoggingBuffer loggingBuffer = HopLogStore.getAppender();
      StringBuffer logTextBuffer = loggingBuffer.getBuffer( cloneJei.getLogChannel().getLogChannelId(), false );
      newResult.setLogText( logTextBuffer.toString() + newResult.getLogText() );

      // Save this result as well...
      //
      ActionResult jerAfter =
        new ActionResult( newResult, cloneJei.getLogChannel().getLogChannelId(), BaseMessages.getString( PKG,
          "Job.Comment.JobFinished" ), null, actionCopy.getName(), actionCopy.getNr(), environmentSubstitute(
          actionCopy.getEntry().getFilename() ) );
      workflowTracker.addWorkflowTracker( new WorkflowTracker( workflowMeta, jerAfter ) );
      synchronized ( actionResults ) {
        actionResults.add( jerAfter );

        // Only keep the last X action results in memory
        //
        if ( maxJobEntriesLogged > 0 ) {
          while ( actionResults.size() > maxJobEntriesLogged ) {
            // Remove the oldest.
            actionResults.removeFirst();
          }
        }
      }
    }

    extension = new WorkflowExecutionExtension( this, prevResult, actionCopy, extension.executeEntry );
    ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobAfterJobEntryExecution.id, extension );

    // Try all next actions.
    //
    // Keep track of all the threads we fired in case of parallel execution...
    // Keep track of the results of these executions too.
    //
    final List<Thread> threads = new ArrayList<Thread>();
    // next 2 lists is being modified concurrently so must be synchronized for this case.
    final Queue<Result> threadResults = new ConcurrentLinkedQueue<Result>();
    final Queue<HopException> threadExceptions = new ConcurrentLinkedQueue<HopException>();
    final List<ActionCopy> threadEntries = new ArrayList<ActionCopy>();

    // Launch only those where the hop indicates true or false
    //
    int nrNext = workflowMeta.findNrNextActions( actionCopy );
    for ( int i = 0; i < nrNext && !isStopped(); i++ ) {
      // The next entry is...
      final ActionCopy nextEntry = workflowMeta.findNextAction( actionCopy, i );

      // See if we need to execute this...
      final WorkflowHopMeta hi = workflowMeta.findWorkflowHop( actionCopy, nextEntry );

      // The next comment...
      final String nextComment;
      if ( hi.isUnconditional() ) {
        nextComment = BaseMessages.getString( PKG, "Job.Comment.FollowedUnconditional" );
      } else {
        if ( newResult.getResult() ) {
          nextComment = BaseMessages.getString( PKG, "Job.Comment.FollowedSuccess" );
        } else {
          nextComment = BaseMessages.getString( PKG, "Job.Comment.FollowedFailure" );
        }
      }

      //
      // If the link is unconditional, execute the next action (entries).
      // If the start point was an evaluation and the link color is correct:
      // green or red, execute the next action...
      //
      if ( hi.isUnconditional() || ( actionCopy.evaluates() && ( !( hi.getEvaluation() ^ newResult
        .getResult() ) ) ) ) {
        // Start this next transform!
        if ( log.isBasic() ) {
          log.logBasic( BaseMessages.getString( PKG, "Job.Log.StartingEntry", nextEntry.getName() ) );
        }

        // Pass along the previous result, perhaps the next workflow can use it...
        // However, set the number of errors back to 0 (if it should be reset)
        // When an evaluation is executed the errors e.g. should not be reset.
        if ( nextEntry.resetErrorsBeforeExecution() ) {
          newResult.setNrErrors( 0 );
        }

        // Now execute!
        //
        // if (we launch in parallel, fire the execution off in a new thread...
        //
        if ( actionCopy.isLaunchingInParallel() ) {
          threadEntries.add( nextEntry );

          Runnable runnable = new Runnable() {
            @Override public void run() {
              try {
                Result threadResult = execute( nr + 1, newResult, nextEntry, actionCopy, nextComment );
                threadResults.add( threadResult );
              } catch ( Throwable e ) {
                log.logError( Const.getStackTracker( e ) );
                threadExceptions.add( new HopException( BaseMessages.getString( PKG, "Job.Log.UnexpectedError",
                  nextEntry.toString() ), e ) );
                Result threadResult = new Result();
                threadResult.setResult( false );
                threadResult.setNrErrors( 1L );
                threadResults.add( threadResult );
              }
            }
          };
          Thread thread = new Thread( runnable );
          threads.add( thread );
          thread.start();
          if ( log.isBasic() ) {
            log.logBasic( BaseMessages.getString( PKG, "Job.Log.LaunchedActionInParallel", nextEntry.getName() ) );
          }
        } else {
          try {
            // Same as before: blocks until it's done
            //
            res = execute( nr + 1, newResult, nextEntry, actionCopy, nextComment );
          } catch ( Throwable e ) {
            log.logError( Const.getStackTracker( e ) );
            throw new HopException( BaseMessages.getString( PKG, "Job.Log.UnexpectedError", nextEntry.toString() ),
              e );
          }
          if ( log.isBasic() ) {
            log.logBasic( BaseMessages.getString( PKG, "Job.Log.FinishedAction", nextEntry.getName(), res.getResult()
              + "" ) );
          }
        }
      }
    }

    // OK, if we run in parallel, we need to wait for all the actions to
    // finish...
    //
    if ( actionCopy.isLaunchingInParallel() ) {
      for ( int i = 0; i < threads.size(); i++ ) {
        Thread thread = threads.get( i );
        ActionCopy nextEntry = threadEntries.get( i );

        try {
          thread.join();
        } catch ( InterruptedException e ) {
          log.logError( workflowMeta.toString(), BaseMessages.getString( PKG,
            "Job.Log.UnexpectedErrorWhileWaitingForAction", nextEntry.getName() ) );
          threadExceptions.add( new HopException( BaseMessages.getString( PKG,
            "Job.Log.UnexpectedErrorWhileWaitingForAction", nextEntry.getName() ), e ) );
        }
      }
    }

    // Perhaps we don't have next transforms??
    // In this case, return the previous result.
    if ( res == null ) {
      res = prevResult;
    }

    // See if there where any errors in the parallel execution
    //
    if ( threadExceptions.size() > 0 ) {
      res.setResult( false );
      res.setNrErrors( threadExceptions.size() );

      for ( HopException e : threadExceptions ) {
        log.logError( workflowMeta.toString(), e.getMessage(), e );
      }

      // Now throw the first Exception for good measure...
      //
      throw threadExceptions.poll();
    }

    // In parallel execution, we aggregate all the results, simply add them to
    // the previous result...
    //
    for ( Result threadResult : threadResults ) {
      res.add( threadResult );
    }

    // If there have been errors, logically, we need to set the result to
    // "false"...
    //
    if ( res.getNrErrors() > 0 ) {
      res.setResult( false );
    }

    return res;
  }

  /**
   * Wait until this workflow has finished.
   */
  public void waitUntilFinished() {
    waitUntilFinished( -1L );
  }

  /**
   * Wait until this workflow has finished.
   *
   * @param maxMiliseconds the maximum number of ms to wait
   */
  public void waitUntilFinished( long maxMiliseconds ) {
    long time = 0L;
    while ( isAlive() && ( time < maxMiliseconds || maxMiliseconds <= 0 ) ) {
      try {
        Thread.sleep( 1 );
        time += 1;
      } catch ( InterruptedException e ) {
        // Ignore sleep errors
      }
    }
  }

  /**
   * Get the number of errors that happened in the workflow.
   *
   * @return nr of error that have occurred during execution. During execution of a workflow the number can change.
   */
  public int getErrors() {
    return errors.get();
  }

  /**
   * Set the number of occured errors to 0.
   */
  public void resetErrors() {
    errors.set( 0 );
  }

  /**
   * Add a number of errors to the total number of erros that occured during execution.
   *
   * @param nrToAdd nr of errors to add.
   */
  public void addErrors( int nrToAdd ) {
    if ( nrToAdd > 0 ) {
      errors.addAndGet( nrToAdd );
    }
  }

  /**
   * Handle logging at start
   *
   * @return true if it went OK.
   * @throws HopException
   */
  public boolean beginProcessing() throws HopException {

    resetErrors();

    WorkflowExecutionExtension extension = new WorkflowExecutionExtension( this, result, null, false );
    ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobBeginProcessing.id, extension );

    return true;
  }

  protected Database createDataBase( DatabaseMeta databaseMeta ) {
    return new Database( this, databaseMeta );
  }

  public boolean isInitialized() {
    int exist = status.get() & BitMaskStatus.INITIALIZED.mask;
    return exist != 0;
  }

  protected void setInitialized( boolean initialized ) {
    status.updateAndGet( v -> initialized ? v | BitMaskStatus.INITIALIZED.mask : ( BitMaskStatus.BIT_STATUS_SUM
      ^ BitMaskStatus.INITIALIZED.mask ) & v );
  }

  public boolean isActive() {
    int exist = status.get() & BitMaskStatus.ACTIVE.mask;
    return exist != 0;
  }

  protected void setActive( boolean active ) {
    status.updateAndGet( v -> active ? v | BitMaskStatus.ACTIVE.mask : ( BitMaskStatus.BIT_STATUS_SUM
      ^ BitMaskStatus.ACTIVE.mask ) & v );
  }

  public boolean isStopped() {
    int exist = status.get() & BitMaskStatus.STOPPED.mask;
    return exist != 0;
  }

  /**
   * Stop all activity by setting the stopped property to true.
   */
  public void stopAll() {
    setStopped( true );
  }

  /**
   * Sets the stopped.
   */
  public void setStopped( boolean stopped ) {
    status.updateAndGet( v -> stopped ? v | BitMaskStatus.STOPPED.mask : ( BitMaskStatus.BIT_STATUS_SUM
      ^ BitMaskStatus.STOPPED.mask ) & v );
  }

  public boolean isFinished() {
    int exist = status.get() & BitMaskStatus.FINISHED.mask;
    return exist != 0;
  }

  public void setFinished( boolean finished ) {
    status.updateAndGet( v -> finished ? v | BitMaskStatus.FINISHED.mask : ( BitMaskStatus.BIT_STATUS_SUM
      ^ BitMaskStatus.FINISHED.mask ) & v );
  }

  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  public Thread getThread() {
    return this;
  }

  public WorkflowTracker getWorkflowTracker() {
    return workflowTracker;
  }

  public void setWorkflowTracker( WorkflowTracker workflowTracker ) {
    this.workflowTracker = workflowTracker;
  }

  public void setSourceRows( List<RowMetaAndData> sourceRows ) {
    this.sourceRows = sourceRows;
  }

  /**
   * Gets the source rows.
   *
   * @return the source rows
   */
  public List<RowMetaAndData> getSourceRows() {
    return sourceRows;
  }

  /**
   * Gets the parent workflow.
   *
   * @return Returns the parentWorkflow
   */
  public Workflow getParentWorkflow() {
    return parentWorkflow;
  }

  /**
   * Sets the parent workflow.
   *
   * @param parentWorkflow The parentWorkflow to set.
   */
  public void setParentWorkflow( Workflow parentWorkflow ) {
    this.logLevel = parentWorkflow.getLogLevel();
    this.log.setLogLevel( logLevel );
    this.containerObjectId = log.getContainerObjectId();
    this.parentWorkflow = parentWorkflow;
  }

  public Result getResult() {
    return result;
  }

  public void setResult( Result result ) {
    this.result = result;
  }

  public long getBatchId() {
    return batchId;
  }

  public void setBatchId( long batchId ) {
    this.batchId = batchId;
  }

  public long getPassedBatchId() {
    return passedBatchId;
  }

  public void setPassedBatchId( long jobBatchId ) {
    this.passedBatchId = jobBatchId;
  }

  /**
   * Sets the internal kettle variables.
   *
   * @param var the new internal kettle variables.
   */
  public void setInternalHopVariables( IVariables var ) {
    boolean hasFilename = workflowMeta != null && !Utils.isEmpty( workflowMeta.getFilename() );
    if ( hasFilename ) { // we have a finename that's defined.
      try {
        FileObject fileObject = HopVfs.getFileObject( workflowMeta.getFilename(), this );
        FileName fileName = fileObject.getName();

        // The filename of the pipeline
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, fileName.getBaseName() );

        // The directory of the pipeline
        FileName fileDir = fileName.getParent();
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, fileDir.getURI() );
      } catch ( Exception e ) {
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, "" );
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "" );
      }
    } else {
      variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, "" );
      variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "" );
    }

    // The name of the workflow
    variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_NAME, Const.NVL( workflowMeta.getName(), "" ) );


  }

  protected void setInternalEntryCurrentDirectory( boolean hasFilename ) {
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, variables.getVariable(
      hasFilename ? Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY
        : Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#copyVariablesFrom(org.apache.hop.core.variables.IVariables)
   */
  @Override public void copyVariablesFrom( IVariables variables ) {
    this.variables.copyVariablesFrom( variables );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#environmentSubstitute(java.lang.String)
   */
  @Override public String environmentSubstitute( String aString ) {
    return variables.environmentSubstitute( aString );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#environmentSubstitute(java.lang.String[])
   */
  @Override public String[] environmentSubstitute( String[] aString ) {
    return variables.environmentSubstitute( aString );
  }

  @Override public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData )
    throws HopValueException {
    return variables.fieldSubstitute( aString, rowMeta, rowData );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getParentVariableSpace()
   */
  @Override public IVariables getParentVariableSpace() {
    return variables.getParentVariableSpace();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hop.core.variables.IVariables#setParentVariableSpace(org.apache.hop.core.variables.IVariables)
   */
  @Override public void setParentVariableSpace( IVariables parent ) {
    variables.setParentVariableSpace( parent );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getVariable(java.lang.String, java.lang.String)
   */
  @Override public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getVariable(java.lang.String)
   */
  @Override public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getBooleanValueOfVariable(java.lang.String, boolean)
   */
  @Override public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaString.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hop.core.variables.IVariables#initializeVariablesFrom(org.apache.hop.core.variables.IVariables)
   */
  @Override public void initializeVariablesFrom( IVariables parent ) {
    variables.initializeVariablesFrom( parent );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#listVariables()
   */
  @Override public String[] listVariables() {
    return variables.listVariables();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#setVariable(java.lang.String, java.lang.String)
   */
  @Override public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#shareVariablesWith(org.apache.hop.core.variables.IVariables)
   */
  @Override public void shareVariablesWith( IVariables variables ) {
    this.variables = variables;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#injectVariables(java.util.Map)
   */
  @Override public void injectVariables( Map<String, String> prop ) {
    variables.injectVariables( prop );
  }

  public String getStatus() {
    String message;

    if ( isActive() ) {
      if ( isStopped() ) {
        message = Pipeline.STRING_HALTING;
      } else {
        message = Pipeline.STRING_RUNNING;
      }
    } else if ( isFinished() ) {
      message = Pipeline.STRING_FINISHED;
      if ( getResult().getNrErrors() > 0 ) {
        message += " (with errors)";
      }
    } else if ( isStopped() ) {
      message = Pipeline.STRING_STOPPED;
      if ( getResult().getNrErrors() > 0 ) {
        message += " (with errors)";
      }
    } else {
      message = Pipeline.STRING_WAITING;
    }

    return message;
  }

  /**
   * Send to slave server.
   *
   * @param workflowMeta           the workflow meta
   * @param executionConfiguration the execution configuration
   * @param metaStore              the metaStore
   * @return the string
   * @throws HopException the kettle exception
   */
  public static String sendToSlaveServer( WorkflowMeta workflowMeta, WorkflowExecutionConfiguration executionConfiguration, IMetaStore metaStore ) throws HopException {
    String carteObjectId;
    SlaveServer slaveServer = executionConfiguration.getRemoteServer();

    if ( slaveServer == null ) {
      throw new HopException( BaseMessages.getString( PKG, "Job.Log.NoSlaveServerSpecified" ) );
    }
    if ( Utils.isEmpty( workflowMeta.getName() ) ) {
      throw new HopException( BaseMessages.getString( PKG, "Job.Log.UniqueJobName" ) );
    }

    // Align logging levels between execution configuration and remote server
    slaveServer.getLogChannel().setLogLevel( executionConfiguration.getLogLevel() );

    try {
      // Inject certain internal variables to make it more intuitive.
      //
      for ( String var : Const.INTERNAL_PIPELINE_VARIABLES ) {
        executionConfiguration.getVariablesMap().put( var, workflowMeta.getVariable( var ) );
      }
      for ( String var : Const.INTERNAL_WORKFLOW_VARIABLES ) {
        executionConfiguration.getVariablesMap().put( var, workflowMeta.getVariable( var ) );
      }

      if ( executionConfiguration.isPassingExport() ) {
        // First export the workflow... slaveServer.getVariable("MASTER_HOST")
        //
        FileObject tempFile =
          HopVfs.createTempFile( "jobExport", ".zip", System.getProperty( "java.io.tmpdir" ), workflowMeta );

        TopLevelResource topLevelResource =
          ResourceUtil.serializeResourceExportInterface( tempFile.getName().toString(), workflowMeta, workflowMeta,
            metaStore, executionConfiguration.getXml(), CONFIGURATION_IN_EXPORT_FILENAME );

        // Send the zip file over to the slave server...
        String result =
          slaveServer.sendExport( topLevelResource.getArchiveName(), RegisterPackageServlet.TYPE_JOB, topLevelResource
            .getBaseResourceName() );
        WebResult webResult = WebResult.fromXMLString( result );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error passing the exported workflow to the remote server: " + Const.CR
            + webResult.getMessage() );
        }
        carteObjectId = webResult.getId();
      } else {
        String xml = new WorkflowConfiguration( workflowMeta, executionConfiguration ).getXml();

        String reply = slaveServer.sendXML( xml, RegisterWorkflowServlet.CONTEXT_PATH + "/?xml=Y" );
        WebResult webResult = WebResult.fromXMLString( reply );
        if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
          throw new HopException( "There was an error posting the workflow on the remote server: " + Const.CR + webResult
            .getMessage() );
        }
        carteObjectId = webResult.getId();
      }

      // Start the workflow
      //
      String reply =
        slaveServer.execService( StartWorkflowServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( workflowMeta.getName(),
          "UTF-8" ) + "&xml=Y&id=" + carteObjectId );
      WebResult webResult = WebResult.fromXMLString( reply );
      if ( !webResult.getResult().equalsIgnoreCase( WebResult.STRING_OK ) ) {
        throw new HopException( "There was an error starting the workflow on the remote server: " + Const.CR + webResult
          .getMessage() );
      }
      return carteObjectId;
    } catch ( HopException ke ) {
      throw ke;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public void addJobListener( IWorkflowListener jobListener ) {
    synchronized ( jobListeners ) {
      jobListeners.add( jobListener );
    }
  }

  public void addJobEntryListener( IActionListener jobEntryListener ) {
    jobEntryListeners.add( jobEntryListener );
  }

  public void removeJobListener( IWorkflowListener jobListener ) {
    synchronized ( jobListeners ) {
      jobListeners.remove( jobListener );
    }
  }

  public void removeJobEntryListener( IActionListener jobEntryListener ) {
    jobEntryListeners.remove( jobEntryListener );
  }

  public List<IActionListener> getJobEntryListeners() {
    return jobEntryListeners;
  }

  public List<IWorkflowListener> getJobListeners() {
    synchronized ( jobListeners ) {
      return new ArrayList<IWorkflowListener>( jobListeners );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#addParameterDefinition(java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override public void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException {
    namedParams.addParameterDefinition( key, defValue, description );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#getParameterDescription(java.lang.String)
   */
  @Override public String getParameterDescription( String key ) throws UnknownParamException {
    return namedParams.getParameterDescription( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#getParameterDefault(java.lang.String)
   */
  @Override public String getParameterDefault( String key ) throws UnknownParamException {
    return namedParams.getParameterDefault( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#getParameterValue(java.lang.String)
   */
  @Override public String getParameterValue( String key ) throws UnknownParamException {
    return namedParams.getParameterValue( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#listParameters()
   */
  @Override public String[] listParameters() {
    return namedParams.listParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#setParameterValue(java.lang.String, java.lang.String)
   */
  @Override public void setParameterValue( String key, String value ) throws UnknownParamException {
    namedParams.setParameterValue( key, value );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#eraseParameters()
   */
  public void eraseParameters() {
    namedParams.eraseParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#clearParameters()
   */
  public void clearParameters() {
    namedParams.clearParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#activateParameters()
   */
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

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParams#copyParametersFrom(org.apache.hop.core.parameters.INamedParams)
   */
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
   * Gets the log channel interface.
   *
   * @return ILogChannel
   */
  public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Gets the workflow name.
   *
   * @return workflowName
   */
  public String getObjectName() {
    return getJobname();
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

  /**
   * Gets the log channel id.
   *
   * @return the logChannelId
   */
  public String getLogChannelId() {
    return log.getLogChannelId();
  }

  /**
   * Gets LoggingObjectType.JOB, which is always the value for Workflow.
   *
   * @return LoggingObjectType LoggingObjectType.JOB
   */
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.JOB;
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
   * Gets the logLevel.
   *
   * @return logLevel
   */
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
    List<LoggingHierarchy> hierarchy = new ArrayList<LoggingHierarchy>();
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
   * Gets the boolean value of interactive.
   *
   * @return the interactive
   */
  public boolean isInteractive() {
    return interactive;
  }

  /**
   * Sets the value of interactive.
   *
   * @param interactive the interactive to set
   */
  public void setInteractive( boolean interactive ) {
    this.interactive = interactive;
  }

  /**
   * Gets the activeJobEntryPipelines.
   *
   * @return the activeJobEntryPipelines
   */
  public Map<ActionCopy, ActionPipeline> getActiveJobEntryPipeline() {
    return activeJobEntryPipeline;
  }

  /**
   * Gets the activeJobEntryWorkflows.
   *
   * @return the activeJobEntryWorkflows
   */
  public Map<ActionCopy, ActionWorkflow> getActiveJobEntryWorkflows() {
    return activeJobEntryWorkflows;
  }

  /**
   * Gets a flat list of results in THIS workflow, in the order of execution of actions.
   *
   * @return A flat list of results in THIS workflow, in the order of execution of actions
   */
  public List<ActionResult> getActionResults() {
    synchronized ( actionResults ) {
      return new ArrayList<ActionResult>( actionResults );
    }
  }

  /**
   * Gets the carteObjectId.
   *
   * @return the carteObjectId
   */
  public String getContainerObjectId() {
    return containerObjectId;
  }

  /**
   * Sets the execution container object id (containerObjectId).
   *
   * @param containerObjectId the execution container object id to set
   */
  public void setContainerObjectId( String containerObjectId ) {
    this.containerObjectId = containerObjectId;
  }

  /**
   * Gets the parent logging object.
   *
   * @return the parent logging object
   */
  public ILoggingObject getParentLoggingObject() {
    return parentLoggingObject;
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
   * Gets the start action copy.
   *
   * @return the startActionCopy
   */
  public ActionCopy getStartActionCopy() {
    return startActionCopy;
  }

  /**
   * Sets the start action copy.
   *
   * @param startActionCopy the startActionCopy to set
   */
  public void setStartActionCopy( ActionCopy startActionCopy ) {
    this.startActionCopy = startActionCopy;
  }

  /**
   * Gets the executing server.
   *
   * @return the executingServer
   */
  public String getExecutingServer() {
    if ( executingServer == null ) {
      setExecutingServer( Const.getHostname() );
    }
    return executingServer;
  }

  /**
   * Sets the executing server.
   *
   * @param executingServer the executingServer to set
   */
  public void setExecutingServer( String executingServer ) {
    this.executingServer = executingServer;
  }

  /**
   * Gets the executing user.
   *
   * @return the executingUser
   */
  public String getExecutingUser() {
    return executingUser;
  }

  /**
   * Sets the executing user.
   *
   * @param executingUser the executingUser to set
   */
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

  public List<IDelegationListener> getDelegationListeners() {
    return delegationListeners;
  }

  public void setDelegationListeners( List<IDelegationListener> delegationListeners ) {
    this.delegationListeners = delegationListeners;
  }

  public void addDelegationListener( IDelegationListener delegationListener ) {
    delegationListeners.add( delegationListener );
  }

  public IPipelineEngine getParentPipeline() {
    return parentPipeline;
  }

  public void setParentPipeline( IPipelineEngine parentPipeline ) {
    this.parentPipeline = parentPipeline;
  }

  public Map<String, Object> getExtensionDataMap() {
    return extensionDataMap;
  }

  public Result getStartJobEntryResult() {
    return startJobEntryResult;
  }

  public void setStartJobEntryResult( Result startJobEntryResult ) {
    this.startJobEntryResult = startJobEntryResult;
  }

  protected ExecutorService startHeartbeat( final long intervalInSeconds ) {

    final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor( new ThreadFactory() {

      @Override
      public Thread newThread( Runnable r ) {
        Thread thread = new Thread( r, "Workflow Heartbeat Thread for: " + getName() );
        thread.setDaemon( true );
        return thread;
      }
    } );

    heartbeat.scheduleAtFixedRate( new Runnable() {
      public void run() {

        if ( Workflow.this.isFinished() ) {
          log.logBasic( "Shutting down heartbeat signal for " + workflowMeta.getName() );
          shutdownHeartbeat( heartbeat );
          return;
        }

        try {

          log.logDebug( "Triggering heartbeat signal for " + workflowMeta.getName() + " at every " + intervalInSeconds
            + " seconds" );
          ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobHeartbeat.id, Workflow.this );

        } catch ( HopException e ) {
          log.logError( e.getMessage(), e );
        }
      }
    }, intervalInSeconds /* initial delay */, intervalInSeconds /* interval delay */, TimeUnit.SECONDS );

    return heartbeat;
  }

  protected void shutdownHeartbeat( ExecutorService heartbeat ) {

    if ( heartbeat != null ) {

      try {
        heartbeat.shutdownNow(); // prevents waiting tasks from starting and attempts to stop currently executing ones

      } catch ( Throwable t ) {
        /* do nothing */
      }
    }
  }

  private int getHeartbeatIntervalInSeconds() {

    WorkflowMeta meta = this.workflowMeta;

    // 1 - check if there's a user defined value ( workflow-specific ) heartbeat periodic interval;
    // 2 - check if there's a default defined value ( workflow-specific ) heartbeat periodic interval;
    // 3 - use default Const.HEARTBEAT_PERIODIC_INTERVAL_IN_SECS if none of the above have been set

    try {

      if ( meta != null ) {

        return Const.toInt( meta.getParameterValue( Const.VARIABLE_HEARTBEAT_PERIODIC_INTERVAL_SECS ), Const.toInt( meta
            .getParameterDefault( Const.VARIABLE_HEARTBEAT_PERIODIC_INTERVAL_SECS ),
          Const.HEARTBEAT_PERIODIC_INTERVAL_IN_SECS ) );
      }

    } catch ( Exception e ) {
      /* do nothing, return Const.HEARTBEAT_PERIODIC_INTERVAL_IN_SECS */
    }

    return Const.HEARTBEAT_PERIODIC_INTERVAL_IN_SECS;
  }

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  public Date getExecutionStartDate() {
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
  public Date getExecutionEndDate() {
    return executionEndDate;
  }

  /**
   * @param executionEndDate The executionEndDate to set
   */
  public void setExecutionEndDate( Date executionEndDate ) {
    this.executionEndDate = executionEndDate;
  }
}
