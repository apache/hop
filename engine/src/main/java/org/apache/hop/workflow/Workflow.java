/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IExecutor;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
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
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.Metrics;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.IExecutionStoppedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * This class executes a workflow as defined by a WorkflowMeta object.
 *
 * <p>The definition of a Hop workflow is represented by a WorkflowMeta object. It is typically
 * loaded from a .hwf file, or it is generated dynamically. The declared parameters of the workflow
 * definition are then queried using listParameters() and assigned values using calls to
 * setParameterValue(..).
 */
public abstract class Workflow extends Variables
    implements IVariables,
        INamedParameters,
        IHasLogChannel,
        ILoggingObject,
        IExecutor,
        IExtensionData,
        IWorkflowEngine<WorkflowMeta> {
  protected static final Class<?> PKG = Workflow.class;
  private static final String CONST_REASON_STARTED = "Workflow.Reason.Started";
  private static final String CONST_WORKFLOW_STARTED = "Workflow.Comment.WorkflowStarted";
  private static final String CONST_WORKFLOW_FINISHED = "Workflow.Comment.WorkflowFinished";
  private static final String CONST_ACTION_STARTED = "Workflow.Comment.ActionStarted";
  private static final String CONST_ACTION_FINISHED = "Workflow.Comment.ActionFinished";

  public static final String CONFIGURATION_IN_EXPORT_FILENAME =
      "__workflow_execution_configuration__.xml";

  protected ILogChannel log;

  protected WorkflowRunConfiguration workflowRunConfiguration;

  protected LogLevel logLevel = DefaultLogLevel.getLogLevel();

  protected String containerObjectId;

  protected WorkflowMeta workflowMeta;

  protected AtomicInteger errors;

  /**
   * The workflow that's launching this (sub-) workflow. This gives us access to the whole chain,
   * including the parent variables, etc.
   */
  protected IWorkflowEngine<WorkflowMeta> parentWorkflow;

  /** The parent pipeline */
  protected IPipelineEngine<PipelineMeta> parentPipeline;

  /** The parent logging interface to reference */
  protected ILoggingObject parentLoggingObject;

  /**
   * Keep a list of the actions that were executed.
   * org.apache.hop.core.logging.CentralLogStore.getInstance()
   */
  protected WorkflowTracker<?> workflowTracker;

  /** A flat list of results in THIS workflow, in the order of execution of actions */
  protected final LinkedList<ActionResult> actionResults = new LinkedList<>();

  protected Date executionStartDate;

  protected Date executionEndDate;

  /**
   * The rows that were passed onto this workflow by a previous pipeline. These rows are passed onto
   * the first workflow entry in this workflow (on the result object)
   */
  protected List<RowMetaAndData> sourceRows;

  /** The result of the workflow, after execution. */
  protected Result result;

  protected List<IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>>>
      executionFinishedListeners;
  protected List<IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>>>
      executionStartedListeners;
  protected List<IExecutionStoppedListener<IWorkflowEngine<WorkflowMeta>>>
      executionStoppedListeners;

  protected List<IActionListener> actionListeners;

  protected Set<ActionMeta> activeActions;

  /** Parameters of the workflow. */
  protected INamedParameters namedParams = new NamedParameters();

  protected int maxActionsLogged;

  protected ActionMeta startActionMeta;
  protected Result startActionResult;

  protected String executingServer;

  protected String executingUser;

  protected Map<String, Object> extensionDataMap;

  /** Int value for storage workflow statuses */
  protected AtomicInteger status;

  protected IHopMetadataProvider metadataProvider;

  /**
   * This enum stores bit masks which are used to manipulate with statuses over field {@link
   * Workflow#status}
   */
  enum BitMaskStatus {
    ACTIVE(1),
    INITIALIZED(2),
    STOPPED(4),
    FINISHED(8);

    private final int mask;
    // the sum of status masks
    public static final int BIT_STATUS_SUM = 63;

    BitMaskStatus(int mask) {
      this.mask = mask;
    }
  }

  private void init() {
    status = new AtomicInteger();

    executionStartedListeners = Collections.synchronizedList(new ArrayList<>());
    executionFinishedListeners = Collections.synchronizedList(new ArrayList<>());
    executionStoppedListeners = Collections.synchronizedList(new ArrayList<>());
    actionListeners = new ArrayList<>();

    // this map is being modified concurrently and must be thread-safe
    activeActions = Collections.synchronizedSet(new HashSet<>());

    extensionDataMap = new HashMap<>();

    workflowTracker = new WorkflowTracker(workflowMeta);
    synchronized (actionResults) {
      actionResults.clear();
    }
    errors = new AtomicInteger(0);
    maxActionsLogged = Const.toInt(EnvUtil.getSystemProperty(Const.HOP_MAX_ACTIONS_LOGGED), 1000);

    result = null;
    startActionMeta = null;
    startActionResult = null;
  }

  public Workflow(WorkflowMeta workflowMeta) {
    this(workflowMeta, null);
  }

  public Workflow(WorkflowMeta workflowMeta, ILoggingObject parentLogging) {
    super();
    this.workflowMeta = workflowMeta;
    this.parentLoggingObject = parentLogging;

    init();

    workflowTracker = new WorkflowTracker(workflowMeta);

    this.log = LogChannel.GENERAL;
    this.containerObjectId = UUID.randomUUID().toString();
  }

  public Workflow() {
    super();
    init();
    // Don't spam the logging backend for nothing. Don't create this.log here.
    this.log = LogChannel.GENERAL;
    this.logLevel = LogLevel.BASIC;
  }

  /**
   * Gets the name property of the WorkflowMeta property.
   *
   * @return String name for the WorkflowMeta
   */
  @Override
  public String toString() {
    if (workflowMeta == null || Utils.isEmpty(workflowMeta.getName())) {
      return super.toString();
    } else {
      return workflowMeta.getName();
    }
  }

  @Override
  public String getWorkflowName() {
    if (workflowMeta == null) {
      return null;
    }
    return workflowMeta.getName();
  }

  private Result newResult() {
    Result r = new Result();
    r.setContainerId(containerObjectId);
    return r;
  }

  private Result newErrorResult() {
    Result r = newResult();
    r.setResult(false);
    r.setNrErrors(1L);
    return r;
  }

  @Override
  public Result startExecution() {

    try {
      // Create a new log channel at every start of an execution
      // This is important if the same workflow is being executed in a loop.
      //
      this.log = new LogChannel(this, parentLoggingObject, isGatheringMetrics(), true);
      this.logLevel = log.getLogLevel();

      // Update the tracker, not always fully initialized
      this.workflowTracker.setWorkflowName(workflowMeta.getName());
      this.workflowTracker.setWorkflowFilename(workflowMeta.getFilename());

      executionStartDate = new Date();
      setStopped(false);
      setFinished(false);
      setInitialized(true);

      setInternalHopVariables();

      // Run the workflow
      //
      fireExecutionStartedListeners();

      result = executeFromStart();
    } catch (Throwable je) {
      log.logError(
          BaseMessages.getString(PKG, "Workflow.Log.ErrorExecWorkflow", je.getMessage()), je);

      //
      // we don't have result object because execute() threw a curve-ball.
      // So we create a new error object.
      //
      result = newErrorResult();
      addErrors(1); // This can be before actual execution

      emergencyWriteWorkflowTracker(result);

      setActive(false);
      setFinished(true);
      setStopped(false);
    } finally {
      try {
        executionEndDate = new Date();

        ExtensionPointHandler.callExtensionPoint(
            log, this, HopExtensionPoint.WorkflowFinish.id, this);

        log.logBasic(BaseMessages.getString(PKG, CONST_WORKFLOW_FINISHED));
        fireExecutionFinishedListeners();

        // release unused vfs connections
        HopVfs.freeUnusedResources();

      } catch (HopException e) {
        result.setNrErrors(1);
        result.setResult(false);
        log.logError(
            BaseMessages.getString(PKG, "Workflow.Log.ErrorExecWorkflow", e.getMessage()), e);

        emergencyWriteWorkflowTracker(result);
      }
    }

    return result;
  }

  private void emergencyWriteWorkflowTracker(Result result) {
    ActionResult actionResult =
        new ActionResult(
            result,
            this.getLogChannelId(),
            BaseMessages.getString(PKG, CONST_WORKFLOW_FINISHED),
            null,
            null,
            null);
    WorkflowTracker finalTrack = new WorkflowTracker(this.getWorkflowMeta(), actionResult);
    // workflowTracker is up to date too.
    this.workflowTracker.addWorkflowTracker(finalTrack);
  }

  /**
   * Execute a workflow without previous results. This is an action point (not recursive)<br>
   * <br>
   *
   * @return the result of the execution
   * @throws HopException
   */
  private Result executeFromStart() throws HopException {
    try {
      log.snap(Metrics.METRIC_WORKFLOW_START);

      setFinished(false);
      setStopped(false);
      HopEnvironment.setExecutionInformation(this);

      log.logBasic(BaseMessages.getString(PKG, CONST_WORKFLOW_STARTED));

      ExtensionPointHandler.callExtensionPoint(log, this, HopExtensionPoint.WorkflowStart.id, this);

      // Start the tracking...
      ActionResult jerStart =
          new ActionResult(
              null,
              null,
              BaseMessages.getString(PKG, CONST_WORKFLOW_STARTED),
              BaseMessages.getString(PKG, CONST_REASON_STARTED),
              null,
              null);
      workflowTracker.addWorkflowTracker(new WorkflowTracker(workflowMeta, jerStart));

      setActive(true);
      // Where do we start?
      ActionMeta startpoint;

      // synchronize this to a parent workflow if needed.
      //
      Object syncObject = this;
      if (parentWorkflow != null) {
        syncObject = parentWorkflow; // parallel execution in a workflow
      }

      synchronized (syncObject) {
        beginProcessing();
      }

      Result res = null;

      if (startActionMeta == null) {
        startpoint = workflowMeta.findStart();
      } else {
        startpoint = startActionMeta;
        res = startActionResult;
      }
      if (startpoint == null) {
        throw new HopWorkflowException(
            BaseMessages.getString(PKG, "Workflow.Log.CounldNotFindStartingPoint"));
      }

      ActionResult jerEnd = null;

      if (startpoint.isStart()) {
        // Perform optional looping in the special Start action...
        //

        boolean isFirst = true;

        // Use a result obj coming from input otherwise init an empty Result object
        Result inputRes = null;
        if (result != null) {
          inputRes = result;
        } else {
          inputRes = newResult();
        }

        // Perhaps there is already a list of input rows available?
        if (getSourceRows() != null) {
          inputRes.setRows(getSourceRows());
        }

        ActionStart jes = (ActionStart) startpoint.getAction();
        while ((jes.isRepeat() || isFirst) && !isStopped()) {
          isFirst = false;
          res =
              executeFromStart(
                  0, inputRes, startpoint, null, BaseMessages.getString(PKG, CONST_REASON_STARTED));
        }
        jerEnd =
            new ActionResult(
                res,
                jes.getLogChannelId(),
                BaseMessages.getString(PKG, CONST_WORKFLOW_FINISHED),
                BaseMessages.getString(PKG, "Workflow.Reason.Finished"),
                null,
                null);
      } else {
        res =
            executeFromStart(
                0, res, startpoint, null, BaseMessages.getString(PKG, CONST_REASON_STARTED));
        jerEnd =
            new ActionResult(
                res,
                startpoint.getAction().getLogChannel().getLogChannelId(),
                BaseMessages.getString(PKG, CONST_WORKFLOW_FINISHED),
                BaseMessages.getString(PKG, "Workflow.Reason.Finished"),
                null,
                null);
      }
      // Save this result...
      workflowTracker.addWorkflowTracker(new WorkflowTracker(workflowMeta, jerEnd));

      setActive(false);
      if (!isStopped()) {
        setFinished(true);
      }

      return res;
    } finally {
      log.snap(Metrics.METRIC_WORKFLOW_STOP);
    }
  }

  /**
   * Execute a workflow with previous results passed in.<br>
   * <br>
   * Execute called by ActionWorkflow: don't clear the actionResults.
   *
   * @param nr The action number
   * @param result the result of the previous execution
   * @return Result of the workflow execution
   * @throws HopWorkflowException
   */
  public Result executeFromStart(int nr, Result result) throws HopException {
    setFinished(false);
    setActive(true);
    setInitialized(true);
    HopEnvironment.setExecutionInformation(this);

    // Where do we start?
    ActionMeta startpoint;

    // Perhaps there is already a list of input rows available?
    if (getSourceRows() != null) {
      result.setRows(getSourceRows());
    }

    startpoint = workflowMeta.findStart();
    if (startpoint == null) {
      throw new HopWorkflowException(
          BaseMessages.getString(PKG, "Workflow.Log.CounldNotFindStartingPoint"));
    }

    ActionStart jes = (ActionStart) startpoint.getAction();
    Result res;
    do {
      res =
          executeFromStart(
              nr,
              result,
              startpoint,
              null,
              BaseMessages.getString(PKG, "Workflow.Reason.StartOfAction"));
      setActive(false);
    } while (jes.isRepeat() && !isStopped());
    return res;
  }

  /**
   * @deprecated
   * @param listener workflow started listener
   */
  @Override
  @Deprecated(since = "2.9", forRemoval = true)
  public void addWorkflowStartedListener(
      IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    addExecutionStartedListener(listener);
  }

  @Override
  public void addExecutionStartedListener(
      IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    synchronized (executionStartedListeners) {
      executionStartedListeners.add(listener);
    }
  }

  @Override
  public void removeExecutionStartedListener(
      IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    synchronized (executionStartedListeners) {
      executionStartedListeners.remove(listener);
    }
  }

  /**
   * @deprecated
   * @throws HopException
   */
  @Override
  @Deprecated(since = "2.9", forRemoval = true)
  public void fireWorkflowStartedListeners() throws HopException {
    fireExecutionStartedListeners();
  }

  @Override
  public void fireExecutionStartedListeners() throws HopException {
    synchronized (executionStartedListeners) {
      for (IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>> listener :
          executionStartedListeners) {
        listener.started(this);
      }
    }
  }

  /**
   * @deprecated
   * @param listener
   */
  @Override
  @Deprecated(since = "2.9", forRemoval = true)
  public void addWorkflowFinishedListener(
      IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    addExecutionFinishedListener(listener);
  }

  @Override
  public void addExecutionFinishedListener(
      IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    synchronized (executionFinishedListeners) {
      executionFinishedListeners.add(listener);
    }
  }

  @Override
  public void removeExecutionFinishedListener(
      IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    synchronized (executionFinishedListeners) {
      executionFinishedListeners.remove(listener);
    }
  }

  /**
   * @deprecated
   * @throws HopException
   */
  @Override
  @Deprecated(since = "2.9", forRemoval = true)
  public void fireWorkflowFinishListeners() throws HopException {
    fireExecutionFinishedListeners();
  }

  @Override
  public void fireExecutionFinishedListeners() throws HopException {
    synchronized (executionFinishedListeners) {
      for (IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>> listener :
          executionFinishedListeners) {
        listener.finished(this);
      }
    }
  }

  @Override
  public void addExecutionStoppedListener(
      IExecutionStoppedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    synchronized (executionStoppedListeners) {
      executionStoppedListeners.add(listener);
    }
  }

  @Override
  public void removeExecutionStoppedListener(
      IExecutionStoppedListener<IWorkflowEngine<WorkflowMeta>> listener) {
    synchronized (executionStoppedListeners) {
      executionStoppedListeners.remove(listener);
    }
  }

  @Override
  public void fireExecutionStoppedListeners() {
    synchronized (executionStoppedListeners) {
      for (IExecutionStoppedListener<IWorkflowEngine<WorkflowMeta>> listener :
          executionStoppedListeners) {
        listener.stopped(this);
      }
    }
  }

  /**
   * Execute a action recursively and move to the next action automatically.<br>
   * Uses a back-tracking algorithm.<br>
   *
   * @param nr
   * @param previousResult
   * @param actionMeta
   * @param previous
   * @param reason
   * @return
   * @throws HopException
   */
  private Result executeFromStart(
      final int nr,
      Result previousResult,
      final ActionMeta actionMeta,
      ActionMeta previous,
      String reason)
      throws HopException {
    Result res = null;

    if (isStopped()) {
      res = newResult();
      res.setEntryNr(nr);
      res.setStopped(true);
      return res;
    }

    // if we didn't have a previous result, create one, otherwise, copy the content...
    //
    final Result newResult;
    Result prevResult = null;
    if (previousResult != null) {
      prevResult = previousResult.clone();
    } else {
      prevResult = newResult();
    }

    WorkflowExecutionExtension extension =
        new WorkflowExecutionExtension(this, prevResult, actionMeta, true);
    ExtensionPointHandler.callExtensionPoint(
        log, this, HopExtensionPoint.WorkflowBeforeActionExecution.id, extension);

    if (extension.result != null) {
      prevResult = extension.result;
    }

    if (!extension.executeAction) {
      newResult = prevResult;
    } else {
      if (log.isDetailed()) {
        log.logDetailed(
            "exec("
                + nr
                + ", "
                + (prevResult != null ? prevResult.getNrErrors() : 0)
                + ", "
                + (actionMeta != null ? actionMeta.toString() : "null")
                + ")");
      }

      // Which action is next?
      IAction action = actionMeta.getAction();
      action.getLogChannel().setLogLevel(logLevel);

      // Track the fact that we are going to launch the next action...
      ActionResult jerBefore =
          new ActionResult(
              null,
              null,
              BaseMessages.getString(PKG, CONST_ACTION_STARTED),
              reason,
              actionMeta.getName(),
              resolve(actionMeta.getAction().getFilename()));
      workflowTracker.addWorkflowTracker(new WorkflowTracker(workflowMeta, jerBefore));

      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(action.getClass().getClassLoader());

      // Execute this action
      IAction cloneAction = (IAction) action.clone();
      cloneAction.copyFrom(this);
      cloneAction.getLogChannel().setLogLevel(getLogLevel());
      cloneAction.setMetadataProvider(metadataProvider);
      cloneAction.setParentWorkflow(this);
      cloneAction.setParentWorkflowMeta(this.getWorkflowMeta());
      final long start = System.currentTimeMillis();

      cloneAction.getLogChannel().logDetailed("Starting action");
      for (IActionListener actionListener : actionListeners) {
        actionListener.beforeExecution(this, actionMeta, cloneAction);
      }

      activeActions.add(actionMeta.clone());

      log.snap(Metrics.METRIC_ACTION_START, cloneAction.toString());
      newResult = cloneAction.execute(prevResult, nr);
      log.snap(Metrics.METRIC_ACTION_STOP, cloneAction.toString());

      // Action execution duration
      newResult.setElapsedTimeMillis(System.currentTimeMillis() - start);

      activeActions.remove(actionMeta);

      for (IActionListener actionListener : actionListeners) {
        actionListener.afterExecution(this, actionMeta, cloneAction, newResult);
      }

      Thread.currentThread().setContextClassLoader(cl);
      addErrors((int) newResult.getNrErrors());

      // Also capture the logging text after the execution...
      //
      LoggingBuffer loggingBuffer = HopLogStore.getAppender();
      StringBuffer logTextBuffer =
          loggingBuffer.getBuffer(cloneAction.getLogChannel().getLogChannelId(), false);
      newResult.setLogText(logTextBuffer.toString() + newResult.getLogText());

      // Save this result as well...
      //
      ActionResult jerAfter =
          new ActionResult(
              newResult,
              cloneAction.getLogChannel().getLogChannelId(),
              BaseMessages.getString(PKG, CONST_ACTION_FINISHED),
              null,
              actionMeta.getName(),
              resolve(actionMeta.getAction().getFilename()));
      workflowTracker.addWorkflowTracker(new WorkflowTracker(workflowMeta, jerAfter));
      synchronized (actionResults) {
        actionResults.add(jerAfter);

        // Only keep the last X action results in memory
        //
        if (maxActionsLogged > 0) {
          while (actionResults.size() > maxActionsLogged) {
            // Remove the oldest.
            actionResults.removeFirst();
          }
        }
      }
    }

    extension =
        new WorkflowExecutionExtension(this, prevResult, actionMeta, extension.executeAction);
    ExtensionPointHandler.callExtensionPoint(
        log, this, HopExtensionPoint.WorkflowAfterActionExecution.id, extension);

    // Try all next actions.
    //
    // Keep track of all the threads we fired in case of parallel execution...
    // Keep track of the results of these executions too.
    //
    final List<Thread> threads = new ArrayList<>();
    // next 2 lists is being modified concurrently so must be synchronized for this case.
    final Queue<Result> threadResults = new ConcurrentLinkedQueue<>();
    final Queue<HopException> threadExceptions = new ConcurrentLinkedQueue<>();
    final List<ActionMeta> threadActions = new ArrayList<>();

    // Launch only those where the hop indicates true or false
    //
    int nrNext = workflowMeta.findNrNextActions(actionMeta);
    for (int i = 0; i < nrNext && !isStopped(); i++) {
      // The next entry is...
      final ActionMeta nextAction = workflowMeta.findNextAction(actionMeta, i);

      // See if we need to execute this...
      final WorkflowHopMeta hopMeta = workflowMeta.findWorkflowHop(actionMeta, nextAction);

      // The next comment...
      final String nextComment;
      if (hopMeta.isUnconditional()) {
        nextComment = BaseMessages.getString(PKG, "Workflow.Comment.FollowedUnconditional");
      } else {
        if (newResult.getResult()) {
          nextComment = BaseMessages.getString(PKG, "Workflow.Comment.FollowedSuccess");
        } else {
          nextComment = BaseMessages.getString(PKG, "Workflow.Comment.FollowedFailure");
        }
      }

      //
      // If the link is unconditional, execute the next action (entries).
      // If the start point was an evaluation and the link color is correct:
      // green or red, execute the next action...
      //
      if (hopMeta.isUnconditional()
          || (actionMeta.isEvaluation() && (hopMeta.isEvaluation() == newResult.getResult()))) {

        // If the next action is a join, only execute once
        if (nextAction.isJoin()) {
          if (activeActions.contains(nextAction)) {
            continue;
          }
        }

        // Start this next action!
        if (log.isBasic()) {
          log.logBasic(
              BaseMessages.getString(PKG, "Workflow.Log.StartingAction", nextAction.getName()));
        }

        // Pass along the previous result, perhaps the next workflow can use it...
        // However, set the number of errors back to 0 (if it should be reset)
        // When an evaluation is executed the errors e.g. should not be reset.
        if (nextAction.resetErrorsBeforeExecution()) {
          newResult.setNrErrors(0);
        }

        // Now execute!
        //
        // if we launch in parallel, fire the execution off in a new thread...
        //
        if (actionMeta.isLaunchingInParallel()) {
          threadActions.add(nextAction);

          Runnable runnable =
              () -> {
                try {
                  Result threadResult =
                      executeFromStart(nr + 1, newResult, nextAction, actionMeta, nextComment);
                  threadResults.add(threadResult);
                } catch (Throwable e) {
                  log.logError(Const.getStackTracker(e));
                  threadExceptions.add(
                      new HopException(
                          BaseMessages.getString(
                              PKG, "Workflow.Log.UnexpectedError", nextAction.toString()),
                          e));
                  Result threadResult = newErrorResult();
                  threadResults.add(threadResult);
                }
              };
          Thread thread = new Thread(runnable);
          threads.add(thread);
          thread.setName("Workflow " + workflowMeta.getName() + ':' + nextAction.getName());
          thread.start();
          if (log.isBasic()) {
            log.logBasic(
                BaseMessages.getString(
                    PKG, "Workflow.Log.LaunchedActionInParallel", nextAction.getName()));
          }
        } else {
          try {
            // Same as before: blocks until it's done
            //
            res = executeFromStart(nr + 1, newResult, nextAction, actionMeta, nextComment);
          } catch (Throwable e) {
            log.logError(Const.getStackTracker(e));
            throw new HopException(
                BaseMessages.getString(PKG, "Workflow.Log.UnexpectedError", nextAction.toString()),
                e);
          }
          if (log.isBasic()) {
            log.logBasic(
                BaseMessages.getString(
                    PKG,
                    "Workflow.Log.FinishedAction",
                    nextAction.getName(),
                    res.getResult() + ""));
          }
        }
      }
    }

    // OK, if we run in parallel, we need to wait for all the actions to
    // finish...
    //
    if (actionMeta.isLaunchingInParallel()) {
      for (int i = 0; i < threads.size(); i++) {
        Thread thread = threads.get(i);
        ActionMeta nextAction = threadActions.get(i);

        try {
          thread.join();
        } catch (InterruptedException e) {
          log.logError(
              workflowMeta.toString(),
              BaseMessages.getString(
                  PKG, "Workflow.Log.UnexpectedErrorWhileWaitingForAction", nextAction.getName()));
          threadExceptions.add(
              new HopException(
                  BaseMessages.getString(
                      PKG,
                      "Workflow.Log.UnexpectedErrorWhileWaitingForAction",
                      nextAction.getName()),
                  e));
        }
      }
    }

    // Perhaps we don't have next transforms??
    // In this case, return the previous result.
    if (res == null) {
      res = prevResult;
    }

    // See if there were any errors in the parallel execution
    //
    if (!threadExceptions.isEmpty()) {
      res.setResult(false);
      res.setNrErrors(threadExceptions.size());

      for (HopException e : threadExceptions) {
        log.logError(workflowMeta.toString(), e.getMessage(), e);
      }

      // Now throw the first Exception for good measure...
      //
      throw threadExceptions.poll();
    }

    // In parallel execution, we aggregate all the results, simply add them to
    // the previous result...
    //
    for (Result threadResult : threadResults) {
      res.add(threadResult);
    }

    // If there have been errors, logically, we need to set the result to
    // "false"...
    //
    if (res.getNrErrors() > 0) {
      res.setResult(false);
    }

    return res;
  }

  /**
   * Get the number of errors that happened in the workflow.
   *
   * @return nr of error that have occurred during execution. During execution of a workflow the
   *     number can change.
   */
  public int getErrors() {
    return errors.get();
  }

  /** Set the number of occured errors to 0. */
  public void resetErrors() {
    errors.set(0);
  }

  /**
   * Add a number of errors to the total number of errors that occurred during execution.
   *
   * @param nrToAdd nr of errors to add.
   */
  public void addErrors(int nrToAdd) {
    if (nrToAdd > 0) {
      errors.addAndGet(nrToAdd);
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

    WorkflowExecutionExtension extension =
        new WorkflowExecutionExtension(this, result, null, false);
    ExtensionPointHandler.callExtensionPoint(
        log, this, HopExtensionPoint.WorkflowBeginProcessing.id, extension);

    return true;
  }

  @Override
  public boolean isInitialized() {
    int exist = status.get() & BitMaskStatus.INITIALIZED.mask;
    return exist != 0;
  }

  protected void setInitialized(boolean initialized) {
    status.updateAndGet(
        v ->
            initialized
                ? v | BitMaskStatus.INITIALIZED.mask
                : (BitMaskStatus.BIT_STATUS_SUM ^ BitMaskStatus.INITIALIZED.mask) & v);
  }

  @Override
  public boolean isActive() {
    int exist = status.get() & BitMaskStatus.ACTIVE.mask;
    return exist != 0;
  }

  protected void setActive(boolean active) {
    status.updateAndGet(
        v ->
            active
                ? v | BitMaskStatus.ACTIVE.mask
                : (BitMaskStatus.BIT_STATUS_SUM ^ BitMaskStatus.ACTIVE.mask) & v);
  }

  @Override
  public boolean isStopped() {
    int exist = status.get() & BitMaskStatus.STOPPED.mask;
    boolean stopped = exist != 0;

    // Keep an eye on the stopped state of a parent workflow and pipeline as well
    //
    if (parentWorkflow != null && parentWorkflow.isStopped()) {
      stopped = true;
    }
    if (parentPipeline != null && parentPipeline.isStopped()) {
      stopped = true;
    }
    return stopped;
  }

  /** Stop all activity by setting the stopped property to true. */
  @Override
  public void stopExecution() {
    setStopped(true);

    log.logBasic(BaseMessages.getString(PKG, "Workflow.Log.StopWorkflowExecution"));
    fireExecutionStoppedListeners();
  }

  /** Sets the stopped. */
  @Override
  public void setStopped(boolean stopped) {
    status.updateAndGet(
        v ->
            stopped
                ? v | BitMaskStatus.STOPPED.mask
                : (BitMaskStatus.BIT_STATUS_SUM ^ BitMaskStatus.STOPPED.mask) & v);
  }

  @Override
  public boolean isFinished() {
    int exist = status.get() & BitMaskStatus.FINISHED.mask;
    return exist != 0;
  }

  @Override
  public void setFinished(boolean finished) {
    status.updateAndGet(
        v ->
            finished
                ? v | BitMaskStatus.FINISHED.mask
                : (BitMaskStatus.BIT_STATUS_SUM ^ BitMaskStatus.FINISHED.mask) & v);
  }

  @Override
  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  /**
   * @param workflowMeta The workflowMeta to set
   */
  @Override
  public void setWorkflowMeta(WorkflowMeta workflowMeta) {
    this.workflowMeta = workflowMeta;
  }

  @Override
  public WorkflowTracker getWorkflowTracker() {
    return workflowTracker;
  }

  public void setWorkflowTracker(WorkflowTracker workflowTracker) {
    this.workflowTracker = workflowTracker;
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
  @Override
  public void setSourceRows(List<RowMetaAndData> sourceRows) {
    this.sourceRows = sourceRows;
  }

  /**
   * Gets the parent workflow.
   *
   * @return Returns the parentWorkflow
   */
  @Override
  public IWorkflowEngine<WorkflowMeta> getParentWorkflow() {
    return parentWorkflow;
  }

  /**
   * Sets the parent workflow.
   *
   * @param parentWorkflow The parentWorkflow to set.
   */
  @Override
  public void setParentWorkflow(IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    this.parentWorkflow = parentWorkflow;
    if (parentWorkflow != null) {
      this.logLevel = parentWorkflow.getLogLevel();
    }
    if (log != null) {
      this.log.setLogLevel(logLevel);
      this.containerObjectId = log.getContainerObjectId();
    }
  }

  @Override
  public IVariables getParentVariables() {
    if (getParentPipeline() != null) {
      return getParentPipeline();
    }
    if (getParentWorkflow() != null) {
      return getParentWorkflow();
    }
    return super.getParentVariables();
  }

  @Override
  public Result getResult() {
    return result;
  }

  @Override
  public void setResult(Result result) {
    this.result = result;
  }

  @Override
  public void setInternalHopVariables() {
    if (workflowMeta == null) {
      setInternalHopVariables(this, null, null);
    } else {
      workflowMeta.setInternalHopVariables(this);
    }
    this.setVariable(
        Const.INTERNAL_VARIABLE_WORKFLOW_ID, log != null ? log.getLogChannelId() : null);
    if (parentLoggingObject != null) {
      this.setVariable(
          Const.INTERNAL_VARIABLE_WORKFLOW_PARENT_ID, parentLoggingObject.getLogChannelId());
    } else {
      this.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_PARENT_ID, null);
    }
  }

  /**
   * Sets the internal hop variables.
   *
   * @param variables the variables in which we want to set the internal variables
   * @param filename the filename if there is any
   * @param name the name of the workflow
   */
  public static void setInternalHopVariables(IVariables variables, String filename, String name) {
    boolean hasFilename = !Utils.isEmpty(filename);
    if (hasFilename) { // we have a filename that's defined.
      try {
        FileObject fileObject = HopVfs.getFileObject(filename);
        FileName fileName = fileObject.getName();

        // The filename of the pipeline
        variables.setVariable(
            Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, fileName.getBaseName());

        // The directory of the pipeline
        FileName fileDir = fileName.getParent();
        variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, fileDir.getURI());
      } catch (Exception e) {
        variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "");
        variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "");
      }
    } else {
      variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "");
      variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "");
    }

    // The name of the workflow
    variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_NAME, Const.NVL(name, ""));
  }

  @Override
  public String getStatusDescription() {
    String message;

    if (isActive()) {
      if (isStopped()) {
        message = Pipeline.STRING_HALTING;
      } else {
        message = Pipeline.STRING_RUNNING;
      }
    } else if (isFinished()) {
      message = Pipeline.STRING_FINISHED;
      if (getResult().getNrErrors() > 0) {
        message += " (with errors)";
      }
    } else if (isStopped()) {
      message = Pipeline.STRING_STOPPED;
      if (getResult().getNrErrors() > 0) {
        message += " (with errors)";
      }
    } else {
      message = Pipeline.STRING_WAITING;
    }

    return message;
  }

  @Override
  public void addActionListener(IActionListener actionListener) {
    actionListeners.add(actionListener);
  }

  public void removeActionListener(IActionListener actionListener) {
    actionListeners.remove(actionListener);
  }

  @Override
  public List<IActionListener> getActionListeners() {
    return actionListeners;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#addParameterDefinition(java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override
  public void addParameterDefinition(String key, String defValue, String description)
      throws DuplicateParamException {
    namedParams.addParameterDefinition(key, defValue, description);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDescription(java.lang.String)
   */
  @Override
  public String getParameterDescription(String key) throws UnknownParamException {
    return namedParams.getParameterDescription(key);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDefault(java.lang.String)
   */
  @Override
  public String getParameterDefault(String key) throws UnknownParamException {
    return namedParams.getParameterDefault(key);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterValue(java.lang.String)
   */
  @Override
  public String getParameterValue(String key) throws UnknownParamException {
    return namedParams.getParameterValue(key);
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
  public void setParameterValue(String key, String value) throws UnknownParamException {
    namedParams.setParameterValue(key, value);
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
    namedParams.activateParameters(variables);
  }

  @Override
  public void copyParametersFromDefinitions(INamedParameterDefinitions definitions) {
    namedParams.copyParametersFromDefinitions(definitions);
  }

  /**
   * Gets the log channel interface.
   *
   * @return ILogChannel
   */
  @Override
  public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Gets the workflow name.
   *
   * @return workflowName
   */
  @Override
  public String getObjectName() {
    return getWorkflowName();
  }

  /**
   * Always returns null for Workflow.
   *
   * @return null
   */
  @Override
  public String getObjectCopy() {
    return null;
  }

  /**
   * Gets the file name.
   *
   * @return the filename
   */
  @Override
  public String getFilename() {
    if (workflowMeta == null) {
      return null;
    }
    return workflowMeta.getFilename();
  }

  /**
   * Gets the log channel id.
   *
   * @return the logChannelId
   */
  @Override
  public String getLogChannelId() {
    return log == null ? null : log.getLogChannelId();
  }

  /**
   * Gets LoggingObjectType.JOB, which is always the value for Workflow.
   *
   * @return LoggingObjectType LoggingObjectType.JOB
   */
  @Override
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.WORKFLOW;
  }

  /**
   * Gets parent logging object.
   *
   * @return parentLoggingObject
   */
  @Override
  public ILoggingObject getParent() {
    return parentLoggingObject;
  }

  /**
   * Gets the logLevel.
   *
   * @return logLevel
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
  @Override
  public void setLogLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
    if (log != null) {
      log.setLogLevel(logLevel);
    }
  }

  /**
   * Gets the boolean value of interactive.
   *
   * @return the interactive
   */
  @Override
  public boolean isInteractive() {
    return true;
  }

  /**
   * Sets the value of interactive.
   *
   * @param interactive the interactive to set
   */
  @Override
  public void setInteractive(boolean interactive) {}

  /**
   * Gets the active actions.
   *
   * @return the active actions
   */
  @Override
  public Set<ActionMeta> getActiveActions() {
    return activeActions;
  }

  /**
   * Gets a flat list of results in THIS workflow, in the order of execution of actions.
   *
   * @return A flat list of results in THIS workflow, in the order of execution of actions
   */
  @Override
  public List<ActionResult> getActionResults() {
    synchronized (actionResults) {
      return new ArrayList<>(actionResults);
    }
  }

  /**
   * Gets the serverObjectId.
   *
   * @return the serverObjectId
   */
  @Override
  public String getContainerId() {
    return containerObjectId;
  }

  /**
   * Sets the execution container object id (containerObjectId).
   *
   * @param containerId the execution container object id to set
   */
  @Override
  public void setContainerId(String containerId) {
    this.containerObjectId = containerId;
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
   * @param parentLoggingObject The parentLoggingObject to set
   */
  public void setParentLoggingObject(ILoggingObject parentLoggingObject) {
    this.parentLoggingObject = parentLoggingObject;
  }

  /**
   * Gets the registration date. For workflow, this always returns null
   *
   * @return null
   */
  @Override
  public Date getRegistrationDate() {
    return null;
  }

  /**
   * Gets the start action meta.
   *
   * @return the startActionMeta
   */
  public ActionMeta getStartActionMeta() {
    return startActionMeta;
  }

  /**
   * Sets the start action meta.
   *
   * @param actionMeta the startActionMeta to set
   */
  @Override
  public void setStartActionMeta(ActionMeta actionMeta) {
    this.startActionMeta = actionMeta;
  }

  /**
   * Gets the executing server.
   *
   * @return the executingServer
   */
  @Override
  public String getExecutingServer() {
    if (executingServer == null) {
      setExecutingServer(Const.getHostname());
    }
    return executingServer;
  }

  /**
   * Sets the executing server.
   *
   * @param executingServer the executingServer to set
   */
  @Override
  public void setExecutingServer(String executingServer) {
    this.executingServer = executingServer;
  }

  /**
   * Gets the executing user.
   *
   * @return the executingUser
   */
  @Override
  public String getExecutingUser() {
    return executingUser;
  }

  /**
   * Sets the executing user.
   *
   * @param executingUser the executingUser to set
   */
  @Override
  public void setExecutingUser(String executingUser) {
    this.executingUser = executingUser;
  }

  @Override
  public boolean isGatheringMetrics() {
    return log != null && log.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics(boolean gatheringMetrics) {
    if (log != null) {
      log.setGatheringMetrics(gatheringMetrics);
    }
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return log != null && log.isForcingSeparateLogging();
  }

  @Override
  public void setForcingSeparateLogging(boolean forcingSeparateLogging) {
    if (log != null) {
      log.setForcingSeparateLogging(forcingSeparateLogging);
    }
  }

  @Override
  public IPipelineEngine<PipelineMeta> getParentPipeline() {
    return parentPipeline;
  }

  @Override
  public void setParentPipeline(IPipelineEngine<PipelineMeta> parentPipeline) {
    this.parentPipeline = parentPipeline;
  }

  @Override
  public Map<String, Object> getExtensionDataMap() {
    return extensionDataMap;
  }

  public Result getStartActionResult() {
    return startActionResult;
  }

  public void setStartActionResult(Result startActionResult) {
    this.startActionResult = startActionResult;
  }

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  @Override
  public Date getExecutionStartDate() {
    return executionStartDate;
  }

  /**
   * @param executionStartDate The executionStartDate to set
   */
  public void setExecutionStartDate(Date executionStartDate) {
    this.executionStartDate = executionStartDate;
  }

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  @Override
  public Date getExecutionEndDate() {
    return executionEndDate;
  }

  /**
   * @param executionEndDate The executionEndDate to set
   */
  public void setExecutionEndDate(Date executionEndDate) {
    this.executionEndDate = executionEndDate;
  }

  /**
   * @deprecated Gets workflowFinishedListeners
   * @return value of workflowFinishedListeners
   */
  @Override
  @Deprecated(since = "2.9", forRemoval = true)
  public List<IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>>>
      getWorkflowFinishedListeners() {
    return executionFinishedListeners;
  }

  /**
   * @deprecated
   * @param workflowFinishedListeners The workflowFinishedListeners to set
   */
  @Deprecated(since = "2.9", forRemoval = true)
  public void setWorkflowFinishedListeners(
      List<IExecutionFinishedListener<IWorkflowEngine<WorkflowMeta>>> workflowFinishedListeners) {
    this.executionFinishedListeners = workflowFinishedListeners;
  }

  /**
   * @deprecated Gets workflowStartedListeners
   * @return value of workflowStartedListeners
   */
  @Override
  @Deprecated(since = "2.9", forRemoval = true)
  public List<IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>>>
      getWorkflowStartedListeners() {
    return executionStartedListeners;
  }

  /**
   * @deprecated
   * @param workflowStartedListeners The workflowStartedListeners to set
   */
  @Deprecated(since = "2.9", forRemoval = true)
  public void setWorkflowStartedListeners(
      List<IExecutionStartedListener<IWorkflowEngine<WorkflowMeta>>> workflowStartedListeners) {
    this.executionStartedListeners = workflowStartedListeners;
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
  @Override
  public void setWorkflowRunConfiguration(WorkflowRunConfiguration workflowRunConfiguration) {
    this.workflowRunConfiguration = workflowRunConfiguration;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  @Override
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }
}
