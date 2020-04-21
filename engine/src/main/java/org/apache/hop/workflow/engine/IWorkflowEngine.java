package org.apache.hop.workflow.engine;

import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface IWorkflowEngine<T extends WorkflowMeta> extends IVariables, ILoggingObject, INamedParams {

  String getWorkflowName();

  Result startExecution();

  Result getResult();

  void setResult( Result result );

  void stopExecution();

  boolean isInitialized();

  boolean isActive();

  boolean isFinished();

  void setFinished( boolean b );

  boolean isStopped();

  void setStopped( boolean b );

  void setLogLevel( LogLevel logLevel );

  Date getExecutionStartDate();

  Date getExecutionEndDate();

  void addWorkflowStartedListener( IExecutionStartedListener<IWorkflowEngine<T>> finishedListener );

  List<IExecutionFinishedListener<IWorkflowEngine<T>>> getWorkflowFinishedListeners();

  void addWorkflowFinishedListener( IExecutionFinishedListener<IWorkflowEngine<T>> finishedListener );

  List<IExecutionStartedListener<IWorkflowEngine<T>>> getWorkflowStartedListeners();

  boolean isInteractive();

  void setInteractive( boolean interactive );

  Map<ActionCopy, ActionPipeline> getActiveActionPipeline();

  Map<ActionCopy, ActionWorkflow> getActiveActionWorkflows();

  Map<String, Object> getExtensionDataMap();

  void addActionListener( IActionListener<T> refreshJobEntryListener );

  List<IActionListener> getActionListeners();

  void setStartActionCopy( ActionCopy startActionCopy );

  T getWorkflowMeta();

  void setWorkflowMeta( T workflowMeta );

  WorkflowTracker getWorkflowTracker();

  List<ActionResult> getActionResults();

  ILogChannel getLogChannel();

  void setWorkflowRunConfiguration( WorkflowRunConfiguration workflowRunConfiguration );

  IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration();

  void setParentWorkflow( IWorkflowEngine<WorkflowMeta> workflow );

  IWorkflowEngine<WorkflowMeta> getParentWorkflow();

  void setParentPipeline( IPipelineEngine<PipelineMeta> pipeline );

  IPipelineEngine<PipelineMeta> getParentPipeline();

  void setInternalHopVariables( IVariables variables );

  void setSourceRows( List<RowMetaAndData> sourceRows );

  void fireWorkflowFinishListeners() throws HopException;

  void fireWorkflowStartedListeners() throws HopException;

  void setContainerObjectId( String toString );

  String getContainerObjectId();

  String getStatusDescription();
}
