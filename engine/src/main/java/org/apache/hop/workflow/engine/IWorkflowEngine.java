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

package org.apache.hop.workflow.engine;

import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface IWorkflowEngine<T extends WorkflowMeta> extends IVariables, ILoggingObject, INamedParameters {

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

  Map<ActionMeta, ActionPipeline> getActiveActionPipeline();

  Map<ActionMeta, ActionWorkflow> getActiveActionWorkflows();

  Map<String, Object> getExtensionDataMap();

  void addActionListener( IActionListener<T> refreshJobEntryListener );

  List<IActionListener> getActionListeners();

  void setStartActionMeta( ActionMeta actionMeta );

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

  void setInternalHopVariables();

  void setSourceRows( List<RowMetaAndData> sourceRows );

  void fireWorkflowFinishListeners() throws HopException;

  void fireWorkflowStartedListeners() throws HopException;

  void setContainerId( String toString );

  String getContainerId();

  String getStatusDescription();

  void setMetadataProvider( IHopMetadataProvider metadataProvider );

  IHopMetadataProvider getMetadataProvider();
}
