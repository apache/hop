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

package org.apache.hop.workflow.engine;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.IExtensionData;
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
import org.apache.hop.pipeline.IExecutionStoppedListener;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;

public interface IWorkflowEngine<T extends WorkflowMeta>
    extends IVariables, ILoggingObject, INamedParameters, IExtensionData {

  String getWorkflowName();

  Result startExecution();

  Result getResult();

  void setResult(Result result);

  void stopExecution();

  boolean isInitialized();

  boolean isActive();

  boolean isFinished();

  void setFinished(boolean b);

  boolean isStopped();

  void setStopped(boolean b);

  void setLogLevel(LogLevel logLevel);

  Date getExecutionStartDate();

  Date getExecutionEndDate();

  /**
   * @deprecated Use the {@link #addExecutionStartedListener} method.
   */
  @Deprecated(since = "2.9", forRemoval = true)
  void addWorkflowStartedListener(IExecutionStartedListener<IWorkflowEngine<T>> listener);

  /**
   * @deprecated
   * @return
   */
  @Deprecated(since = "2.9", forRemoval = true)
  List<IExecutionFinishedListener<IWorkflowEngine<T>>> getWorkflowFinishedListeners();

  /**
   * @deprecated Use the {@link #addExecutionFinishedListener} method.
   */
  @Deprecated(since = "2.9", forRemoval = true)
  void addWorkflowFinishedListener(IExecutionFinishedListener<IWorkflowEngine<T>> listener);

  /**
   * @deprecated
   * @return
   */
  @Deprecated(since = "2.9", forRemoval = true)
  List<IExecutionStartedListener<IWorkflowEngine<T>>> getWorkflowStartedListeners();

  /**
   * Attach a listener to notify when the workflow has started execution.
   *
   * @param listener the workflow started listener
   */
  void addExecutionStartedListener(IExecutionStartedListener<IWorkflowEngine<T>> listener);

  /**
   * Detach a listener to notify when the workflow has started execution.
   *
   * @param listener the workflow started listener
   */
  void removeExecutionStartedListener(IExecutionStartedListener<IWorkflowEngine<T>> listener);

  /**
   * Attach a listener to notify when the workflow has stopped execution.
   *
   * @param listener the workflow stopped listener
   */
  void addExecutionStoppedListener(IExecutionStoppedListener<IWorkflowEngine<T>> listener);

  /**
   * Detach a listener to notify when the workflow has stopped execution.
   *
   * @param listener the workflow stopped listener
   */
  void removeExecutionStoppedListener(IExecutionStoppedListener<IWorkflowEngine<T>> listener);

  /**
   * Attach a listener to notify when the workflow has completed execution.
   *
   * @param listener the workflow finished listener
   */
  void addExecutionFinishedListener(IExecutionFinishedListener<IWorkflowEngine<T>> listener);

  /**
   * Detach a listener to notify when the workflow has completed execution.
   *
   * @param listener the workflow finished listener
   */
  void removeExecutionFinishedListener(IExecutionFinishedListener<IWorkflowEngine<T>> listener);

  @Deprecated(since = "2.15", forRemoval = true)
  boolean isInteractive();

  @Deprecated(since = "2.15", forRemoval = true)
  void setInteractive(boolean interactive);

  Set<ActionMeta> getActiveActions();

  @Override
  Map<String, Object> getExtensionDataMap();

  void addActionListener(IActionListener<T> refreshJobEntryListener);

  List<IActionListener> getActionListeners();

  void setStartActionMeta(ActionMeta actionMeta);

  T getWorkflowMeta();

  void setWorkflowMeta(T workflowMeta);

  WorkflowTracker getWorkflowTracker();

  List<ActionResult> getActionResults();

  ILogChannel getLogChannel();

  void setWorkflowRunConfiguration(WorkflowRunConfiguration workflowRunConfiguration);

  WorkflowRunConfiguration getWorkflowRunConfiguration();

  IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration();

  void setParentWorkflow(IWorkflowEngine<WorkflowMeta> workflow);

  IWorkflowEngine<WorkflowMeta> getParentWorkflow();

  void setParentPipeline(IPipelineEngine<PipelineMeta> pipeline);

  IPipelineEngine<PipelineMeta> getParentPipeline();

  void setInternalHopVariables();

  void setSourceRows(List<RowMetaAndData> sourceRows);

  /**
   * @deprecated Use the {@link #fireExecutionFinishedListeners} method.
   * @throws HopException
   */
  @Deprecated(since = "2.9", forRemoval = true)
  void fireWorkflowFinishListeners() throws HopException;

  /**
   * @deprecated Use the {@link #fireExecutionStartedListeners} method.
   * @throws HopException
   */
  @Deprecated(since = "2.9", forRemoval = true)
  void fireWorkflowStartedListeners() throws HopException;

  /**
   * Make attempt to fire all registered started execution listeners if possible.
   *
   * @throws HopException if any errors occur during notification
   */
  void fireExecutionStartedListeners() throws HopException;

  /**
   * Make attempt to fire all registered stopped execution listeners if possible.
   *
   * @throws HopException if any errors occur during notification
   */
  void fireExecutionStoppedListeners() throws HopException;

  /**
   * Make attempt to fire all registered finished execution listeners if possible.
   *
   * @throws HopException if any errors occur during notification
   */
  void fireExecutionFinishedListeners() throws HopException;

  void setContainerId(String toString);

  @Override
  String getContainerId();

  String getStatusDescription();

  void setMetadataProvider(IHopMetadataProvider metadataProvider);

  IHopMetadataProvider getMetadataProvider();
}
