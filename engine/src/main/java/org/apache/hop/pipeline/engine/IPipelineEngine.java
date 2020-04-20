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

package org.apache.hop.pipeline.engine;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.workflow.Workflow;

import java.util.Date;
import java.util.List;

/**
 * Describes the capabilities of an execution engine for a certain type of object called the subject
 *
 * @param <T> The subject class to execute
 */
public interface IPipelineEngine<T> extends IVariables, ILoggingObject {

  T getSubject();

  void setSubject(T t);

  String getPluginId();
  void setPluginId(String pluginId);

  /**
   * Ask the engine to generate a default pipeline engine configuration for this engine
   * @return a new pipeline engine run configuration
   */
  IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration();

  void setPipelineRunConfiguration( PipelineRunConfiguration pipelineRunConfiguration);

  PipelineRunConfiguration getPipelineRunConfiguration();

  /**
   * Executes the object/subject: calls prepareExecution and startThreads in sequence.
   *
   * @throws HopException if the object execution could not be prepared (initialized)
   */
  void execute() throws HopException;

  /**
   * Prepares for execution. This includes setting the parameters as well as preparing
   * anything needed in terms of resources, threads,....
   * It runs initialization on all the work that is needed to be done up-front.
   *
   * @throws HopException In case of an initialization error
   */
  void prepareExecution() throws HopException;

  /**
   * @return true if the engine is preparing execution
   */
  boolean isPreparing();

  /**
   * Check to see if the pipeline engine is ready to start threads.
   * @return True if the pipeline engine was prepared and is ready to start.
   */
  boolean isReadyToStart();

  /**
   * Starts the engine itself after prepareExecution.
   *
   * @throws HopException If there is an engine error during execution.
   */
  void startThreads() throws HopException;

  /**
   * Collect metrics from the various components in the engine.
   *
   * @return The engine metrics
   */
  EngineMetrics getEngineMetrics();

  /**
   * Get the engine metrics for a specific component name and/or copy nr
   * @param componentName the name of the component or null for all components
   * @param copyNr The copy nr to select or a negative number for all components
   * @return The engine metrics for the given
   */
  EngineMetrics getEngineMetrics(String componentName, int copyNr);

  /**
   * This method performs any cleanup operations on the various sub-components of the engine after execution.
   */
  void cleanup();

  /**
   * Wait until the execution is done.
   */
  void waitUntilFinished();

  /**
   * Stops all parts of the execution from running and alerts any registered listeners.
   */
  void stopAll();

  /**
   * See if there are any halted components in the engine: actions, auditing, ...
   * @return True if there are halted components
   */
  public boolean hasHaltedComponents();

  /**
   * Indicates whether or not the engine is running
   * @return True is the engine is running
   */
  boolean isRunning();

  /**
   * Indicates whether or not the engine is stopped.
   * @return True is the engine execution has stopped
   */
  boolean isStopped();

  /**
   * Pauses the execution (all components).
   */
  void pauseExecution();

  /**
   * Resume the execution (all components).
   */
  void resumeExecution();

  /**
   * @return a number larger than 0 in case of errors
   */
  int getErrors();

  /**
   * See if the engine has finished executing.
   *
   * @return True if the execution has finished
   */
  boolean isFinished();

  /**
   * See if the engine is paused
   *
   * @return true if the engine is paused
   */
  boolean isPaused();

  /**
   * Call the given listener lambda when this pipeline engine has started execution.
   *
   * @param listener
   * @throws HopException
   */
  void addExecutionStartedListener( IExecutionStartedListener<T> listener ) throws HopException;

  /**
   * Call the given listener lambda when this pipeline engine has completed execution.
   *
   * @param listener
   * @throws HopException
   */
  void addExecutionFinishedListener( IExecutionFinishedListener<T> listener ) throws HopException;

  /**
   * Retrieve the logging text of a particular component in the engine
   *
   * @param componentName The name of the component (transform)
   * @param copyNr        The copy number if multiple components are running in parallel (0 based).
   * @return The logging text
   */
  String getComponentLogText( String componentName, int copyNr );

  /**
   * Get a list of components
   *
   * @return The list of components in the engine
   */
  List<IEngineComponent> getComponents();

  /**
   * Find all component copies by name
   *
   * @param name   The name of the component to look for (transform)
   * @return The list of components found
   */
  List<IEngineComponent> getComponentCopies( String name );

  /**
   * Find a component by name and copy nr
   *
   * @param name   The name of the component to look for (transform)
   * @param copyNr The copy number to match
   * @return The component or null if nothing was found
   */
  IEngineComponent findComponent( String name, int copyNr );

  /**
   * Gets the log channel interface for the pipeline.
   *
   * @return the log channel
   */
  ILogChannel getLogChannel();

  /**
   * The log channel ID if there is any
   * @return
   */
  String getLogChannelId();

  /**
   * Sets the parent logging object.
   *
   * @param parent the new parent
   */
  void setParent( ILoggingObject parent );

  /**
   * Get the result of the execution after it's done, a resume
   *
   * @return The Result of the execution of the object
   */
  Result getResult();


  void setMetaStore( IMetaStore metaStore );

  IMetaStore getMetaStore();

  void setLogLevel( LogLevel logLevel );

  LogLevel getLogLevel();


  /**
   * Temporary until we find a better way to preview/debug
   * @param preview
   */
  @Deprecated
  void setPreview( boolean preview );

  /**
   * Temporary until we find a better way to preview/debug
   * @return
   */
  @Deprecated
  boolean isPreview();

  /**
   * Retrieve output rows from a component copy.  Pass the rows to the rows received lambda.
   *
   * @param componentName
   * @param copyNr
   * @param nrRows
   * @param rowsReceived
   * @throws HopException
   */
  void retrieveComponentOutput( String componentName, int copyNr, int nrRows, IPipelineComponentRowsReceived rowsReceived ) throws HopException;

  /**
   * Determine the pipeline engine which is executing this pipeline engine.
   * @return The executing pipeline or null if none is known.
   */
  IPipelineEngine<T> getParentPipeline();

  /**
   * Determine the workflow engine which is executing this pipeline engine.
   *
   * @return The executing workflow of null if none is known.
   */
  Workflow getParentWorkflow();

  /**
   * True if the engine is doing extra validations at runtime to detect possible issues with data types and so on.
   * @return True if safe mode is enabled.
   */
  boolean isSafeModeEnabled();

  /**
   * For engines that support it we allow the retrieval of a rowset from one transform copy to another
   * @param fromTransformName
   * @param fromTransformCopy
   * @param toTransformName
   * @param toTransformCopy
   * @return The rowset if one was found.
   * @throws RuntimeException in case the engine doesn't support this operation.
   */
  IRowSet findRowSet( String fromTransformName, int fromTransformCopy, String toTransformName, int toTransformCopy );

  /**
   * True if feedback need to be given every X rows
   * @return True if feedback needs to be given
   */
  @Deprecated // TODO: move this to the run configuration API
  boolean isFeedbackShown();

  /**
   * The feedback size in rows
   * @return The feedback size in rows
   */
  @Deprecated // TODO: move the run configuration API
  int getFeedbackSize();

  /**
   * Get the execution result of a previous execution in a workflow
   * @return the previous execution result
   */
  Result getPreviousResult();

  /**
   * @return The start date of the pipeline execution
   */
  Date getExecutionStartDate();

  /**
   * @return The end date of the pipeline preparation
   */
  Date getExecutionEndDate();

  /**
   * Add an active sub-pipeline to allow drill-down in the GUI
   * @param transformName
   * @param executorPipeline
   */
  void addActiveSubPipeline( String transformName, IPipelineEngine executorPipeline );

  /**
   * Get the active sub-pipeline with the given name
   * @param subPipelineName
   * @return The active pipeline engine or null if it was not found
   */
  IPipelineEngine getActiveSubPipeline( final String subPipelineName );

  /**
   * Add an active sub-workflow to allow drill-down in the GUI
   * @param subWorkflowName
   * @param subWorkflow
   */
  void addActiveSubWorkflow( final String subWorkflowName, Workflow subWorkflow );

  /**
   * Get the active sub-workflow with the given name
   * @param subWorkflowName
   * @return The active workflow or null if nothing was found
   */
  Workflow getActiveSubWorkflow( final String subWorkflowName );

}
