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
 *
 */

package org.apache.hop.execution;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPluginType;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * This interface describes how execution information can be interacted with for a certain location.
 * We have methods to register and update execution information as well as methods to query this
 * information.
 */
@HopMetadataObject(objectFactory = IExecutionInfoLocation.ExecutionInfoLocationObjectFactory.class)
public interface IExecutionInfoLocation extends Cloneable {
  String getPluginId();

  void setPluginId(String pluginId);

  String getPluginName();

  void setPluginName(String pluginName);

  IExecutionInfoLocation clone();

  /**
   * Initialize a location. Load files, open database connections, ...
   *
   * @param variables
   * @param metadataProvider
   * @throws HopException
   */
  void initialize(IVariables variables, IHopMetadataProvider metadataProvider) throws HopException;

  /**
   * When you're done with this location you can call this method to clean up any left-over
   * temporary files, memory structures or database connections.
   *
   * @throws HopException
   */
  void close() throws HopException;

  /**
   * Remove any buffering or caching for the execution information with the given ID.
   *
   * @param executionId The ID of the execution information to remove from buffers or caches.
   * @throws HopException In case something goes wrong
   */
  void unBuffer(String executionId) throws HopException;

  /**
   * Register an execution of a pipeline or workflow at this location * *
   *
   * @param execution The execution information to register
   * @throws HopException In case there was a problem with the registration
   */
  void registerExecution(Execution execution) throws HopException;

  /**
   * update the execution details of an executor: pipeline, workflow, transform or action
   *
   * @param executionState The execution state to update
   * @throws HopException In case there was a problem with the update
   */
  void updateExecutionState(ExecutionState executionState) throws HopException;

  /**
   * Delete the execution with the given ID, its children and the associated information.
   *
   * @param executionId The ID of the execution to delete
   * @return true if the execution was found and deleted.
   * @throws HopException
   */
  boolean deleteExecution(String executionId) throws HopException;

  /**
   * Get the execution state for an execution. Any large logging text associated with the requested
   * execution state is also loaded.
   *
   * @param executionId The id of the execution
   * @return The state of the execution or null if not found
   * @throws HopException In case there was a problem reading the state
   */
  ExecutionState getExecutionState(String executionId) throws HopException;

  /**
   * Get the execution state for an execution
   *
   * @param executionId The id of the execution
   * @param includeLogging Set to true if the logging text (can be big) also needs to be loaded.
   * @return The state of the execution or null if not found
   * @throws HopException In case there was a problem reading the state
   */
  ExecutionState getExecutionState(String executionId, boolean includeLogging) throws HopException;

  /**
   * Load the logging text of an execution state separately.
   *
   * @param executionId The id of the execution to look for.
   * @param sizeLimit The maximum amount of characters to load from the logging text. Set the limit
   *     to {@literal <=0} if you want to load everything up to a global limit of usually 20MB.
   * @return The complete logging text
   * @throws HopException
   */
  String getExecutionStateLoggingText(String executionId, int sizeLimit) throws HopException;

  /**
   * register output data for a given transform
   *
   * @param data
   * @throws HopException
   */
  void registerData(ExecutionData data) throws HopException;

  /**
   * Retrieve a list of execution IDs (log channel IDs) for all pipelines and workflows. The list is
   * reverse ordered by (start of) execution date.
   *
   * @param includeChildren set to true if you want to see child executions of workflows and
   *     pipelines.
   * @param limit the maximum number of IDs to retrieve or a value {@literal <=0} to get all IDs.
   * @return The list of execution IDs
   * @throws HopException in case something went wrong
   */
  List<String> getExecutionIds(boolean includeChildren, int limit) throws HopException;

  /**
   * Get the execution information for a specific execution ID. This is the execution information of
   * a workflow or pipeline.
   *
   * @param executionId The ID of the execution to look for
   * @return The Execution or null if nothing was found
   * @throws HopException in case something went wrong
   */
  Execution getExecution(String executionId) throws HopException;

  /**
   * Find all the child executions for a particular execution ID. For example if you want to know
   * the execution of a particular action you can use this method.
   *
   * @param parentExecutionId The parent execution ID
   * @return A list of executions or an empty list if nothing was found.
   * @throws HopException In case of an unexpected error.
   */
  List<Execution> findExecutions(String parentExecutionId) throws HopException;

  /**
   * Find the previous successful execution of a pipeline or workflow.
   *
   * @param executionType The type of execution to look for
   * @param name The name of the executor
   * @return The execution or null if no previous successful execution could be found.
   * @throws HopException
   */
  Execution findPreviousSuccessfulExecution(ExecutionType executionType, String name)
      throws HopException;

  /**
   * Find executions with a matcher. This will parse through all executions in the location.
   *
   * @param matcher The matcher to allow you to filter any execution from the system.
   * @return A list of executions or an empty list if nothing was found.
   * @throws HopException In case of an unexpected error.
   */
  List<Execution> findExecutions(IExecutionMatcher matcher) throws HopException;

  /**
   * Get execution data for transforms or an action. The parent ID would typically be a pipeline ID,
   * and you'd get data for all the transforms. You can also get the execution data for specific
   * actions in a workflow (when finished).
   *
   * @param parentExecutionId The ID of the parent (pipeline) execution.
   * @param executionId The ID of the transforms (all transforms) or a specific action. Set this
   *     parameter to null if you want to collect all the data associated with the parent execution
   *     ID. This is for the Beam use case where we don't know up-front how many transforms are
   *     running or when they'll pop up.
   * @return The ExecutionData
   * @throws HopException In case something went wrong
   */
  ExecutionData getExecutionData(String parentExecutionId, String executionId) throws HopException;

  /**
   * Find the last execution of with a given type and name
   *
   * @param executionType The type to look for
   * @param name The name to match
   * @return The last execution or null if none could be found
   */
  Execution findLastExecution(ExecutionType executionType, String name) throws HopException;

  /**
   * Find children of an execution. A workflow can find child actions with this method.
   *
   * @param parentExecutionType The parent execution type (Workflow or Pipeline)
   * @param parentExecutionId The parent execution ID to look into
   * @return A list of IDs or an empty list if nothing could be found.
   * @throws HopException in case of a serialization error
   */
  List<String> findChildIds(ExecutionType parentExecutionType, String parentExecutionId)
      throws HopException;

  String findParentId(String childId) throws HopException;

  /**
   * This object factory is needed to instantiate the correct plugin class based on the value of the
   * Object ID which is simply the object ID.
   */
  final class ExecutionInfoLocationObjectFactory implements IHopMetadataObjectFactory {

    @Override
    public Object createObject(String id, Object parentObject) throws HopException {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.findPluginWithId(ExecutionInfoLocationPluginType.class, id);
      return registry.loadClass(plugin);
    }

    @Override
    public String getObjectId(Object object) throws HopException {
      if (!(object instanceof IExecutionInfoLocation)) {
        throw new HopException(
            "Object is not of class IExecutionInfoLocation but of "
                + object.getClass().getName()
                + "'");
      }
      return ((IExecutionInfoLocation) object).getPluginId();
    }
  }
}
