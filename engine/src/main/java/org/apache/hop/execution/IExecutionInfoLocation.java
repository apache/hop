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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPluginType;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;

import java.util.List;

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
   * Get the execution state for an execution
   * @param type the type of execution we want the status for
   * @param executionId The id of the execution
   * @return The state of the execution or null if not found
   * @throws HopException In case there was a problem reading the state
   */
  ExecutionState getExecutionState(ExecutionType type, String executionId) throws HopException;

  /**
   * register output data for a given transform
   * @param data
   * @throws HopException
   */
  void registerData(ExecutionData data) throws HopException;

  /**
   * Retrieve a list of execution IDs (log channel IDs) for all pipelines and workflows.
   * The list is reverse ordered by (start of) execution date.
   * @param pipelines Set to true if you want to retrieve pipeline execution IDs.
   * @param workflows Set to true if you want to retrieve workflow execution IDs.
   * @param limit the maximum number of IDs to retrieve or a value <=0 to get all IDs.
   * @return The list of execution IDs
   * @throws HopException in case something went wrong
   */
  List<String> getExecutionIds(boolean pipelines, boolean workflows, int limit) throws HopException;

  /**
   * Get the execution information for a specific execution ID.
   * This is the execution information of a workflow or pipeline.
   * @param executionId The ID of the execution to look for
   * @return The Execution
   * @throws HopException in case something went wrong
   */
  Execution getExecution(String executionId) throws HopException;

  /**
   * Get the aggregated execution data for the parent execution ID.
   * The parent ID would typically be a pipeline ID and you'd get data for all the transforms.
   * @param parentExecutionId The ID of the parent (pipeline) execution.
   * @return The aggregated ExecutionData for the given parent execution ID
   * @throws HopException In case something went wrong
   */
  ExecutionData getExecutionData(String parentExecutionId) throws HopException;

  /**
   * Find the last execution of with a given type and name
   * @param executionType The type to look for
   * @param name The name to match
   * @return The last execution or null if none could be found
   */
  Execution findLastExecution(ExecutionType executionType, String name) throws HopException;

    /**
   * This object factory is needed to instantiate the correct plugin class
   * based on the value of the Object ID which is simply the object ID.
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
                "Object is not of class IExecutionInfoLocation but of " + object.getClass().getName() + "'");
      }
      return ((IExecutionInfoLocation) object).getPluginId();
    }
  }
}
