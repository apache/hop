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

package org.apache.hop.execution.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionInfoLocationTypeMetadata;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.IExecutionMatcher;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.engines.remote.HopServerTypeMetadata;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.www.GetExecutionInfoServlet;
import org.apache.hop.www.RegisterExecutionInfoServlet;
import org.apache.http.client.utils.URIBuilder;

@GuiPlugin(description = "File execution information location GUI elements")
@ExecutionInfoLocationPlugin(
    id = "remote-location",
    name = "Remote location",
    description = "Stores execution information on a remote Hop server")
public class RemoteExecutionInfoLocation implements IExecutionInfoLocation {

  @HopMetadataProperty protected String pluginId;

  @HopMetadataProperty protected String pluginName;

  @GuiWidgetElement(
      id = "hopServer",
      order = "010",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      typeMetadata = HopServerTypeMetadata.class,
      toolTip = "i18n::RemoteExecutionInfoLocation.HopServer.Tooltip",
      label = "i18n::RemoteExecutionInfoLocation.HopServer.Label")
  @HopMetadataProperty(key = "server")
  protected String serverName;

  @GuiWidgetElement(
      id = "location",
      order = "020",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      typeMetadata = ExecutionInfoLocationTypeMetadata.class,
      toolTip = "i18n::RemoteExecutionInfoLocation.LocationName.Tooltip",
      label = "i18n::RemoteExecutionInfoLocation.LocationName.Label")
  @HopMetadataProperty(key = "location")
  protected String locationName;

  private HopServerMeta server;
  private ExecutionInfoLocation location;
  private IVariables variables;

  public RemoteExecutionInfoLocation() {}

  public RemoteExecutionInfoLocation(RemoteExecutionInfoLocation location) {
    this.pluginId = location.pluginId;
    this.pluginName = location.pluginName;
    this.serverName = location.serverName;
    this.locationName = location.locationName;
  }

  @Override
  public RemoteExecutionInfoLocation clone() {
    return new RemoteExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    this.variables = variables;
    try {
      if (StringUtils.isNotEmpty(serverName)) {
        server =
            metadataProvider.getSerializer(HopServerMeta.class).load(variables.resolve(serverName));
      }
      if (StringUtils.isNotEmpty(locationName)) {
        location =
            metadataProvider
                .getSerializer(ExecutionInfoLocation.class)
                .load(variables.resolve(locationName));
      }
      validateSettings();
    } catch (Exception e) {
      throw new HopException("Error initializing remote execution information location", e);
    }
  }

  @Override
  public void close() throws HopException {
    if (location != null) {
      location.getExecutionInfoLocation().close();
    }
  }

  @Override
  public void unBuffer(String executionId) throws HopException {
    if (location != null) {
      location.getExecutionInfoLocation().unBuffer(executionId);
    }
  }

  private String getJson(Object object) throws JsonProcessingException {
    return HopJson.newMapper().writeValueAsString(object);
  }

  private void validateSettings() throws HopException {
    if (server == null) {
      throw new HopException("Please specify a Hop server to send execution information to.");
    }
    if (location == null) {
      throw new HopException(
          "Please specify an execution information location (on the Hop server) to send execution information to.");
    }
  }

  @Override
  public void registerExecution(Execution execution) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(RegisterExecutionInfoServlet.CONTEXT_PATH + "/")
              .addParameter(
                  RegisterExecutionInfoServlet.PARAMETER_TYPE,
                  RegisterExecutionInfoServlet.TYPE_EXECUTION)
              .addParameter(RegisterExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .build();

      server.sendJson(variables, getJson(execution), uri.toString());
    } catch (Exception e) {
      throw new HopException("Error registering execution at remote location", e);
    }
  }

  @Override
  public boolean deleteExecution(String executionId) throws HopException {
    try {
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE,
                  GetExecutionInfoServlet.Type.DELETE.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, executionId)
              .build();

      validateSettings();

      String json = server.execService(variables, uri.toString());
      return Const.toBoolean(json);
    } catch (Exception e) {
      throw new HopException("Error deleting execution at remote location", e);
    }
  }

  @Override
  public void updateExecutionState(ExecutionState executionState) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(RegisterExecutionInfoServlet.CONTEXT_PATH + "/")
              .addParameter(
                  RegisterExecutionInfoServlet.PARAMETER_TYPE,
                  RegisterExecutionInfoServlet.TYPE_STATE)
              .addParameter(RegisterExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .build();

      server.sendJson(variables, getJson(executionState), uri.toString());
    } catch (Exception e) {
      throw new HopException("Error registering execution state at remote location", e);
    }
  }

  @Override
  public void registerData(ExecutionData data) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(RegisterExecutionInfoServlet.CONTEXT_PATH + "/")
              .addParameter(
                  RegisterExecutionInfoServlet.PARAMETER_TYPE,
                  RegisterExecutionInfoServlet.TYPE_DATA)
              .addParameter(RegisterExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .build();

      server.sendJson(variables, getJson(data), uri.toString());
    } catch (Exception e) {
      throw new HopException("Error registering execution data at remote location", e);
    }
  }

  @Override
  public List<String> getExecutionIds(boolean includeChildren, int limit) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE, GetExecutionInfoServlet.Type.IDS.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_CHILDREN, includeChildren ? "Y" : "N")
              .addParameter(GetExecutionInfoServlet.PARAMETER_LIMIT, Integer.toString(limit))
              .build();

      String json = server.execService(variables, uri.toString());
      String[] ids = HopJson.newMapper().readValue(json, String[].class);
      return Arrays.asList(ids);
    } catch (Exception e) {
      throw new HopException("Error get execution IDs from remote location", e);
    }
  }

  @Override
  public Execution getExecution(String executionId) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE,
                  GetExecutionInfoServlet.Type.EXECUTION.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, executionId)
              .build();

      String json = server.execService(variables, uri.toString());
      return HopJson.newMapper().readValue(json, Execution.class);
    } catch (Exception e) {
      throw new HopException("Error getting execution from remote location", e);
    }
  }

  @Override
  public ExecutionState getExecutionState(String executionId, boolean includeLogging)
      throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE, GetExecutionInfoServlet.Type.STATE.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, executionId)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_INCLUDE_LARGE_LOGGING,
                  includeLogging ? "Y" : "N")
              .build();

      String json = server.execService(variables, uri.toString());
      return HopJson.newMapper().readValue(json, ExecutionState.class);
    } catch (Exception e) {
      throw new HopException("Error getting execution state from remote location", e);
    }
  }

  @Override
  public String getExecutionStateLoggingText(String executionId, int sizeLimit)
      throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE,
                  GetExecutionInfoServlet.Type.STATE_LOGGING.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, executionId)
              .addParameter(GetExecutionInfoServlet.PARAMETER_LIMIT, Integer.toString(sizeLimit))
              .build();

      String loggingText = server.execService(variables, uri.toString());
      return HopJson.newMapper().readValue(loggingText, String.class);
    } catch (Exception e) {
      throw new HopException("Error reading state logging text for execution ID " + executionId, e);
    }
  }

  @Override
  public ExecutionState getExecutionState(String executionId) throws HopException {
    return getExecutionState(executionId, true);
  }

  @Override
  public List<Execution> findExecutions(String parentExecutionId) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE,
                  GetExecutionInfoServlet.Type.CHILDREN.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, parentExecutionId)
              .build();

      String json = server.execService(variables, uri.toString());
      Execution[] executions = HopJson.newMapper().readValue(json, Execution[].class);
      return Arrays.asList(executions);
    } catch (Exception e) {
      throw new HopException("Error getting execution state from remote location", e);
    }
  }

  @Override
  public List<Execution> findExecutions(IExecutionMatcher matcher) throws HopException {
    try {
      List<Execution> executions = new ArrayList<>();

      for (String id : getExecutionIds(true, 0)) {
        Execution execution = getExecution(id);
        if (matcher.matches(execution)) {
          executions.add(execution);
        }
      }
      return executions;
    } catch (Exception e) {
      throw new HopException("Error finding executions with a matcher", e);
    }
  }

  @Override
  public Execution findPreviousSuccessfulExecution(ExecutionType executionType, String name)
      throws HopException {
    try {
      List<Execution> executions =
          findExecutions(e -> e.getExecutionType() == executionType && name.equals(e.getName()));
      for (Execution execution : executions) {
        ExecutionState executionState = getExecutionState(execution.getId());
        if (executionState != null && !executionState.isFailed()) {
          return execution;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException("Error finding previous successful execution", e);
    }
  }

  @Override
  public ExecutionData getExecutionData(String parentExecutionId, String executionId)
      throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE, GetExecutionInfoServlet.Type.DATA.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_PARENT_ID, parentExecutionId)
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, executionId)
              .build();

      String json = server.execService(variables, uri.toString());
      return HopJson.newMapper().readValue(json, ExecutionData.class);
    } catch (Exception e) {
      throw new HopException("Error getting execution data from remote location", e);
    }
  }

  @Override
  public Execution findLastExecution(ExecutionType executionType, String name) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE,
                  GetExecutionInfoServlet.Type.LAST_EXECUTION.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_EXEC_TYPE, executionType.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_NAME, name)
              .build();

      String json = server.execService(variables, uri.toString());
      return HopJson.newMapper().readValue(json, Execution.class);
    } catch (Exception e) {
      throw new HopException("Error finding last execution from remote location", e);
    }
  }

  @Override
  public List<String> findChildIds(ExecutionType parentExecutionType, String parentExecutionId)
      throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE,
                  GetExecutionInfoServlet.Type.CHILD_IDS.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_EXEC_TYPE, parentExecutionType.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, parentExecutionId)
              .build();

      String json = server.execService(variables, uri.toString());
      String[] ids = HopJson.newMapper().readValue(json, String[].class);
      return Arrays.asList(ids);
    } catch (Exception e) {
      throw new HopException("Error finding execution child IDs from remote location", e);
    }
  }

  @Override
  public String findParentId(String childId) throws HopException {
    try {
      validateSettings();
      URI uri =
          new URIBuilder(GetExecutionInfoServlet.CONTEXT_PATH)
              .addParameter(
                  GetExecutionInfoServlet.PARAMETER_TYPE,
                  GetExecutionInfoServlet.Type.PARENT_ID.name())
              .addParameter(GetExecutionInfoServlet.PARAMETER_LOCATION, location.getName())
              .addParameter(GetExecutionInfoServlet.PARAMETER_ID, childId)
              .build();

      String json = server.execService(variables, uri.toString());
      return HopJson.newMapper().readValue(json, String.class);
    } catch (Exception e) {
      throw new HopException("Error finding parent execution ID from remote location", e);
    }
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  @Override
  public String getPluginId() {
    return pluginId;
  }

  /**
   * Sets pluginId
   *
   * @param pluginId value of pluginId
   */
  @Override
  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  /**
   * Gets pluginName
   *
   * @return value of pluginName
   */
  @Override
  public String getPluginName() {
    return pluginName;
  }

  /**
   * Sets pluginName
   *
   * @param pluginName value of pluginName
   */
  @Override
  public void setPluginName(String pluginName) {
    this.pluginName = pluginName;
  }

  /**
   * Gets server
   *
   * @return value of server
   */
  public HopServerMeta getServer() {
    return server;
  }

  /**
   * Gets serverName
   *
   * @return value of serverName
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * Sets serverName
   *
   * @param serverName value of serverName
   */
  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  /**
   * Gets locationName
   *
   * @return value of locationName
   */
  public String getLocationName() {
    return locationName;
  }

  /**
   * Sets locationName
   *
   * @param locationName value of locationName
   */
  public void setLocationName(String locationName) {
    this.locationName = locationName;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * Sets variables
   *
   * @param variables value of variables
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Sets server
   *
   * @param server value of server
   */
  public void setServer(HopServerMeta server) {
    this.server = server;
  }

  /**
   * Gets location
   *
   * @return value of location
   */
  public ExecutionInfoLocation getLocation() {
    return location;
  }

  /**
   * Sets location
   *
   * @param location value of location
   */
  public void setLocation(ExecutionInfoLocation location) {
    this.location = location;
  }
}
