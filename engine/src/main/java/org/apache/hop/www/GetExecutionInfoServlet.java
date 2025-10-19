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

package org.apache.hop.www;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.xml.bind.DataBindingException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

@HopServerServlet(id = "getExecInfo", name = "Get execution information")
public class GetExecutionInfoServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = GetExecutionInfoServlet.class;

  private static final long serialVersionUID = -1624876141322415729L;

  public static final String CONTEXT_PATH = "/hop/getExecInfo";
  public static final String PARAMETER_TYPE = "type";
  public static final String PARAMETER_LOCATION = "location";
  public static final String PARAMETER_CHILDREN = "children";
  public static final String PARAMETER_LIMIT = "limit";
  public static final String PARAMETER_INCLUDE_LARGE_LOGGING = "includeLargeLogging";
  public static final String PARAMETER_ID = "id";
  public static final String PARAMETER_NAME = "name";
  public static final String PARAMETER_EXEC_TYPE = "execType";
  public static final String PARAMETER_PARENT_ID = "parentId";

  public enum Type {
    STATE,
    STATE_LOGGING,
    IDS,
    EXECUTION,
    CHILDREN,
    DATA,
    LAST_EXECUTION,
    CHILD_IDS,
    PARENT_ID,
    DELETE
  }

  public GetExecutionInfoServlet() {}

  public GetExecutionInfoServlet(WorkflowMap workflowMap) {
    super(workflowMap);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "GetWorkflowStatusServlet.Log.WorkflowStatusRequested"));
    }

    // Set character encoding before getting writer
    //
    response.setContentType("application/json");
    response.setCharacterEncoding(Const.XML_ENCODING);

    PrintWriter out = response.getWriter();

    // The type of information to request
    //
    String typeString = request.getParameter(PARAMETER_TYPE);

    // The name of the location is also in a parameter
    //
    String locationName = request.getParameter(PARAMETER_LOCATION);

    try {
      // validate the parameters
      //
      if (StringUtils.isEmpty(typeString)) {
        throw new HopException(
            "Please specify the type of execution information to register with parameter 'type'");
      }
      Type type = Type.valueOf(typeString);

      if (StringUtils.isEmpty(locationName)) {
        throw new HopException(
            "Please specify the name of the execution information location to register at with parameter 'location'");
      }

      if (log.isDebug()) {
        logDebug(
            "Execution information requested of type "
                + typeString
                + " and location "
                + locationName);
      }

      // Look up the location in the metadata.
      //
      MultiMetadataProvider provider = pipelineMap.getHopServerConfig().getMetadataProvider();
      IHopMetadataSerializer<ExecutionInfoLocation> serializer =
          provider.getSerializer(ExecutionInfoLocation.class);
      ExecutionInfoLocation location = serializer.load(locationName);
      if (location == null) {
        throw new HopException("Unable to find execution information location " + locationName);
      }

      IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

      // Initialize the location.
      iLocation.initialize(variables, provider);

      try {
        switch (type) {
          case STATE:
            {
              // Get the state of an execution: we need an execution ID
              //
              String id = request.getParameter(PARAMETER_ID);
              if (StringUtils.isEmpty(id)) {
                throw new HopException(
                    "Please specify the ID of execution state with parameter 'id'");
              }
              ExecutionState executionState =
                  location.getExecutionInfoLocation().getExecutionState(id);
              outputExecutionStateAsJson(out, executionState);
            }
            break;
          case STATE_LOGGING:
            {
              // Get the logging text for the state of an execution: we need an execution ID
              //
              String id = request.getParameter(PARAMETER_ID);
              if (StringUtils.isEmpty(id)) {
                throw new HopException(
                    "Please specify the ID of execution state with parameter 'id'");
              }
              // -1 means: no limit. The underlying plugin(s) might still limit though.
              // Log files can get really large.
              //
              String sizeLimitString = request.getParameter(PARAMETER_LIMIT);
              int sizeLimit = Const.toInt(sizeLimitString, -1);
              String loggingText =
                  location.getExecutionInfoLocation().getExecutionStateLoggingText(id, sizeLimit);
              outputIdAsJson(out, loggingText);
            }
            break;
          case IDS:
            {
              String children = request.getParameter(PARAMETER_CHILDREN);
              boolean includeChildren =
                  "Y".equalsIgnoreCase(children) || "true".equalsIgnoreCase(children);
              String limit = request.getParameter(PARAMETER_LIMIT);
              int limitNr = Const.toInt(limit, 100);
              List<String> ids =
                  location.getExecutionInfoLocation().getExecutionIds(includeChildren, limitNr);
              outputExecutionIdsAsJson(out, ids);
            }
            break;
          case EXECUTION:
            {
              // Get an execution: we need an execution ID
              //
              String id = request.getParameter(PARAMETER_ID);
              if (StringUtils.isEmpty(id)) {
                throw new HopException("Please specify the execution ID with parameter 'id'");
              }
              Execution execution = location.getExecutionInfoLocation().getExecution(id);
              outputExecutionAsJson(out, execution);
            }
            break;
          case CHILDREN:
            {
              String id = request.getParameter(PARAMETER_ID);
              if (StringUtils.isEmpty(id)) {
                throw new HopException(
                    "Please specify the parent execution ID with parameter 'id'");
              }
              List<Execution> children = location.getExecutionInfoLocation().findExecutions(id);
              outputExecutionChildrenAsJson(out, children);
            }
            break;
          case DATA:
            {
              // Get execution data: we need an execution ID and its parent
              //
              String parentId = request.getParameter(PARAMETER_PARENT_ID);
              if (StringUtils.isEmpty(parentId)) {
                throw new HopException(
                    "Please specify the parent execution ID with parameter 'parentId'");
              }
              String id = request.getParameter("id");
              ExecutionData data =
                  location.getExecutionInfoLocation().getExecutionData(parentId, id);
              outputExecutionDataAsJson(out, data);
            }
            break;
          case LAST_EXECUTION:
            {
              // Get the last execution: we need an execution type and a name
              //
              String name = request.getParameter(PARAMETER_NAME);
              if (StringUtils.isEmpty(name)) {
                throw new HopException(
                    "Please specify the name of the last execution to find with parameter 'name'");
              }
              String execType = request.getParameter(PARAMETER_EXEC_TYPE);
              if (StringUtils.isEmpty(execType)) {
                throw new HopException(
                    "Please specify the type of the last execution to find with parameter 'execType'");
              }
              ExecutionType executionType = ExecutionType.valueOf(execType);

              Execution execution =
                  location.getExecutionInfoLocation().findLastExecution(executionType, name);
              outputExecutionAsJson(out, execution);
            }
            break;
          case CHILD_IDS:
            {
              String execType = request.getParameter(PARAMETER_EXEC_TYPE);
              if (StringUtils.isEmpty(execType)) {
                throw new HopException(
                    "Please specify the type of execution to find children for with parameter 'execType'");
              }
              ExecutionType executionType = ExecutionType.valueOf(execType);

              String id = request.getParameter(PARAMETER_ID);
              if (StringUtils.isEmpty(id)) {
                throw new HopException(
                    "Please specify the ID of execution to find children for with parameter 'id'");
              }

              List<String> ids =
                  location.getExecutionInfoLocation().findChildIds(executionType, id);
              outputExecutionIdsAsJson(out, ids);
            }
            break;
          case PARENT_ID:
            {
              String id = request.getParameter(PARAMETER_ID);
              if (StringUtils.isEmpty(id)) {
                throw new HopException(
                    "Please specify the child execution ID to find the parent for with parameter 'id'");
              }
              String parentId = location.getExecutionInfoLocation().findParentId(id);
              outputIdAsJson(out, parentId);
            }
            break;
          case DELETE:
            {
              String id = request.getParameter(PARAMETER_ID);
              if (StringUtils.isEmpty(id)) {
                throw new HopException(
                    "Please specify the ID of the execution to delete with parameter 'id'");
              }
              boolean deleted = location.getExecutionInfoLocation().deleteExecution(id);
              outputSuccessAsJson(out, deleted);
            }
            break;
          default:
            StringBuilder message =
                new StringBuilder("Unknown update type: " + type + ". Allowed values are: ");
            for (Type typeValue : Type.values()) {
              message.append(typeValue.name()).append(" ");
            }
            throw new HopException(message.toString());
        }
      } finally {
        iLocation.close();
      }
    } catch (Exception e) {
      String message = Const.getStackTracker(e);
      try {
        HopJson.newMapper().writeValue(out, message);
      } catch (IOException | DataBindingException ex) {
        throw new ServletException(
            "Error writing execution state as JSON to servlet output stream", ex);
      }
      response.setStatus(500);
    }
  }

  private void outputIdAsJson(PrintWriter out, String parentId) throws HopException {
    try {
      HopJson.newMapper().writeValue(out, parentId);
    } catch (IOException | DataBindingException e) {
      throw new HopException("Error writing execution ID as JSON to servlet output stream", e);
    }
  }

  private void outputSuccessAsJson(PrintWriter out, boolean success) throws HopException {
    try {
      HopJson.newMapper().writeValue(out, success);
    } catch (IOException | DataBindingException e) {
      throw new HopException(
          "Error writing success boolean value as JSON to servlet output stream", e);
    }
  }

  private void outputExecutionDataAsJson(PrintWriter out, ExecutionData data) throws HopException {
    try {
      HopJson.newMapper().writeValue(out, data);
    } catch (IOException | DataBindingException e) {
      throw new HopException("Error writing execution data as JSON to servlet output stream", e);
    }
  }

  private void outputExecutionChildrenAsJson(PrintWriter out, List<Execution> children)
      throws HopException {
    try {
      HopJson.newMapper().writeValue(out, children);
    } catch (IOException | DataBindingException e) {
      throw new HopException(
          "Error writing execution children as JSON to servlet output stream", e);
    }
  }

  private void outputExecutionAsJson(PrintWriter out, Execution execution) throws HopException {
    try {
      HopJson.newMapper().writeValue(out, execution);
    } catch (IOException | DataBindingException e) {
      throw new HopException("Error writing execution as JSON to servlet output stream", e);
    }
  }

  private void outputExecutionIdsAsJson(PrintWriter out, List<String> ids) throws HopException {
    try {
      HopJson.newMapper().writeValue(out, ids);
    } catch (IOException | DataBindingException e) {
      throw new HopException("Error writing execution IDs as JSON to servlet output stream", e);
    }
  }

  private void outputExecutionStateAsJson(PrintWriter out, ExecutionState executionState)
      throws HopException {
    try {
      HopJson.newMapper().writeValue(out, executionState);
    } catch (IOException | DataBindingException e) {
      throw new HopException("Error writing execution state as JSON to servlet output stream", e);
    }
  }

  public String toString() {
    return "Workflow Status IHandler";
  }

  @Override
  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
