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

package org.apache.hop.www.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

@HopServerServlet(
    id = "asyncStatus",
    name = "Get the status of an asynchronously executing workflow")
public class AsyncStatusServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final Class<?> PKG = WebServiceServlet.class; // For Translator

  private static final long serialVersionUID = 2943295824369134751L;

  public static final String CONTEXT_PATH = "/hop/asyncStatus";

  public AsyncStatusServlet() {}

  public AsyncStatusServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "AsyncStatusServlet.Log.AsyncStatusRequested"));
    }

    IVariables variables = pipelineMap.getHopServerConfig().getVariables();

    MultiMetadataProvider metadataProvider =
        new MultiMetadataProvider(Encr.getEncoder(), new ArrayList<>(), variables);
    metadataProvider.getProviders().add(HopMetadataUtil.getStandardHopMetadataProvider(variables));

    String metadataFolder = pipelineMap.getHopServerConfig().getMetadataFolder();
    if (StringUtils.isNotEmpty(metadataFolder)) {
      // Get the metadata from the specified metadata folder...
      //
      metadataProvider
          .getProviders()
          .add(new JsonMetadataProvider(Encr.getEncoder(), metadataFolder, variables));
    }

    String webServiceName = request.getParameter("service");
    if (StringUtils.isEmpty(webServiceName)) {
      throw new ServletException(
          "Please specify a service parameter pointing to the name of the asynchronous webservice object");
    }

    String serverObjectId = request.getParameter("id");
    if (StringUtils.isEmpty(serverObjectId)) {
      throw new ServletException(
          "Please specify an id parameter pointing to the unique ID of the asynchronous webservice object");
    }

    try {
      // Load the web service metadata
      //
      IHopMetadataSerializer<AsyncWebService> serializer =
          metadataProvider.getSerializer(AsyncWebService.class);
      AsyncWebService webService = serializer.load(webServiceName);
      if (webService == null) {
        throw new HopException(
            "Unable to find asynchronous web service '"
                + webServiceName
                + "'.  You can set option metadata_folder in the Hop server XML configuration");
      }

      // Get the workflow...
      //
      IWorkflowEngine<WorkflowMeta> workflow =
          workflowMap.findWorkflow(webServiceName, serverObjectId);

      // Report back in JSON format
      //
      AsyncStatus status = new AsyncStatus();

      status.setService(webServiceName);
      status.setId(serverObjectId);
      status.setStartDate(workflow.getExecutionStartDate());
      status.setEndDate(workflow.getExecutionEndDate());
      status.setStatusDescription(workflow.getStatusDescription());

      // Grab the status variables
      //
      for (String statusVariable : webService.getStatusVariablesList(variables)) {
        String statusValue = workflow.getVariable(statusVariable);
        status.getStatusVariables().put(statusVariable, statusValue);
      }

      // Add the pipeline statuses found in the parent workflow...
      //
      for (Object dataValue : workflow.getExtensionDataMap().values()) {
        if (dataValue instanceof HopServerPipelineStatus) {
          status.getPipelineStatuses().add((HopServerPipelineStatus) dataValue);
        }
      }

      // We give back all this information about the executing workflow in JSON format...
      //
      response.setContentType("application/json");
      response.setCharacterEncoding(Const.XML_ENCODING);
      final OutputStream outputStream = response.getOutputStream();

      ObjectMapper mapper = new ObjectMapper();
      String jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(status);

      byte[] data = jsonString.getBytes(StandardCharsets.UTF_8);
      response.setContentLength(data.length);
      outputStream.write(data);
      outputStream.flush();

      response.setStatus(HttpServletResponse.SC_OK);

    } catch (Exception e) {
      throw new ServletException("Error getting asynchronous web service status", e);
    }
  }

  public String toString() {
    return "Asynchronous Web Service Status Servlet";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
