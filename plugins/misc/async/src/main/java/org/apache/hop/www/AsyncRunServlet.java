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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

@HopServerServlet(id = "asyncRun", name = "Asynchronously run a workflow")
public class AsyncRunServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final Class<?> PKG = WebServiceServlet.class; // For Translator

  private static final long serialVersionUID = 3834384735363246432L;

  public static final String CONTEXT_PATH = "/hop/asyncRun";

  public AsyncRunServlet() {}

  public AsyncRunServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {

    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "AsyncRunServlet.Log.AsyncRunRequested"));
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
      log.logError(
          "Please specify a service parameter pointing to the name of the asynchronous webservice object");
    }

    try {
      IHopMetadataSerializer<AsyncWebService> serializer =
          metadataProvider.getSerializer(AsyncWebService.class);
      AsyncWebService webService = serializer.load(webServiceName);
      if (webService == null) {
        throw new HopException(
            "Unable to find asynchronous web service '"
                + webServiceName
                + "'.  You can set option metadata_folder in the Hop server XML configuration");
      }

      if (!webService.isEnabled()) {
        throw new HopException("Asynchronous Web service '" + webServiceName + "' is disabled.");
      }

      String filename = variables.resolve(webService.getFilename());

      // We give back the ID of the executing workflow...
      //
      response.setContentType("application/json");
      response.setCharacterEncoding(Const.XML_ENCODING);

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
          new SimpleLoggingObject(CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null);
      servletLoggingObject.setContainerObjectId(serverObjectId);

      // Load and start the workflow
      // Output the ID to the response output stream...
      //
      WorkflowMeta workflowMeta = new WorkflowMeta(variables, filename, metadataProvider);

      LocalWorkflowEngine workflow = new LocalWorkflowEngine(workflowMeta, servletLoggingObject);
      workflow.setContainerId(serverObjectId);
      workflow.setMetadataProvider(metadataProvider);
      workflow.setLogLevel(LogLevel.BASIC);
      workflow.initializeFrom(variables);
      workflow.setVariable("SERVER_OBJECT_ID", serverObjectId);

      // See if we need to pass a variable with the content in it...
      //
      // Read the content posted?
      //
      String contentVariable = variables.resolve(webService.getBodyContentVariable());
      String content = "";
      if (StringUtils.isNotEmpty(contentVariable)) {
        try (InputStream in = request.getInputStream()) {
          try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = in.read(buffer)) != -1) {
              outputStream.write(buffer, 0, length);
            }
            outputStream.flush();

            // Now we have the content...
            //
            content = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
          }
        }
        workflow.setVariable(contentVariable, Const.NVL(content, ""));
      }

      // Set all the other parameters as variables/parameters...
      //
      String[] pipelineParameters = workflowMeta.listParameters();
      workflow.copyParametersFromDefinitions(workflowMeta);
      for (String requestParameter : request.getParameterMap().keySet()) {
        if ("service".equals(requestParameter)) {
          continue;
        }
        String requestParameterValue = request.getParameter(requestParameter);
        if (Const.indexOfString(requestParameter, pipelineParameters) < 0) {
          workflow.setVariable(requestParameter, Const.NVL(requestParameterValue, ""));
        } else {
          try {
            workflow.setParameterValue(requestParameter, Const.NVL(requestParameterValue, ""));
          } catch (UnknownParamException e) {
            log.logError("Error running asynchronous web service", e);
          }
        }
      }
      workflow.activateParameters(workflow);

      // Add the workflow to the status map, so we can retrieve statuses later on
      //

      WorkflowExecutionConfiguration workflowExecutionConfiguration =
          new WorkflowExecutionConfiguration();
      WorkflowConfiguration workflowConfiguration =
          new WorkflowConfiguration(
              workflowMeta,
              workflowExecutionConfiguration,
              new SerializableMetadataProvider(metadataProvider));

      // We use the service name to store the workflow under!
      // That way we don't have to look up the name of the workflow when retrieving the status.
      //
      getWorkflowMap().addWorkflow(webServiceName, serverObjectId, workflow, workflowConfiguration);

      // Allocate the workflow in the background...
      //
      new Thread(workflow::startExecution).start();

      try (OutputStream outputStream = response.getOutputStream()) {

        // Report the ID in a JSON block
        //
        JSONObject json = new JSONObject();
        json.put("name", workflowMeta.getName());
        json.put("id", serverObjectId);

        String jsonString = json.toJSONString();
        outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
        outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
      } catch (IOException e) {
        log.logError("Error running asynchronous web service", e);
      }
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (IOException | HopException e) {
      log.logError("Error running asynchronous web service", e);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      super.doPost(request, response);
    } catch (ServletException | IOException e) {
      log.logError("Error running asynchronous web service", e);
    }
  }

  public String toString() {
    return "Asynchronous Web Service Run Servlet";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
