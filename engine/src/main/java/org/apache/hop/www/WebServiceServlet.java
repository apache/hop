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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.UUID;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.www.service.WebService;

@HopServerServlet(id = "webService", name = "Output the content of a field in a transform")
public class WebServiceServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final Class<?> PKG = WebServiceServlet.class;

  private static final long serialVersionUID = 3634806745373343432L;

  public static final String CONTEXT_PATH = "/hop/webService";

  public WebServiceServlet() {}

  public WebServiceServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    this.doGet(request, response);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "WebServiceServlet.Log.WebServiceRequested"));
    }

    IVariables variables = pipelineMap.getHopServerConfig().getVariables();

    IHopMetadataProvider metadataProvider = pipelineMap.getHopServerConfig().getMetadataProvider();

    String webServiceName = request.getParameter("service");
    if (StringUtils.isEmpty(webServiceName)) {
      throw new ServletException(
          "Please specify a service parameter pointing to the name of the web service object");
    }

    String runConfigurationName = request.getParameter("runConfig");

    try {
      IHopMetadataSerializer<WebService> serializer =
          metadataProvider.getSerializer(WebService.class);
      WebService webService = serializer.load(webServiceName);
      if (webService == null) {
        throw new HopException(
            "Unable to find web service '"
                + webServiceName
                + "'.  You can set the metadata_folder in the Hop server XML configuration");
      }

      if (!webService.isEnabled()) {
        throw new HopException("Web service '" + webServiceName + "' is disabled.");
      }

      // If a run configuration is set in the web service and none is specified here, we take it.
      //
      if (StringUtils.isEmpty(runConfigurationName)) {
        runConfigurationName = variables.resolve(webService.getRunConfigurationName());
      }

      String filename = variables.resolve(webService.getFilename());
      String transformName = variables.resolve(webService.getTransformName());
      String fieldName = variables.resolve(webService.getFieldName());
      String contentType = variables.resolve(webService.getContentType());
      String statusCodeField = variables.resolve(webService.getStatusCode());
      String bodyContentVariable = variables.resolve(webService.getBodyContentVariable());
      String headerContentVariable = variables.resolve(webService.getHeaderContentVariable());

      String bodyContent = "";
      if (StringUtils.isNotEmpty(bodyContentVariable)) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(request.getInputStream(), out);
        bodyContent = out.toString(StandardCharsets.UTF_8);
      }

      String headerContent = "";
      if (StringUtils.isNotEmpty(headerContentVariable)) {
        // Create JSON object containing all request headers
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode headersJson = objectMapper.createObjectNode();

        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
          String headerName = headerNames.nextElement();
          String headerValue = request.getHeader(headerName);
          headersJson.put(headerName, headerValue);
        }

        headerContent = objectMapper.writeValueAsString(headersJson);
      }

      if (StringUtils.isEmpty(contentType)) {
        response.setContentType("text/plain");
      } else {
        response.setContentType(contentType);
      }
      response.setCharacterEncoding(Const.XML_ENCODING);

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
          new SimpleLoggingObject(CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null);
      servletLoggingObject.setContainerObjectId(serverObjectId);

      // Load and start the pipeline
      // Output the data to the response output stream...
      //
      PipelineMeta pipelineMeta = new PipelineMeta(filename, metadataProvider, variables);
      IPipelineEngine<PipelineMeta> pipeline;
      if (StringUtils.isEmpty(runConfigurationName)) {
        pipeline = new LocalPipelineEngine(pipelineMeta, variables, servletLoggingObject);
      } else {
        pipeline =
            PipelineEngineFactory.createPipelineEngine(
                variables, runConfigurationName, metadataProvider, pipelineMeta);
      }
      pipeline.setContainerId(serverObjectId);

      if (StringUtils.isNotEmpty(bodyContentVariable)) {
        pipeline.setVariable(bodyContentVariable, Const.NVL(bodyContent, ""));
      }

      if (StringUtils.isNotEmpty(headerContentVariable)) {
        pipeline.setVariable(headerContentVariable, Const.NVL(headerContent, ""));
      }

      // Set all the other parameters as variables/parameters...
      //
      String[] pipelineParameters = pipelineMeta.listParameters();
      pipeline.copyParametersFromDefinitions(pipelineMeta);
      for (String requestParameter : request.getParameterMap().keySet()) {
        if ("service".equals(requestParameter)) {
          continue;
        }
        String requestParameterValue = request.getParameter(requestParameter);
        if (Const.indexOfString(requestParameter, pipelineParameters) < 0) {
          pipeline.setVariable(requestParameter, Const.NVL(requestParameterValue, ""));
        } else {
          pipeline.setParameterValue(requestParameter, Const.NVL(requestParameterValue, ""));
        }
      }
      pipeline.activateParameters(pipeline);

      // See if we need to add this to the status map...
      //
      if (webService.isListingStatus()) {
        PipelineExecutionConfiguration pipelineExecutionConfiguration =
            new PipelineExecutionConfiguration();
        PipelineConfiguration pipelineConfiguration =
            new PipelineConfiguration(
                pipelineMeta,
                pipelineExecutionConfiguration,
                new SerializableMetadataProvider(metadataProvider));
        getPipelineMap()
            .addPipeline(pipelineMeta.getName(), serverObjectId, pipeline, pipelineConfiguration);
      }

      // Allocate the threads...
      pipeline.prepareExecution();

      final OutputStream outputStream = response.getOutputStream();

      // Add the row listener to the transform/field...
      // TODO: add to all copies
      //
      IEngineComponent component = pipeline.findComponent(transformName, 0);
      component.addRowListener(
          new RowAdapter() {
            @Override
            public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                throws HopTransformException {
              try {
                String outputString = rowMeta.getString(row, fieldName, "");
                response.setStatus(rowMeta.getInteger(row, statusCodeField, 200L).intValue());
                outputStream.write(outputString.getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
              } catch (HopValueException e) {
                throw new HopTransformException(
                    "Error getting output field '"
                        + fieldName
                        + " from row: "
                        + rowMeta.toStringMeta(),
                    e);
              } catch (IOException e) {
                throw new HopTransformException("Error writing output of '" + fieldName + "'", e);
              }
            }
          });

      pipeline.startThreads();
      pipeline.waitUntilFinished();

    } catch (Exception e) {
      throw new ServletException("Error producing web service output", e);
    }
  }

  public String toString() {
    return "Web Service Servlet";
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
