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

package org.apache.hop.www.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.www.PipelineMap;

/**
 * Resolves a {@link WebServiceRequest} against the {@link WebService} metadata and prepares the
 * pipeline behind it for execution.
 *
 * <p>This is the single implementation shared by the {@code /hop/webService} servlet and the JSON
 * API, so both honour the same web service options: the body and header content variables, the
 * status code field, binary output fields, and listing the run on the server status page.
 */
public class WebServiceExecutor {

  /**
   * Logging subject for a web service run. Kept as the servlet's context path even when the JSON
   * API is the caller, so log lines and execution information records stay greppable across the
   * change.
   */
  public static final String LOGGING_SUBJECT = "/hop/webService";

  public static final String PARAMETER_SERVICE = "service";
  public static final String PARAMETER_RUN_CONFIG = "runConfig";
  private static final String DEFAULT_CONTENT_TYPE = "text/plain";

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final PipelineMap pipelineMap;

  /**
   * @param variables the server's variables, used to resolve every web service option
   * @param metadataProvider the server's metadata provider, to load the web service from
   * @param pipelineMap the map to register the run in when the service lists its status, may be
   *     null
   */
  public WebServiceExecutor(
      IVariables variables, IHopMetadataProvider metadataProvider, PipelineMap pipelineMap) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.pipelineMap = pipelineMap;
  }

  /**
   * Load the requested web service, build and parameterise its pipeline and allocate the threads.
   *
   * @param request the resolved request
   * @return the prepared pipeline, ready to be executed against an output
   * @throws WebServiceException if the request is invalid, or the service is missing or disabled
   * @throws HopException if the pipeline could not be loaded or prepared
   */
  public PreparedWebService prepare(WebServiceRequest request) throws HopException {

    String webServiceName = request.getServiceName();
    if (StringUtils.isEmpty(webServiceName)) {
      throw new WebServiceException(
          WebServiceException.Reason.BAD_REQUEST,
          "Please specify a service parameter pointing to the name of the web service object");
    }

    IHopMetadataSerializer<WebService> serializer =
        metadataProvider.getSerializer(WebService.class);
    WebService webService = serializer.load(webServiceName);
    if (webService == null) {
      throw new WebServiceException(
          WebServiceException.Reason.NOT_FOUND,
          "Unable to find web service '"
              + webServiceName
              + "'.  You can set the metadata_folder in the Hop server XML configuration");
    }

    if (!webService.isEnabled()) {
      throw new WebServiceException(
          WebServiceException.Reason.DISABLED, "Web service '" + webServiceName + "' is disabled.");
    }

    // If a run configuration is set in the web service and none is specified here, we take it.
    //
    String runConfigurationName = request.getRunConfigurationName();
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

    if (StringUtils.isEmpty(contentType)) {
      contentType = DEFAULT_CONTENT_TYPE;
    }

    String serverObjectId = UUID.randomUUID().toString();
    SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject(LOGGING_SUBJECT, LoggingObjectType.HOP_SERVER, null);
    servletLoggingObject.setContainerObjectId(serverObjectId);

    // Load and prepare the pipeline
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

    // Only read the request body when the service actually asked for it.
    //
    if (StringUtils.isNotEmpty(bodyContentVariable)) {
      pipeline.setVariable(
          bodyContentVariable, Const.NVL(request.getBodyContentSupplier().get(), ""));
    }

    if (StringUtils.isNotEmpty(headerContentVariable)) {
      pipeline.setVariable(
          headerContentVariable, Const.NVL(headersAsJson(request.getHeaders()), ""));
    }

    // Set all the other parameters as variables/parameters...
    //
    String[] pipelineParameters = pipelineMeta.listParameters();
    pipeline.copyParametersFromDefinitions(pipelineMeta);
    for (Map.Entry<String, String> entry : request.getParameters().entrySet()) {
      String requestParameter = entry.getKey();
      if (PARAMETER_SERVICE.equals(requestParameter)) {
        continue;
      }
      String requestParameterValue = entry.getValue();
      if (Const.indexOfString(requestParameter, pipelineParameters) < 0) {
        pipeline.setVariable(requestParameter, Const.NVL(requestParameterValue, ""));
      } else {
        pipeline.setParameterValue(requestParameter, Const.NVL(requestParameterValue, ""));
      }
    }
    pipeline.activateParameters(pipeline);

    // See if we need to add this to the status map...
    //
    if (webService.isListingStatus() && pipelineMap != null) {
      PipelineExecutionConfiguration pipelineExecutionConfiguration =
          new PipelineExecutionConfiguration();
      PipelineConfiguration pipelineConfiguration =
          new PipelineConfiguration(
              pipelineMeta,
              pipelineExecutionConfiguration,
              new SerializableMetadataProvider(metadataProvider));
      pipelineMap.addPipeline(
          pipelineMeta.getName(), serverObjectId, pipeline, pipelineConfiguration);
    }

    // Allocate the threads...
    pipeline.prepareExecution();

    return new PreparedWebService(
        pipeline,
        serverObjectId,
        contentType,
        Const.UTF_8,
        transformName,
        fieldName,
        statusCodeField);
  }

  /** Build a JSON object holding every request header, for the header content variable. */
  private static String headersAsJson(Map<String, String> headers) throws HopException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      ObjectNode headersJson = objectMapper.createObjectNode();
      headers.forEach(headersJson::put);
      return objectMapper.writeValueAsString(headersJson);
    } catch (Exception e) {
      throw new HopException("Error serializing the request headers to JSON", e);
    }
  }
}
