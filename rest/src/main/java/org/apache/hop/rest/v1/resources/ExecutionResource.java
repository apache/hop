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

package org.apache.hop.rest.v1.resources;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.rest.Hop;
import org.apache.hop.rest.v1.resources.execute.SyncRequest;
import org.apache.hop.www.service.WebService;

/** The Synchronous and Asynchronous web services to execute a pipeline or a workflow. */
@Path("/execute")
public class ExecutionResource extends BaseResource {
  private final Hop hop = Hop.getInstance();

  /**
   * Execute a pipeline synchronously, by referencing the Web Service metadata object name.
   *
   * @param request The JSON containing the request details.
   * @return The output of the web service.
   */
  @POST
  @Path("/sync")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response executeSynchronously(SyncRequest request) {
    try {
      if (StringUtils.isEmpty(request.getService())) {
        throw new HopException(
            "Please specify the name of the service in the JSON you post: service");
      }

      MultiMetadataProvider metadataProvider = hop.getMetadataProvider();
      IHopMetadataSerializer<WebService> serializer =
          metadataProvider.getSerializer(WebService.class);
      WebService service = serializer.load(request.getService());
      if (service == null) {
        throw new HopException("Unable to find service with name '" + request.getService() + "'");
      }

      if (!service.isEnabled()) {
        throw new HopException("Web service '" + service.getName() + "' is disabled.");
      }

      IVariables variables = hop.getVariables();

      String runConfigurationName = request.getRunConfig();
      if (StringUtils.isEmpty(runConfigurationName)) {
        runConfigurationName = variables.resolve(service.getRunConfigurationName());
      }

      if (StringUtils.isEmpty(runConfigurationName)) {
        throw new HopException(
            "Please specify the name of the run configuration to use, "
                + "either in the request (runConfig) or in the Web Service metadata element '"
                + service.getName()
                + "'");
      }

      String filename = variables.resolve(service.getFilename());
      String transformName = variables.resolve(service.getTransformName());
      String fieldName = variables.resolve(service.getFieldName());
      String contentType = variables.resolve(service.getContentType());
      String bodyContentVariable = variables.resolve(service.getBodyContentVariable());

      String bodyContent = request.getBodyContent();

      if (StringUtils.isEmpty(contentType)) {
        contentType = "text/plain";
      }

      // Load and execute the pipeline.
      //
      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
          new SimpleLoggingObject("/service/sync/", LoggingObjectType.HOP_SERVER, null);
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

      // Set all the other parameters as variables/parameters...
      //
      String[] pipelineParameters = pipelineMeta.listParameters();
      pipeline.copyParametersFromDefinitions(pipelineMeta);
      for (String requestParameter : request.getVariables().keySet()) {
        if ("service".equals(requestParameter)) {
          continue;
        }
        String requestParameterValue = request.getVariables().get(requestParameter);
        if (Const.indexOfString(requestParameter, pipelineParameters) < 0) {
          pipeline.setVariable(requestParameter, Const.NVL(requestParameterValue, ""));
        } else {
          pipeline.setParameterValue(requestParameter, Const.NVL(requestParameterValue, ""));
        }
      }
      pipeline.activateParameters(pipeline);

      // Allocate the threads...
      pipeline.prepareExecution();

      // Add the row listener to the transform/field...
      //
      final StringBuilder output = new StringBuilder();
      IEngineComponent component = pipeline.findComponent(transformName, 0);
      component.addRowListener(
          new RowAdapter() {
            @Override
            public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                throws HopTransformException {
              try {
                output.append(rowMeta.getString(row, fieldName, ""));
              } catch (HopValueException e) {
                throw new HopTransformException(
                    "Error getting output field '"
                        + fieldName
                        + " from row: "
                        + rowMeta.toStringMeta(),
                    e);
              }
            }
          });

      pipeline.startThreads();
      pipeline.waitUntilFinished();

      // For now just give back the request as JSON
      //
      return Response.ok(output.toString()).type(contentType).encoding(Const.XML_ENCODING).build();
    } catch (Exception e) {
      String errorMessage =
          "Unexpected error executing synchronous web service (pipeline) with name "
              + request.getService();
      return getServerError(errorMessage, e);
    }
  }
}
