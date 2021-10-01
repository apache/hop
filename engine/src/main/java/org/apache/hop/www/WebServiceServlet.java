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
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.www.service.WebService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

@HopServerServlet(id = "webService", name = "Output the content of a field in a transform")
public class WebServiceServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final Class<?> PKG = WebServiceServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745373343432L;

  public static final String CONTEXT_PATH = "/hop/webService";

  public WebServiceServlet() {}

  public WebServiceServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
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

      String filename = variables.resolve(webService.getFilename());
      String transformName = variables.resolve(webService.getTransformName());
      String fieldName = variables.resolve(webService.getFieldName());
      String contentType = variables.resolve(webService.getContentType());

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
      PipelineMeta pipelineMeta = new PipelineMeta(filename, metadataProvider, true, variables);
      LocalPipelineEngine pipeline =
          new LocalPipelineEngine(pipelineMeta, variables, servletLoggingObject);
      pipeline.setContainerId(serverObjectId);

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

      response.setStatus(HttpServletResponse.SC_OK);

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
