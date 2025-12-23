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
 */

package org.apache.hop.www;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;

@HopServerServlet(id = "execPipeline", name = "Execute pipeline from file path")
public class ExecPipelineServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final long serialVersionUID = -5879219287669847357L;

  private static final String UNABLE_TO_FIND_PIPELINE = "Unable to find pipeline";

  private static final String PIPELINE = "pipeline";
  private static final String LEVEL = "level";
  private static final String RUN_CONFIG = "runConfig";

  public static final String CONTEXT_PATH = "/hop/execPipeline";

  public ExecPipelineServlet() {}

  public ExecPipelineServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  /**
   * Executes a pipeline from the specified file path.
   *
   * <p>Example Request:<br>
   * GET /hop/execPipeline/?pipeline=/path/to/pipeline.hpl&level=INFO
   *
   * <h3>Parameters</h3>
   *
   * <table>
   * <tr><th>name</th><th>description</th><th>type</th></tr>
   * <tr><td>pipeline</td><td>File path to pipeline (.hpl file)</td><td>query (required)</td></tr>
   * <tr><td>level</td><td>Logging level (i.e. Debug, Basic, Detailed)</td><td>query (optional)</td></tr>
   * <tr><td>runConfig</td><td>Run configuration name (optional, defaults to LocalPipelineEngine)</td><td>query (optional)</td></tr>
   * <tr><td>*any name*</td><td>All other parameters will be set as variables/parameters in the pipeline</td><td>query</td></tr>
   * </table>
   *
   * <h3>Response</h3>
   *
   * Returns XML with webresult containing OK or ERROR status and message.
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug("Execute pipeline requested");
    }

    String[] knownOptions = new String[] {PIPELINE, LEVEL, RUN_CONFIG};

    String pipelineOption = request.getParameter(PIPELINE);
    String levelOption = request.getParameter(LEVEL);
    String runConfigOption = request.getParameter(RUN_CONFIG);

    response.setStatus(HttpServletResponse.SC_OK);

    String encoding = System.getProperty("HOP_DEFAULT_SERVLET_ENCODING", null);
    if (encoding != null && !Utils.isEmpty(encoding.trim())) {
      response.setCharacterEncoding(encoding);
      response.setContentType("text/html; charset=" + encoding);
    } else {
      response.setContentType("text/xml");
      response.setCharacterEncoding(Const.XML_ENCODING);
    }

    PrintWriter out = response.getWriter();

    if (pipelineOption == null) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      out.println(
          new WebResult(WebResult.STRING_ERROR, "Missing mandatory parameter: " + PIPELINE));
      return;
    }

    try {
      // Get metadata provider from server config
      IHopMetadataProvider metadataProvider = getServerConfig().getMetadataProvider();
      if (metadataProvider == null) {
        throw new HopException("Metadata provider is not available");
      }

      // Resolve variables in the pipeline path (e.g., ${PROJECT_HOME})
      String resolvedPipelinePath = variables.resolve(pipelineOption);

      // Load pipeline from file
      PipelineMeta pipelineMeta =
          new PipelineMeta(resolvedPipelinePath, metadataProvider, variables);

      // Set the servlet parameters as variables/parameters in the pipeline
      String[] parameters = pipelineMeta.listParameters();
      Enumeration<String> parameterNames = request.getParameterNames();
      while (parameterNames.hasMoreElements()) {
        String parameter = parameterNames.nextElement();
        String[] values = request.getParameterValues(parameter);

        // Ignore the known options, set the rest as variables/parameters
        if (Const.indexOfString(parameter, knownOptions) < 0) {
          // If it's a pipeline parameter, it will be set later via setParameterValue
          // Otherwise, set as variable
          if (Const.indexOfString(parameter, parameters) < 0) {
            variables.setVariable(parameter, values[0]);
          }
        }
      }

      // Create execution configuration
      PipelineExecutionConfiguration pipelineExecutionConfiguration =
          new PipelineExecutionConfiguration();

      // Set logging level
      LogLevel logLevel = null;
      if (!Utils.isEmpty(levelOption)) {
        logLevel = LogLevel.lookupCode(levelOption);
      }
      if (logLevel != null) {
        pipelineExecutionConfiguration.setLogLevel(logLevel);
      }

      // Determine run configuration
      String runConfigurationName;
      if (!StringUtils.isEmpty(runConfigOption)) {
        // Use specified run configuration
        runConfigurationName = runConfigOption;
      } else {
        // Try to find a default run configuration
        PipelineRunConfiguration defaultRunConfiguration =
            PipelineRunConfiguration.findDefault(metadataProvider);
        if (defaultRunConfiguration != null) {
          runConfigurationName = defaultRunConfiguration.getName();
        } else {
          // Fallback to "local" if no default is found
          runConfigurationName = "local";
        }
      }
      pipelineExecutionConfiguration.setRunConfiguration(runConfigurationName);

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
          new SimpleLoggingObject(CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null);
      servletLoggingObject.setContainerObjectId(serverObjectId);
      if (logLevel != null) {
        servletLoggingObject.setLogLevel(logLevel);
      }

      // Create the pipeline engine using the run configuration from execution configuration
      IPipelineEngine<PipelineMeta> pipeline =
          PipelineEngineFactory.createPipelineEngine(
              variables,
              variables.resolve(pipelineExecutionConfiguration.getRunConfiguration()),
              metadataProvider,
              pipelineMeta);
      pipeline.setParent(servletLoggingObject);
      pipeline.setMetadataProvider(metadataProvider);

      // Set parameters from request
      pipeline.copyParametersFromDefinitions(pipelineMeta);
      for (String parameter : parameters) {
        String value = request.getParameter(parameter);
        if (value != null) {
          pipeline.setParameterValue(parameter, value);
        }
      }
      pipeline.activateParameters(pipeline);

      // Create pipeline configuration for registration
      SerializableMetadataProvider serializableMetadataProvider =
          new SerializableMetadataProvider(metadataProvider);
      PipelineConfiguration pipelineConfiguration =
          new PipelineConfiguration(
              pipelineMeta, pipelineExecutionConfiguration, serializableMetadataProvider);

      // Register pipeline in the map
      getPipelineMap()
          .addPipeline(pipelineMeta.getName(), serverObjectId, pipeline, pipelineConfiguration);
      pipeline.setContainerId(serverObjectId);

      // Execute the pipeline synchronously
      executePipeline(pipeline);

      // Get logging output
      String logging =
          HopLogStore.getAppender().getBuffer(pipeline.getLogChannelId(), false).toString();

      // Check for errors
      if (pipeline.isFinished() && pipeline.getErrors() > 0) {
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        out.println(new WebResult(WebResult.STRING_ERROR, "Error executing pipeline: " + logging));
      } else {
        out.println(new WebResult(WebResult.STRING_OK, "Pipeline executed successfully"));
      }
      out.flush();

    } catch (Exception ex) {
      if (ex.getMessage() != null && ex.getMessage().contains(UNABLE_TO_FIND_PIPELINE)) {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        out.println(
            new WebResult(
                WebResult.STRING_ERROR,
                "Unable to find pipeline: "
                    + pipelineOption
                    + " (resolved: "
                    + (pipelineOption != null ? variables.resolve(pipelineOption) : "null")
                    + ")"));
      } else {
        String logging = Const.getStackTracker(ex);
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        out.println(
            new WebResult(
                WebResult.STRING_ERROR,
                "Unexpected error executing pipeline: " + Const.CR + logging));
      }
    }
  }

  protected void executePipeline(IPipelineEngine<PipelineMeta> pipeline) throws HopException {
    pipeline.prepareExecution();
    pipeline.startThreads();
    pipeline.waitUntilFinished();
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override
  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String toString() {
    return "Execute pipeline from file";
  }
}
