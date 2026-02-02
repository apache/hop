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
import java.io.Serial;
import java.util.Enumeration;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;

@HopServerServlet(id = "execWorkflow", name = "Execute workflow from file path")
public class ExecWorkflowServlet extends BaseHttpServlet implements IHopServerPlugin {
  @Serial private static final long serialVersionUID = -5879219287669847357L;

  private static final String UNABLE_TO_FIND_WORKFLOW = "Unable to find workflow";

  private static final String WORKFLOW = "workflow";
  private static final String LEVEL = "level";
  private static final String RUN_CONFIG = "runConfig";

  public static final String CONTEXT_PATH = "/hop/execWorkflow";

  public ExecWorkflowServlet() {}

  public ExecWorkflowServlet(WorkflowMap workflowMap) {
    super(workflowMap);
  }

  /**
   * Executes a workflow from the specified file path.
   *
   * <p>Example Request:<br>
   * GET /hop/execWorkflow/?workflow=/path/to/workflow.hwf&#38;level=INFO
   *
   * <table>
   * <caption>Parameters</caption>
   * <tr><th>name</th><th>description</th><th>type</th></tr>
   * <tr><td>workflow</td><td>File path to workflow (.hwf file)</td><td>query (required)</td></tr>
   * <tr><td>level</td><td>Logging level (i.e. Debug, Basic, Detailed)</td><td>query (optional)</td></tr>
   * <tr><td>runConfig</td><td>Run configuration name (optional, defaults to default workflow run config)</td><td>query (optional)</td></tr>
   * <tr><td>*any name*</td><td>All other parameters will be set as variables/parameters in the workflow</td><td>query</td></tr>
   * </table>
   *
   * <h4>Response</h4>
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
      logDebug("Execute workflow requested");
    }

    String[] knownOptions = new String[] {WORKFLOW, LEVEL, RUN_CONFIG};

    String workflowOption = request.getParameter(WORKFLOW);
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

    if (workflowOption == null) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      out.println(
          new WebResult(WebResult.STRING_ERROR, "Missing mandatory parameter: " + WORKFLOW));
      return;
    }

    try {
      // Get metadata provider from server config
      IHopMetadataProvider metadataProvider = getServerConfig().getMetadataProvider();
      if (metadataProvider == null) {
        throw new HopException("Metadata provider is not available");
      }

      // Resolve variables in the workflow path (e.g., ${PROJECT_HOME})
      String resolvedWorkflowPath = variables.resolve(workflowOption);

      // Load workflow from file
      WorkflowMeta workflowMeta =
          new WorkflowMeta(variables, resolvedWorkflowPath, metadataProvider);

      // Set the servlet parameters as variables/parameters in the workflow
      String[] parameters = workflowMeta.listParameters();
      Enumeration<String> parameterNames = request.getParameterNames();
      while (parameterNames.hasMoreElements()) {
        String parameter = parameterNames.nextElement();
        String[] values = request.getParameterValues(parameter);

        // Ignore the known options, set the rest as variables/parameters
        if (Const.indexOfString(parameter, knownOptions) < 0) {
          // If it's a workflow parameter, it will be set later via setParameterValue
          // Otherwise, set as variable
          if (Const.indexOfString(parameter, parameters) < 0) {
            variables.setVariable(parameter, values[0]);
          }
        }
      }

      // Create execution configuration
      WorkflowExecutionConfiguration workflowExecutionConfiguration =
          new WorkflowExecutionConfiguration();

      // Set logging level
      LogLevel logLevel = null;
      if (!Utils.isEmpty(levelOption)) {
        logLevel = LogLevel.lookupCode(levelOption);
      }
      if (logLevel != null) {
        workflowExecutionConfiguration.setLogLevel(logLevel);
      }

      // Determine run configuration
      String runConfigurationName;
      if (!StringUtils.isEmpty(runConfigOption)) {
        // Use specified run configuration
        runConfigurationName = runConfigOption;
      } else {
        // Try to find a default run configuration
        WorkflowRunConfiguration defaultRunConfiguration =
            WorkflowRunConfiguration.findDefault(metadataProvider);
        if (defaultRunConfiguration != null) {
          runConfigurationName = defaultRunConfiguration.getName();
        } else {
          // Fallback to "local" if no default is found
          runConfigurationName = "local";
        }
      }
      workflowExecutionConfiguration.setRunConfiguration(runConfigurationName);

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
          new SimpleLoggingObject(CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null);
      servletLoggingObject.setContainerObjectId(serverObjectId);
      if (logLevel != null) {
        servletLoggingObject.setLogLevel(logLevel);
      }

      // Create the workflow engine using the run configuration from execution configuration
      IWorkflowEngine<WorkflowMeta> workflow =
          WorkflowEngineFactory.createWorkflowEngine(
              variables,
              variables.resolve(workflowExecutionConfiguration.getRunConfiguration()),
              metadataProvider,
              workflowMeta,
              servletLoggingObject);
      workflow.setMetadataProvider(metadataProvider);

      // Set parameters from request
      workflow.copyParametersFromDefinitions(workflowMeta);
      for (String parameter : parameters) {
        String value = request.getParameter(parameter);
        if (value != null) {
          workflow.setParameterValue(parameter, value);
        }
      }
      workflow.activateParameters(workflow);

      // Create workflow configuration for registration
      WorkflowConfiguration workflowConfiguration =
          new WorkflowConfiguration(workflowMeta, workflowExecutionConfiguration, metadataProvider);

      // Register workflow in the map
      getWorkflowMap()
          .addWorkflow(workflowMeta.getName(), serverObjectId, workflow, workflowConfiguration);
      workflow.setContainerId(serverObjectId);

      // Execute the workflow synchronously
      executeWorkflow(workflow);

      // Get logging output
      String logging =
          HopLogStore.getAppender().getBuffer(workflow.getLogChannelId(), false).toString();

      // Check for errors
      Result result = workflow.getResult();
      if (workflow.isFinished() && (result == null || result.getNrErrors() > 0)) {
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        out.println(new WebResult(WebResult.STRING_ERROR, "Error executing workflow: " + logging));
      } else {
        out.println(new WebResult(WebResult.STRING_OK, "Workflow executed successfully"));
      }
      out.flush();

    } catch (Exception ex) {
      if (ex.getMessage() != null && ex.getMessage().contains(UNABLE_TO_FIND_WORKFLOW)) {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        out.println(
            new WebResult(
                WebResult.STRING_ERROR,
                "Unable to find workflow: "
                    + workflowOption
                    + " (resolved: "
                    + (workflowOption != null ? variables.resolve(workflowOption) : "null")
                    + ")"));
      } else {
        String logging = Const.getStackTracker(ex);
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        out.println(
            new WebResult(
                WebResult.STRING_ERROR,
                "Unexpected error executing workflow: " + Const.CR + logging));
      }
    }
  }

  protected void executeWorkflow(IWorkflowEngine<WorkflowMeta> workflow) throws HopException {
    // startExecution() is synchronous and returns the Result when finished
    workflow.startExecution();
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
    return "Execute workflow from file";
  }
}
