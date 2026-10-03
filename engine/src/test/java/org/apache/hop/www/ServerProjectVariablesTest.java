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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A client sends all of its variables along with a pipeline or workflow it runs on a server. The
 * variables that say where the project lives and which project and environment are active describe
 * the machine they were set on, so the server keeps its own when it has a project enabled.
 * Otherwise a sub-pipeline at {@code ${PROJECT_HOME}/...} resolves to a folder on the client. An
 * exported run is the exception: the export rewrote its file references relative to the client's
 * project, so it keeps the client's values. See issue #8597.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ServerProjectVariablesTest {

  private static final String RUN_CONFIGURATION_NAME = "local";
  private static final String PIPELINE_NAME = "a-pipeline";

  /** What a Windows client with project "client-project" in environment "dev" sends along. */
  private static final Map<String, String> CLIENT_VARIABLES =
      Map.of(
          "PROJECT_HOME", "C:\\Users\\someone\\client-project",
          "PARENT_PROJECT_HOME", "C:\\Users\\someone\\parent-project",
          "PARENT_PROJECT_NAME", "client-parent",
          "HOP_PROJECT_NAME", "client-project",
          "HOP_ENVIRONMENT_NAME", "dev",
          "MY_VARIABLE", "from-the-client");

  @Test
  void pipelineKeepsTheProjectOfTheServer() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = registerAndPreparePipeline(serverWithProject(), false);

    assertServerProject(pipeline);
  }

  @Test
  void workflowKeepsTheProjectOfTheServer() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = registerWorkflow(serverWithProject(), false);

    assertServerProject(workflow);
  }

  /**
   * A server without a project has no values of its own to keep. The client's values still arrive,
   * so a client and server sharing the same folders keep working.
   */
  @Test
  void pipelineOnAServerWithoutProjectGetsTheProjectOfTheClient() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = registerAndPreparePipeline(new Variables(), false);

    assertClientProject(pipeline);
  }

  @Test
  void workflowOnAServerWithoutProjectGetsTheProjectOfTheClient() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = registerWorkflow(new Variables(), false);

    assertClientProject(workflow);
  }

  /**
   * A server started without a project or environment still runs the default project of its
   * configuration. Nobody chose that project, so the client's values stay, as they do on a server
   * without a project.
   */
  @Test
  void pipelineOnAServerWithTheDefaultProjectGetsTheProjectOfTheClient() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline =
        registerAndPreparePipeline(serverWithDefaultProject(), false);

    assertClientProject(pipeline);
  }

  @Test
  void workflowOnAServerWithTheDefaultProjectGetsTheProjectOfTheClient() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = registerWorkflow(serverWithDefaultProject(), false);

    assertClientProject(workflow);
  }

  /**
   * The export rewrote the file references of the pipeline relative to the project of the client,
   * so ${PROJECT_HOME} has to keep pointing there. Otherwise references the export left alone and
   * rewritten ones end up in two different folders.
   */
  @Test
  void exportedPipelineKeepsTheProjectOfTheClient() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = registerAndPreparePipeline(serverWithProject(), true);

    assertClientProject(pipeline);
  }

  @Test
  void exportedWorkflowKeepsTheProjectOfTheClient() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = registerWorkflow(serverWithProject(), true);

    assertClientProject(workflow);
  }

  /**
   * The server runs project "server-project" without a parent project or an environment. The
   * client's parent project and environment belong to the client's project, not this one.
   */
  private static IVariables serverWithProject() {
    IVariables serverVariables = new Variables();
    serverVariables.setVariable("PROJECT_HOME", "/opt/hop/server-project");
    serverVariables.setVariable("HOP_PROJECT_NAME", "server-project");
    serverVariables.setVariable("PARENT_PROJECT_HOME", "");
    serverVariables.setVariable("PARENT_PROJECT_NAME", "");
    return serverVariables;
  }

  /** What hop-server runs with when it is started without a project or environment. */
  private static IVariables serverWithDefaultProject() {
    IVariables serverVariables = new Variables();
    serverVariables.setVariable("PROJECT_HOME", "config/projects/default");
    serverVariables.setVariable("HOP_PROJECT_NAME", "default");
    serverVariables.setVariable("HOP_PROJECT_IS_DEFAULT", "Y");
    return serverVariables;
  }

  private static void assertServerProject(IVariables variables) {
    assertEquals("/opt/hop/server-project", variables.getVariable("PROJECT_HOME"));
    assertEquals("server-project", variables.getVariable("HOP_PROJECT_NAME"));
    assertEquals("", variables.getVariable("PARENT_PROJECT_HOME"));
    assertEquals("", variables.getVariable("PARENT_PROJECT_NAME"));
    assertNull(variables.getVariable("HOP_ENVIRONMENT_NAME"));
    assertEquals(
        "from-the-client",
        variables.getVariable("MY_VARIABLE"),
        "Other variables of the client should still arrive");
  }

  private static void assertClientProject(IVariables variables) {
    CLIENT_VARIABLES.forEach(
        (name, value) -> assertEquals(value, variables.getVariable(name), name));
  }

  /**
   * @param exported true to register the pipeline the way hop-server does when it unpacks an export
   *     archive
   */
  private static IPipelineEngine<PipelineMeta> registerAndPreparePipeline(
      IVariables serverVariables, boolean exported) throws Exception {
    HopServerConfig serverConfig = serverConfig(serverVariables);
    PipelineMap pipelineMap = new PipelineMap();
    pipelineMap.setHopServerConfig(serverConfig);
    WorkflowMap workflowMap = new WorkflowMap();
    workflowMap.setHopServerConfig(serverConfig);

    RegisterPipelineServlet registerServlet = new RegisterPipelineServlet();
    registerServlet.setup(pipelineMap, workflowMap);
    PipelineConfiguration pipelineConfiguration = pipelineConfiguration();
    pipelineConfiguration.setExported(exported);
    IPipelineEngine<PipelineMeta> pipeline = registerServlet.createPipeline(pipelineConfiguration);

    // The client asks the server to prepare the pipeline next. That is where the variables of the
    // execution configuration are applied.
    //
    PrepareExecutionPipelineServlet prepareServlet = new PrepareExecutionPipelineServlet();
    prepareServlet.setup(pipelineMap, workflowMap);
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter("name")).thenReturn(PIPELINE_NAME);
    when(request.getParameter("id")).thenReturn(pipeline.getContainerId());
    when(request.getParameter("xml")).thenReturn("Y");
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(out));

    prepareServlet.doGet(request, response);

    return pipeline;
  }

  private static IWorkflowEngine<WorkflowMeta> registerWorkflow(
      IVariables serverVariables, boolean exported) throws Exception {
    HopServerConfig serverConfig = serverConfig(serverVariables);
    PipelineMap pipelineMap = new PipelineMap();
    pipelineMap.setHopServerConfig(serverConfig);
    WorkflowMap workflowMap = new WorkflowMap();
    workflowMap.setHopServerConfig(serverConfig);

    RegisterWorkflowServlet servlet = new RegisterWorkflowServlet();
    servlet.setup(pipelineMap, workflowMap);
    WorkflowConfiguration workflowConfiguration = workflowConfiguration();
    workflowConfiguration.setExported(exported);
    return servlet.createWorkflow(workflowConfiguration);
  }

  private static HopServerConfig serverConfig(IVariables serverVariables) {
    HopServerConfig serverConfig = new HopServerConfig();
    serverConfig.setMetadataProvider(new MultiMetadataProvider(new Variables()));
    serverConfig.setVariables(serverVariables);
    return serverConfig;
  }

  private static PipelineConfiguration pipelineConfiguration() throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(PIPELINE_NAME);

    PipelineExecutionConfiguration executionConfiguration = new PipelineExecutionConfiguration();
    executionConfiguration.setRunConfiguration(RUN_CONFIGURATION_NAME);
    executionConfiguration.getVariablesMap().putAll(CLIENT_VARIABLES);

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    LocalPipelineRunConfiguration engineConfiguration = new LocalPipelineRunConfiguration();
    engineConfiguration.setEnginePluginId("Local");
    metadataProvider
        .getSerializer(PipelineRunConfiguration.class)
        .save(
            new PipelineRunConfiguration(
                RUN_CONFIGURATION_NAME,
                "",
                null,
                new ArrayList<>(),
                engineConfiguration,
                null,
                false));

    return new PipelineConfiguration(
        pipelineMeta, executionConfiguration, new SerializableMetadataProvider(metadataProvider));
  }

  private static WorkflowConfiguration workflowConfiguration() throws HopException {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("a-workflow");

    WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
    executionConfiguration.setRunConfiguration(RUN_CONFIGURATION_NAME);
    executionConfiguration.getVariablesMap().putAll(CLIENT_VARIABLES);

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    LocalWorkflowRunConfiguration engineConfiguration = new LocalWorkflowRunConfiguration();
    engineConfiguration.setEnginePluginId("Local");
    metadataProvider
        .getSerializer(WorkflowRunConfiguration.class)
        .save(
            new WorkflowRunConfiguration(
                RUN_CONFIGURATION_NAME, "", null, engineConfiguration, false));

    return new WorkflowConfiguration(workflowMeta, executionConfiguration, metadataProvider);
  }
}
