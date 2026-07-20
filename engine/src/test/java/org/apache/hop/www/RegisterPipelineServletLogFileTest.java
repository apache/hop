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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Registering a pipeline with a log file used to fail with a NullPointerException, because the
 * logging object of the servlet had no log channel to hang the log file writer on. See issue #4677.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class RegisterPipelineServletLogFileTest {

  private static final String RUN_CONFIGURATION_NAME = "local";

  private Path logFile;

  @BeforeEach
  void setUp() throws Exception {
    logFile = Files.createTempFile("register-pipeline-servlet", ".log");
    Files.delete(logFile);
  }

  @AfterEach
  void tearDown() throws Exception {
    Files.deleteIfExists(logFile);
  }

  @Test
  void registeringAPipelineWithALogFileWorks() throws Exception {
    RegisterPipelineServlet servlet = createServlet();

    IPipelineEngine<PipelineMeta> pipeline =
        assertDoesNotThrow(() -> servlet.createPipeline(createPipelineConfiguration()));

    assertNotNull(
        pipeline.getParent().getLogChannelId(),
        "The pipeline should log to a channel the log file writer can be found by");
  }

  /**
   * Everything the pipeline logs has the logging object of the servlet as its parent, so the log
   * file writer registered on it has to be found back from the log channel of the pipeline. Without
   * that the log file stays empty.
   */
  @Test
  void theLogFileWriterIsFoundFromTheLogChannelOfThePipeline() throws Exception {
    RegisterPipelineServlet servlet = createServlet();

    IPipelineEngine<PipelineMeta> pipeline = servlet.createPipeline(createPipelineConfiguration());

    // This is what the server does next: it is only then that the pipeline creates the log channel
    // it really runs with.
    pipeline.prepareExecution();

    assertNotNull(
        LoggingRegistry.getInstance()
            .getLogChannelFileWriterBuffer(pipeline.getLogChannel().getLogChannelId()),
        "The pipeline should write its log to the log file that was asked for");
  }

  /** Registering a pipeline without a log file keeps working. */
  @Test
  void registeringAPipelineWithoutALogFileWorks() throws Exception {
    RegisterPipelineServlet servlet = createServlet();
    PipelineConfiguration pipelineConfiguration = createPipelineConfiguration();
    pipelineConfiguration.getPipelineExecutionConfiguration().setSetLogfile(false);

    IPipelineEngine<PipelineMeta> pipeline =
        assertDoesNotThrow(() -> servlet.createPipeline(pipelineConfiguration));

    assertNotNull(pipeline);
  }

  private RegisterPipelineServlet createServlet() {
    HopServerConfig serverConfig = new HopServerConfig();
    serverConfig.setMetadataProvider(new MultiMetadataProvider(new Variables()));

    PipelineMap pipelineMap = new PipelineMap();
    pipelineMap.setHopServerConfig(serverConfig);

    RegisterPipelineServlet servlet = new RegisterPipelineServlet();
    servlet.setup(pipelineMap, new WorkflowMap());
    return servlet;
  }

  private PipelineConfiguration createPipelineConfiguration() throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("a-pipeline");

    PipelineExecutionConfiguration executionConfiguration = new PipelineExecutionConfiguration();
    executionConfiguration.setRunConfiguration(RUN_CONFIGURATION_NAME);
    executionConfiguration.setSetLogfile(true);
    executionConfiguration.setLogFileName(logFile.toString());

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
}
