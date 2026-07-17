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

package org.apache.hop.pipeline.engines.remote;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The run configuration a remote run configuration hands the pipeline to is used on the server.
 * When that leads back to a remote run configuration the pipeline keeps being handed on and
 * registered again and again. See issue #4086.
 */
class RemotePipelineEngineTest {

  private static final String SERVER_NAME = "a-server";

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  /** A remote run configuration that names itself never reaches a server that would run it. */
  @Test
  void runConfigurationThatRefersToItselfIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "remote"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "remote"));

    assertTrue(e.getMessage().contains("remote -> remote"), "The chain should be reported: " + e);
  }

  /**
   * The name of the run configuration to use on the server can hold a variable, which the check has
   * to resolve before it can see that it leads back to itself.
   */
  @Test
  void runConfigurationThatRefersToItselfThroughAVariableIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "${RUN_CONFIG}"));

    RemotePipelineEngine engine = engine(metadataProvider, "remote");
    engine.setVariable("RUN_CONFIG", "remote");

    HopException e = assertThrows(HopException.class, engine::prepareExecution);

    assertTrue(e.getMessage().contains("remote -> remote"), "The chain should be reported: " + e);
  }

  /** A chain of remote run configurations that leads back to an earlier one is rejected. */
  @Test
  void cyclicalRunConfigurationChainIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("first", "second"));
    save(metadataProvider, remote("second", "first"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "first"));

    assertTrue(
        e.getMessage().contains("first -> second -> first"), "The chain should be reported: " + e);
  }

  /**
   * A remote run configuration that hands the pipeline to a local one is what a remote run
   * configuration is meant to do, so it may not be rejected. It fails later on, when it looks for
   * the server that does not exist here.
   */
  @Test
  void runConfigurationThatLeadsToALocalOneIsAccepted() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "local"));
    save(metadataProvider, local("local"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "remote"));

    assertTrue(
        e.getMessage().contains(SERVER_NAME),
        "It should get as far as looking for the server: " + e);
  }

  /**
   * A run configuration that only exists on the server cannot be followed from here, which may not
   * be reported as a problem.
   */
  @Test
  void runConfigurationThatIsUnknownHereIsAccepted() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "only-known-on-the-server"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "remote"));

    assertTrue(
        e.getMessage().contains(SERVER_NAME),
        "It should get as far as looking for the server: " + e);
  }

  private static PipelineRunConfiguration remote(String name, String runConfigurationName) {
    RemotePipelineRunConfiguration engineConfiguration = new RemotePipelineRunConfiguration();
    engineConfiguration.setEnginePluginId("Remote");
    engineConfiguration.setHopServerName(SERVER_NAME);
    engineConfiguration.setRunConfigurationName(runConfigurationName);
    return new PipelineRunConfiguration(
        name, "", null, new ArrayList<>(), engineConfiguration, null, false);
  }

  private static PipelineRunConfiguration local(String name) {
    LocalPipelineRunConfiguration engineConfiguration = new LocalPipelineRunConfiguration();
    engineConfiguration.setEnginePluginId("Local");
    return new PipelineRunConfiguration(
        name, "", null, new ArrayList<>(), engineConfiguration, null, false);
  }

  private static void save(IHopMetadataProvider metadataProvider, PipelineRunConfiguration c)
      throws HopException {
    metadataProvider.getSerializer(PipelineRunConfiguration.class).save(c);
  }

  private static void prepare(IHopMetadataProvider metadataProvider, String name) throws Exception {
    engine(metadataProvider, name).prepareExecution();
  }

  private static RemotePipelineEngine engine(IHopMetadataProvider metadataProvider, String name)
      throws HopException {
    RemotePipelineEngine engine = new RemotePipelineEngine();
    engine.setLogLevel(LogLevel.BASIC);
    engine.setMetadataProvider(metadataProvider);
    engine.setPipelineRunConfiguration(
        metadataProvider.getSerializer(PipelineRunConfiguration.class).load(name));
    engine.setPipelineMeta(new PipelineMeta());
    return engine;
  }
}
