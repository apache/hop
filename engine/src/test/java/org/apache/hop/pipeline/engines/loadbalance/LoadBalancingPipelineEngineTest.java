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

package org.apache.hop.pipeline.engines.loadbalance;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class LoadBalancingPipelineEngineTest {

  @Test
  void runConfigurationThatRefersToItselfIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, loadBalancing("lb", "lb"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "lb"));

    assertTrue(e.getMessage().contains("lb -> lb"), "The chain should be reported: " + e);
  }

  @Test
  void runConfigurationThatLeadsToALocalOneIsAcceptedUntilServersAreMissing() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, loadBalancing("lb", "local"));
    save(metadataProvider, local("local"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "lb"));

    assertTrue(
        e.getMessage().contains("no Hop servers") || e.getMessage().contains("No eligible"),
        "It should fail on the empty server group, not the chain: " + e);
  }

  private static PipelineRunConfiguration loadBalancing(String name, String runConfigurationName) {
    LoadBalancingPipelineRunConfiguration engineConfiguration =
        new LoadBalancingPipelineRunConfiguration();
    engineConfiguration.setEnginePluginId("LoadBalancing");
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
    LoadBalancingPipelineEngine engine = new LoadBalancingPipelineEngine();
    engine.setLogLevel(LogLevel.BASIC);
    engine.setMetadataProvider(metadataProvider);
    engine.setPipelineRunConfiguration(
        metadataProvider.getSerializer(PipelineRunConfiguration.class).load(name));
    engine.setPipelineMeta(new PipelineMeta());
    engine.prepareExecution();
  }
}
