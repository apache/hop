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

package org.apache.hop.www.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.www.HopServerConfig;
import org.apache.hop.www.PipelineMap;
import org.apache.hop.www.WorkflowMap;
import org.apache.hop.www.api.v1.resources.ExecutionResource;
import org.apache.hop.www.api.v1.resources.LocationResource;
import org.apache.hop.www.api.v1.resources.MetadataResource;
import org.apache.hop.www.api.v1.resources.PluginsResource;
import org.junit.jupiter.api.Test;

class HopApiApplicationTest {

  private static HopServerApiContext context() {
    PipelineMap pipelineMap = mock(PipelineMap.class);
    when(pipelineMap.getHopServerConfig()).thenReturn(new HopServerConfig());
    return new HopServerApiContext(pipelineMap, mock(WorkflowMap.class), mock(ILogChannel.class));
  }

  @Test
  void theContextPathIsTheOneTheDocsAndTheWarBothUse() {
    // Changing this breaks every client and the hop-web web.xml mapping; it must be deliberate.
    assertEquals("/hop/api/v1", HopApiApplication.CONTEXT_PATH);
  }

  @Test
  void everyResourceIsRegistered() {
    HopApiApplication application = new HopApiApplication(context());

    assertTrue(application.getClasses().contains(ExecutionResource.class));
    assertTrue(application.getClasses().contains(LocationResource.class));
    assertTrue(application.getClasses().contains(MetadataResource.class));
    assertTrue(application.getClasses().contains(PluginsResource.class));
  }

  @Test
  void theExceptionMapperIsRegisteredSoErrorsAreJson() {
    HopApiApplication application = new HopApiApplication(context());

    assertTrue(
        application.getInstances().stream().anyMatch(HopApiExceptionMapper.class::isInstance),
        "without the mapper the API would leak container HTML error pages");
  }

  @Test
  void theContextReadsTheProviderBackFromTheConfigEveryTime() {
    // A project or environment switch replaces the provider on the config; a context that captured
    // it once would keep serving the old metadata, which is the bug the old rest singleton had.
    HopServerConfig config = new HopServerConfig();
    PipelineMap pipelineMap = mock(PipelineMap.class);
    when(pipelineMap.getHopServerConfig()).thenReturn(config);
    HopServerApiContext context =
        new HopServerApiContext(pipelineMap, mock(WorkflowMap.class), mock(ILogChannel.class));

    MultiMetadataProvider first = mock(MultiMetadataProvider.class);
    config.setMetadataProvider(first);
    assertSame(first, context.getMetadataProvider());

    MultiMetadataProvider second = mock(MultiMetadataProvider.class);
    config.setMetadataProvider(second);
    assertSame(second, context.getMetadataProvider());
  }

  @Test
  void theContextReadsTheVariablesBackFromTheConfigEveryTime() {
    HopServerConfig config = new HopServerConfig();
    PipelineMap pipelineMap = mock(PipelineMap.class);
    when(pipelineMap.getHopServerConfig()).thenReturn(config);
    HopServerApiContext context =
        new HopServerApiContext(pipelineMap, mock(WorkflowMap.class), mock(ILogChannel.class));

    IVariables variables = new Variables();
    config.setVariables(variables);

    assertSame(variables, context.getVariables());
  }

  @Test
  void theContextExposesTheServerMaps() {
    PipelineMap pipelineMap = mock(PipelineMap.class);
    WorkflowMap workflowMap = mock(WorkflowMap.class);
    when(pipelineMap.getHopServerConfig()).thenReturn(new HopServerConfig());

    HopServerApiContext context =
        new HopServerApiContext(pipelineMap, workflowMap, mock(ILogChannel.class));

    assertSame(pipelineMap, context.getPipelineMap());
    assertSame(workflowMap, context.getWorkflowMap());
  }
}
