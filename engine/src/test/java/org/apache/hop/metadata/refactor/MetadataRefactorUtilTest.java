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

package org.apache.hop.metadata.refactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.MetadataRefactorUtil;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MetadataRefactorUtilTest {

  private static MemoryMetadataProvider provider;

  @BeforeAll
  static void setup() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        DatabaseMeta.class.getName(), MetadataPluginType.class, HopMetadata.class);
    registry.registerPluginClass(
        PipelineRunConfiguration.class.getName(), MetadataPluginType.class, HopMetadata.class);
    registry.registerPluginClass(
        WorkflowRunConfiguration.class.getName(), MetadataPluginType.class, HopMetadata.class);
    registry.registerPluginClass(
        HopServerMeta.class.getName(), MetadataPluginType.class, HopMetadata.class);

    provider = new MemoryMetadataProvider();
  }

  @Test
  void testGetPropertyTypeForRdbmsConnection() throws Exception {
    HopMetadataPropertyType type =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(provider, "rdbms");
    assertEquals(HopMetadataPropertyType.RDBMS_CONNECTION, type);
  }

  @Test
  void testGetPropertyTypeForPipelineRunConfiguration() throws Exception {
    HopMetadataPropertyType type =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(provider, "pipeline-run-configuration");
    assertEquals(HopMetadataPropertyType.PIPELINE_RUN_CONFIG, type);
  }

  @Test
  void testGetPropertyTypeForWorkflowRunConfiguration() throws Exception {
    HopMetadataPropertyType type =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(provider, "workflow-run-configuration");
    assertEquals(HopMetadataPropertyType.WORKFLOW_RUN_CONFIG, type);
  }

  @Test
  void testGetPropertyTypeForUnknownKeyReturnsNone() {
    HopMetadataPropertyType type =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(provider, "non-existent-key");
    assertEquals(HopMetadataPropertyType.NONE, type);
  }

  @Test
  void testSupportsGlobalReplaceForRdbmsConnection() throws Exception {
    assertTrue(MetadataRefactorUtil.supportsGlobalReplace(provider, "rdbms"));
  }

  @Test
  void testSupportsGlobalReplaceForPipelineRunConfiguration() throws Exception {
    assertTrue(MetadataRefactorUtil.supportsGlobalReplace(provider, "pipeline-run-configuration"));
  }

  @Test
  void testSupportsGlobalReplaceForUnknownKeyReturnsFalse() {
    assertFalse(MetadataRefactorUtil.supportsGlobalReplace(provider, "non-existent-key"));
  }

  @Test
  void testGetPropertyTypeForHopServer() throws Exception {
    HopMetadataPropertyType type =
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(provider, "server");
    assertEquals(HopMetadataPropertyType.SERVER_DEFINITION, type);
  }
}
