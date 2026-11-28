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

package org.apache.hop.airflow.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.airflow.launcher.HopDagSettings;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.refactor.MetadataObjectReference;
import org.apache.hop.metadata.refactor.MetadataReferenceFinder;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Moving a workflow in Hop GUI takes its Airflow DAG objects along. */
class AirflowDagDefinitionRenameTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance().registerType(MetadataPluginType.getInstance());
  }

  @Test
  void followsTheWorkflowWhenItIsMoved() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    IHopMetadataSerializer<AirflowDagDefinition> serializer =
        provider.getSerializer(AirflowDagDefinition.class);
    AirflowDagDefinition daily = new AirflowDagDefinition("daily");
    // As the file dialog of Hop GUI puts it in a project
    daily.setWorkflow("${PROJECT_HOME}/flows/load.hwf");
    serializer.save(daily);
    AirflowDagDefinition other = new AirflowDagDefinition("other");
    other.setWorkflow("${PROJECT_HOME}/flows/other.hwf");
    serializer.save(other);
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/opt/project");
    MetadataReferenceFinder finder = new MetadataReferenceFinder(provider);

    List<MetadataObjectReference> references =
        finder.findFilePathReferencesInMetadata("/opt/project/flows/load.hwf", variables);
    assertEquals(1, references.size());
    assertEquals("daily", references.get(0).getContainerObjectName());

    finder.replaceFilePathReferencesInMetadata(
        "/opt/project/flows/load.hwf", "/opt/project/flows/daily/load.hwf", references, variables);

    String moved = serializer.load("daily").getWorkflow();
    assertEquals("${PROJECT_HOME}/flows/daily/load.hwf", moved);
    // In the form the launcher on the workers takes
    assertEquals("flows/daily/load.hwf", HopDagSettings.projectRelative(moved));
    assertEquals("${PROJECT_HOME}/flows/other.hwf", serializer.load("other").getWorkflow());
  }
}
