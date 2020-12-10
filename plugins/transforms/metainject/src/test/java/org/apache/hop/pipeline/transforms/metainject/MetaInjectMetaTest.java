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

package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class MetaInjectMetaTest {

  private static final String SOURCE_TRANSFORM_NAME = "SOURCE_TRANSFORM_NAME";

  private static final String SOURCE_FIELD_NAME = "SOURCE_TRANSFORM_NAME";

  private static final String TARGET_TRANSFORM_NAME = "TARGET_TRANSFORM_NAME";

  private static final String TARGET_FIELD_NAME = "TARGET_TRANSFORM_NAME";

  private static final String TEST_FILE_NAME = "TEST_FILE_NAME";

  private static final String EXPORTED_FILE_NAME = TEST_FILE_NAME;

  private static MetaInjectMeta metaInjectMeta;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void SetUp() throws Exception {
    if (!HopClientEnvironment.isInitialized()) {
      HopClientEnvironment.init();
    }
    metaInjectMeta = new MetaInjectMeta();
  }

  @Before
  public void before() {
    metaInjectMeta = new MetaInjectMeta();
  }

  @Test
  public void getResourceDependencies() {
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);

    List<ResourceReference> actualResult =
        metaInjectMeta.getResourceDependencies(new Variables(), transformMeta);
    assertEquals(1, actualResult.size());
    ResourceReference reference = actualResult.iterator().next();
    assertEquals(0, reference.getEntries().size());
  }

  @Test
  public void getResourceDependencies_with_defined_fileName() {
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    metaInjectMeta.setFileName("FILE_NAME");
    // doReturn("FILE_NAME_WITH_SUBSTITUTIONS").when(pipelineMeta).environmentSubstitute("FILE_NAME");

    List<ResourceReference> actualResult =
        metaInjectMeta.getResourceDependencies(new Variables(), transformMeta);
    assertEquals(1, actualResult.size());
    ResourceReference reference = actualResult.iterator().next();
    assertEquals(1, reference.getEntries().size());
  }

  @Test
  public void exportResources() throws HopException {
    IVariables variables = mock(IVariables.class);
    IResourceNaming resourceNamingInterface = mock(IResourceNaming.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    MetaInjectMeta injectMetaSpy = spy(metaInjectMeta);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    Map<String, ResourceDefinition> definitions =
        Collections.emptyMap();
    doReturn(TEST_FILE_NAME)
        .when(pipelineMeta)
        .exportResources(variables, definitions, resourceNamingInterface, metadataProvider);
    doReturn(pipelineMeta).when(injectMetaSpy).loadPipelineMeta(metadataProvider, variables);

    String actualExportedFileName =
        injectMetaSpy.exportResources(
            variables, definitions, resourceNamingInterface, metadataProvider);
    assertEquals(TEST_FILE_NAME, actualExportedFileName);
    assertEquals(EXPORTED_FILE_NAME, injectMetaSpy.getFileName());
    verify(pipelineMeta)
        .exportResources(variables, definitions, resourceNamingInterface, metadataProvider);
  }

  @Test
  public void convertToMap() {
    MetaInjectMapping metaInjectMapping = new MetaInjectMapping();
    metaInjectMapping.setSourceTransform(SOURCE_TRANSFORM_NAME);
    metaInjectMapping.setSourceField(SOURCE_FIELD_NAME);
    metaInjectMapping.setTargetTransform(TARGET_TRANSFORM_NAME);
    metaInjectMapping.setTargetField(TARGET_FIELD_NAME);

    Map<TargetTransformAttribute, SourceTransformField> actualResult =
        MetaInjectMeta.convertToMap(Collections.singletonList(metaInjectMapping));

    assertEquals(1, actualResult.size());

    TargetTransformAttribute targetTransformAttribute = actualResult.keySet().iterator().next();
    assertEquals(TARGET_TRANSFORM_NAME, targetTransformAttribute.getTransformName());
    assertEquals(TARGET_FIELD_NAME, targetTransformAttribute.getAttributeKey());

    SourceTransformField sourceTransformField = actualResult.values().iterator().next();
    assertEquals(SOURCE_TRANSFORM_NAME, sourceTransformField.getTransformName());
    assertEquals(SOURCE_FIELD_NAME, sourceTransformField.getField());
  }
}
