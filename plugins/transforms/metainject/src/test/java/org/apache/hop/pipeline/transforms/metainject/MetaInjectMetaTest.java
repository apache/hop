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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.inject.HopMetadataInjector;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetaInjectMetaTest {

  private static final String TEST_FILE_NAME = "TEST_FILE_NAME";

  private static final String EXPORTED_FILE_NAME =
      "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + TEST_FILE_NAME;

  private static MetaInjectMeta metaInjectMeta;

  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(),
      ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(),
      ValueMetaNumber.class.getName(),
      ValueMetaJson.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
    metaInjectMeta = new MetaInjectMeta();
  }

  @Test
  void testRoundTrip() throws Exception {
    MetaInjectMeta meta =
        TransformSerializationTestUtil.testSerialization("/meta-inject.xml", MetaInjectMeta.class);
    assertNotNull(meta);

    assertEquals("${PROJECT_HOME}/0038-json-input-template.hpl", meta.getTemplateFileName());
    assertEquals("local", meta.getRunConfigurationName());
    assertEquals("${java.io.tmpdir}/json-input.hpl", meta.getTargetFile());
    assertTrue(meta.isCreateParentFolder());
    assertTrue(meta.isNoExecution());
    assertFalse(meta.isAllowEmptyStreamOnExecution());
    assertEquals("sourceTransform", meta.getSourceTransformName());
    assertEquals("targetTransform", meta.getStreamTargetTransformName());

    // source output Fields
    assertEquals(2, meta.getSourceOutputFields().size());
    MetaInjectOutputField f = meta.getSourceOutputFields().getFirst();
    assertEquals("f1", f.getName());
    assertEquals(IValueMeta.TYPE_STRING, f.getType());
    assertEquals(100, f.getLength());
    assertEquals(-1, f.getPrecision());
    f = meta.getSourceOutputFields().getLast();
    assertEquals("f2", f.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, f.getType());
    assertEquals(7, f.getLength());
    assertEquals(2, f.getPrecision());

    // Mappings
    assertEquals(38, meta.getMappings().size());
    // Let's test the first and the last only
    //
    MetaInjectMapping m = meta.getMappings().getFirst();
    assertEquals("JSON input", m.getTargetTransformName());
    assertEquals("IGNORE_EMPTY_FILE", m.getTargetAttributeKey());
    assertFalse(m.isTargetDetail());
    assertEquals("files/json-input.xml", m.getSourceTransformName());
    assertEquals("IsIgnoreEmptyFile", m.getSourceField());

    m = meta.getMappings().getLast();
    assertEquals("JSON input", m.getTargetTransformName());
    assertEquals("FILE_REQUIRED", m.getTargetAttributeKey());
    assertTrue(m.isTargetDetail());
    assertEquals("files/json-input.xml files", m.getSourceTransformName());
    assertEquals("file_required", m.getSourceField());
  }

  @Test
  void exportResources() throws HopException {
    IVariables variables = mock(IVariables.class);
    IResourceNaming resourceNamingInterface = mock(IResourceNaming.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    MetaInjectMeta injectMetaSpy = spy(metaInjectMeta);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    Map<String, ResourceDefinition> definitions = Collections.emptyMap();
    doReturn(TEST_FILE_NAME)
        .when(pipelineMeta)
        .exportResources(variables, definitions, resourceNamingInterface, metadataProvider);
    doReturn(pipelineMeta).when(injectMetaSpy).loadPipelineMeta(metadataProvider, variables);

    String actualExportedFileName =
        injectMetaSpy.exportResources(
            variables, definitions, resourceNamingInterface, metadataProvider);
    assertEquals(TEST_FILE_NAME, actualExportedFileName);
    assertEquals(EXPORTED_FILE_NAME, injectMetaSpy.getTemplateFileName());
    verify(pipelineMeta)
        .exportResources(variables, definitions, resourceNamingInterface, metadataProvider);
    verify(pipelineMeta).setFilename(EXPORTED_FILE_NAME);
  }

  @Test
  void testSampleMetaMapping() throws Exception {
    Map<String, Set<String>> map = HopMetadataInjector.findInjectionGroupKeys(MetaInjectMeta.class);
    assertNotNull(map);
    assertEquals(2, map.size());
    Set<String> fieldKeys = map.get("SOURCE_OUTPUT_FIELDS");
    assertEquals(4, fieldKeys.size());
    Set<String> mappingKeys = map.get("MAPPING_FIELDS");
    assertEquals(5, mappingKeys.size());

    assertTrue(fieldKeys.contains("SOURCE_OUTPUT_NAME"));
    assertTrue(fieldKeys.contains("SOURCE_OUTPUT_TYPE"));
    assertTrue(fieldKeys.contains("SOURCE_OUTPUT_LENGTH"));
    assertTrue(fieldKeys.contains("SOURCE_OUTPUT_PRECISION"));
    assertTrue(mappingKeys.contains("MAPPING_SOURCE_TRANSFORM"));
    assertTrue(mappingKeys.contains("MAPPING_SOURCE_FIELD"));
    assertTrue(mappingKeys.contains("MAPPING_TARGET_TRANSFORM"));
    assertTrue(mappingKeys.contains("MAPPING_TARGET_FIELD"));
    assertTrue(mappingKeys.contains("MAPPING_TARGET_DETAIL"));
  }
}
