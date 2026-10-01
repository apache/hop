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
package org.apache.hop.pipeline.transforms.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.metadata.inject.HopMetadataInjector;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleMappingMetaInjectionTest {
  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void injectionKeysAreUnique() {
    BeanInjectionInfo<SimpleMappingMeta> info = new BeanInjectionInfo<>(SimpleMappingMeta.class);
    Set<String> keys = info.getProperties().keySet();

    assertTrue(keys.contains("filename"));
    assertTrue(keys.contains("runConfiguration"));
    assertTrue(keys.contains("INPUT_SOURCE_TRANSFORM"));
    assertTrue(keys.contains("INPUT_TARGET_TRANSFORM"));
    assertTrue(keys.contains("OUTPUT_SOURCE_TRANSFORM"));
    assertTrue(keys.contains("OUTPUT_TARGET_TRANSFORM"));
    assertTrue(keys.contains("INPUT_SOURCE_FIELD"));
    assertTrue(keys.contains("OUTPUT_SOURCE_FIELD"));
    assertTrue(keys.contains("PARAMETERS_INHERIT_ALL_VARIABLES"));
    assertTrue(keys.contains("PARAMETERS_VARIABLE"));
    assertTrue(keys.contains("PARAMETERS_VALUE"));
    assertFalse(keys.contains("input_transform"));
    assertFalse(keys.contains("output_transform"));

    assertEquals(keys.size(), new HashSet<>(keys).size());
    assertEquals("INPUT_FIELDS", info.getProperties().get("INPUT_SOURCE_FIELD").getGroupKey());
    assertEquals("OUTPUT_FIELDS", info.getProperties().get("OUTPUT_SOURCE_FIELD").getGroupKey());
    assertEquals(
        "PARAMETERS_MAPPINGS", info.getProperties().get("PARAMETERS_VARIABLE").getGroupKey());
  }

  @Test
  void injectsInputAndOutputIndependently() throws Exception {
    SimpleMappingMeta meta = new SimpleMappingMeta();

    Map<String, Object> injectionKeyMap = new HashMap<>();
    injectionKeyMap.put("filename", "child.hpl");
    injectionKeyMap.put("INPUT_SOURCE_TRANSFORM", "Reader");
    injectionKeyMap.put("INPUT_TARGET_TRANSFORM", "Mapping input");
    injectionKeyMap.put("OUTPUT_SOURCE_TRANSFORM", "Mapping output");
    injectionKeyMap.put("OUTPUT_TARGET_TRANSFORM", "Writer");
    injectionKeyMap.put("INPUT_MAIN_PATH", "Y");
    injectionKeyMap.put("OUTPUT_MAIN_PATH", "N");
    injectionKeyMap.put("INPUT_RENAME_ON_OUTPUT", "Y");
    injectionKeyMap.put("PARAMETERS_INHERIT_ALL_VARIABLES", "N");

    Map<String, RowBuffer> injectionGroupMap = new HashMap<>();
    RowBuffer inputFields = new RowBuffer();
    inputFields.setRowMeta(
        new RowMetaBuilder()
            .addString("INPUT_SOURCE_FIELD")
            .addString("INPUT_TARGET_FIELD")
            .build());
    inputFields.addRow("a", "fieldA");
    inputFields.addRow("b", "fieldB");
    injectionGroupMap.put("INPUT_FIELDS", inputFields);

    RowBuffer outputFields = new RowBuffer();
    outputFields.setRowMeta(
        new RowMetaBuilder()
            .addString("OUTPUT_SOURCE_FIELD")
            .addString("OUTPUT_TARGET_FIELD")
            .build());
    outputFields.addRow("fieldSum", "sum");
    injectionGroupMap.put("OUTPUT_FIELDS", outputFields);

    RowBuffer parameters = new RowBuffer();
    parameters.setRowMeta(
        new RowMetaBuilder()
            .addString("PARAMETERS_VARIABLE")
            .addString("PARAMETERS_VALUE")
            .build());
    parameters.addRow("LIMIT", "100");
    injectionGroupMap.put("PARAMETERS_MAPPINGS", parameters);

    HopMetadataInjector.inject(
        new MemoryMetadataProvider(), meta, injectionKeyMap, injectionGroupMap);

    assertEquals("child.hpl", meta.getFilename());
    assertEquals("Reader", meta.getInputMapping().getInputTransformName());
    assertEquals("Mapping input", meta.getInputMapping().getOutputTransformName());
    assertEquals("Mapping output", meta.getOutputMapping().getInputTransformName());
    assertEquals("Writer", meta.getOutputMapping().getOutputTransformName());
    assertTrue(meta.getInputMapping().isMainDataPath());
    assertFalse(meta.getOutputMapping().isMainDataPath());
    assertTrue(meta.getInputMapping().isRenamingOnOutput());
    assertFalse(meta.getMappingParameters().isInheritingAllVariables());

    assertEquals(2, meta.getInputMapping().getValueRenames().size());
    assertEquals("a", meta.getInputMapping().getValueRenames().get(0).getSourceValueName());
    assertEquals("fieldA", meta.getInputMapping().getValueRenames().get(0).getTargetValueName());
    assertEquals("b", meta.getInputMapping().getValueRenames().get(1).getSourceValueName());
    assertEquals("fieldB", meta.getInputMapping().getValueRenames().get(1).getTargetValueName());

    assertEquals(1, meta.getOutputMapping().getValueRenames().size());
    assertEquals("fieldSum", meta.getOutputMapping().getValueRenames().get(0).getSourceValueName());
    assertEquals("sum", meta.getOutputMapping().getValueRenames().get(0).getTargetValueName());

    assertEquals(1, meta.getMappingParameters().getVariableMappings().size());
    assertEquals("LIMIT", meta.getMappingParameters().getVariableMappings().get(0).getName());
    assertEquals("100", meta.getMappingParameters().getVariableMappings().get(0).getValue());
  }
}
