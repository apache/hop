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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleMappingMetaTest {
  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    TransformSerializationTestUtil.testSerialization(
        "/simple-mapping-transform.xml", SimpleMappingMeta.class);
  }

  @Test
  void testNoInputAndNoOutputFlags() throws Exception {
    SimpleMappingMeta meta = new SimpleMappingMeta();
    meta.setDefault();
    assertFalse(meta.isNoInput());
    assertFalse(meta.isNoOutput());
    assertTrue(meta.getTransformIOMeta().isInputAcceptor());
    assertTrue(meta.getTransformIOMeta().isOutputProducer());

    meta.setNoInput(true);
    assertTrue(meta.isNoInput());
    assertFalse(meta.getTransformIOMeta().isInputAcceptor());
    assertTrue(meta.getTransformIOMeta().isOutputProducer());

    meta.setNoOutput(true);
    assertTrue(meta.isNoOutput());
    assertFalse(meta.getTransformIOMeta().isInputAcceptor());
    assertFalse(meta.getTransformIOMeta().isOutputProducer());

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("a"));
    meta.getFields(row, "origin", null, null, new Variables(), new MemoryMetadataProvider());
    assertEquals(0, row.size(), "getFields should clear the row when noOutput is true");
  }
}
