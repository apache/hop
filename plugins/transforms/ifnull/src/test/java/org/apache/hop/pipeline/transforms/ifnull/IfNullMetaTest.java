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
package org.apache.hop.pipeline.transforms.ifnull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class IfNullMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  public void testLoadSaveValueType() throws Exception {
    IfNullMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/if-null-transform-value-type.xml", IfNullMeta.class);

    assertTrue(meta.isSelectValuesType());
    assertEquals(3, meta.getValueTypes().size());
    assertFalse(meta.isSelectFields());
    assertEquals(0, meta.getFields().size());

    assertEquals("String", meta.getValueTypes().get(0).getName());
    assertNull(meta.getValueTypes().get(0).getValue());
    assertTrue(meta.getValueTypes().get(0).isSetEmptyString());
    assertEquals("Number", meta.getValueTypes().get(1).getName());
    assertNull(meta.getValueTypes().get(1).getValue());
    assertFalse(meta.getValueTypes().get(1).isSetEmptyString());
    assertEquals("Date", meta.getValueTypes().get(2).getName());
    assertNull(meta.getValueTypes().get(2).getValue());
    assertFalse(meta.getValueTypes().get(2).isSetEmptyString());
  }

  @Test
  public void testLoadSaveField() throws Exception {
    IfNullMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/if-null-transform-field.xml", IfNullMeta.class);

    assertFalse(meta.isSelectValuesType());
    assertEquals(0, meta.getValueTypes().size());
    assertTrue(meta.isSelectFields());
    assertEquals(3, meta.getFields().size());

    assertEquals("F1", meta.getFields().get(0).getName());
    assertEquals("EMPTY", meta.getFields().get(0).getValue());
    assertFalse(meta.getFields().get(0).isSetEmptyString());
    assertEquals("F2", meta.getFields().get(1).getName());
    assertEquals("01019999", meta.getFields().get(1).getValue());
    assertEquals("ddMMYYYY", meta.getFields().get(1).getMask());
    assertFalse(meta.getFields().get(1).isSetEmptyString());
    assertEquals("F3", meta.getFields().get(2).getName());
    assertNull(meta.getFields().get(2).getValue());
    assertTrue(meta.getFields().get(2).isSetEmptyString());
  }

  @Test
  public void testSetDefault() throws Exception {
    IfNullMeta meta = new IfNullMeta();
    meta.setDefault();
    assertTrue((meta.getValueTypes() != null) && (meta.getValueTypes().isEmpty()));
    assertTrue((meta.getFields() != null) && (meta.getFields().isEmpty()));
    assertFalse(meta.isSelectFields());
    assertFalse(meta.isSelectValuesType());
  }
}
