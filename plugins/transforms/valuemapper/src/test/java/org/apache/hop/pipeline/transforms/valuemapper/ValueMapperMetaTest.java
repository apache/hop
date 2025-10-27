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
package org.apache.hop.pipeline.transforms.valuemapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ValueMapperMetaTest {

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testBackwardNoNewFieldNoType() throws Exception {
    ValueMapperMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/value-mapper-transform.xml", ValueMapperMeta.class);

    // backward-compat scenario: in-place mapping, no explicit type
    meta.setFieldToUse("Country_Code");
    meta.setTargetField("");
    meta.setTargetType(null);

    // Input schema: only the source field, String
    IRowMeta input = new RowMeta();
    ValueMetaString src = new ValueMetaString("Country_Code");
    src.setLength(50);
    input.addValueMeta(src);

    // target field must not exist in input
    assertNull(input.searchValueMeta("Country_Name"));

    IRowMeta out = input.clone();
    meta.getFields(out, "ValueMapper", null, new TransformMeta(), null, null);

    // No new field added
    assertEquals(
        input.size(), out.size(), "Output meta should have same number of fields as input");
    assertNull(out.searchValueMeta("Country_Name"), "No target field should be added");

    // Same field present with same name and type (String), unchanged length
    IValueMeta inVm = input.searchValueMeta("Country_Code");
    IValueMeta outVm = out.searchValueMeta("Country_Code");
    assertNotNull(outVm, "Source field must remain present");
    assertEquals(inVm.getType(), outVm.getType(), "Type should remain unchanged in-place");
    assertEquals(inVm.getName(), outVm.getName(), "Name should remain unchanged");
    assertEquals(inVm.getLength(), outVm.getLength(), "Length should remain unchanged");
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, outVm.getStorageType(), "Storage must be NORMAL");
  }

  @Test
  void testSerialization() throws Exception {
    ValueMapperMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/value-mapper-transform.xml", ValueMapperMeta.class);

    assertEquals(7, meta.getValues().size());
    // Test serialization with null source attribute
    assertNull(meta.getValues().get(0).getSource());
    assertEquals("[${NOT_DEFINED}]", meta.getValues().get(0).getTarget());

    assertEquals("BE", meta.getValues().get(1).getSource());
    assertEquals("Belgium", meta.getValues().get(1).getTarget());

    assertEquals("Country_Code", meta.getFieldToUse());
    assertEquals("Country_Name", meta.getTargetField());
    assertEquals("[${NOT_FOUND}]", meta.getNonMatchDefault());

    assertNull(meta.getTargetType(), "Expected null target type right after deserialization");
    // Before getFields: input has only the source field
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("Country_Code"));
    assertNull(input.searchValueMeta("Country_Name"));

    // After getFields: a NEW field "Country_Name" is added and defaults to String type
    IRowMeta out = input.clone();
    meta.getFields(out, "ValueMapper", null, new TransformMeta(), null, null);

    assertNotNull(
        out.searchValueMeta("Country_Name"), "Target field should be added by getFields()");
    assertEquals(
        IValueMeta.TYPE_STRING,
        out.searchValueMeta("Country_Name").getType(),
        "New target field should default to String type when no explicit target type is set");
  }
}
