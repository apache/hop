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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
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
  void testInPlaceNoTargetTypeDefaultsToString() throws Exception {
    ValueMapperMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/value-mapper-transform.xml", ValueMapperMeta.class);

    // In-place mapping, no explicit target type: output column type is String
    meta.setFieldToUse("Country_Code");
    meta.setTargetField("");
    meta.setTargetType(null);

    // Input schema: numeric codes mapped to country names → output must be string
    IRowMeta input = new RowMeta();
    ValueMetaInteger src = new ValueMetaInteger("Country_Code");
    input.addValueMeta(src);

    assertNull(input.searchValueMeta("Country_Name"));

    IRowMeta out = input.clone();
    meta.getFields(out, "ValueMapper", null, new TransformMeta(), null, null);

    assertEquals(
        input.size(), out.size(), "Output meta should have same number of fields as input");
    assertNull(out.searchValueMeta("Country_Name"), "No target field should be added");

    IValueMeta outVm = out.searchValueMeta("Country_Code");
    assertNotNull(outVm, "Source field must remain present");
    assertEquals(IValueMeta.TYPE_STRING, outVm.getType(), "Unspecified target type must be String");
    assertEquals("Country_Code", outVm.getName(), "Name should remain unchanged");
    assertEquals(16, outVm.getLength(), "Length from longest mapping / default literals");
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, outVm.getStorageType(), "Storage must be NORMAL");
  }

  @Test
  void legacyXmlMissingEmptyStringEqualsNullDefaultsToTrue() throws Exception {
    ValueMapperMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/value-mapper-transform.xml", ValueMapperMeta.class);

    for (Values v : meta.getValues()) {
      assertEquals(
          true,
          v.isEmptyStringEqualsNull(),
          "Older pipelines without empty_string_equals_null must default to Y (true)");
    }
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

  @Test
  void cloneCopiesValuesAndEmptyStringEqualsNull() {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("src");
    Values v = new Values();
    v.setSource("x");
    v.setTarget("y");
    v.setEmptyStringEqualsNull(false);
    meta.getValues().add(v);

    ValueMapperMeta clone = (ValueMapperMeta) meta.clone();
    assertEquals(1, clone.getValues().size());
    assertFalse(clone.getValues().get(0).isEmptyStringEqualsNull());
    assertEquals("x", clone.getValues().get(0).getSource());
    assertEquals("y", clone.getValues().get(0).getTarget());
  }

  @Test
  void checkAddsWarningWhenNoPreviousFields() {
    ValueMapperMeta meta = new ValueMapperMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        null,
        new String[] {"in"},
        new String[0],
        null,
        null,
        null);
    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(1).getType());
  }

  @Test
  void checkAddsErrorWhenNoInputTransforms() {
    ValueMapperMeta meta = new ValueMapperMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("c"));
    meta.check(
        remarks, pipelineMeta, transformMeta, prev, new String[0], new String[0], null, null, null);
    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(1).getType());
  }

  @Test
  void getFieldsAddsIntegerTypedTargetColumnWhenSpecified() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("id");
    meta.setTargetField("amt");
    meta.setTargetType("Integer");
    meta.getValues().add(new Values("1", "42"));
    IRowMeta in = new RowMeta();
    in.addValueMeta(new ValueMetaString("id"));
    IRowMeta out = in.clone();
    meta.getFields(out, "vm", null, new TransformMeta(), null, null);
    assertEquals(IValueMeta.TYPE_INTEGER, out.searchValueMeta("amt").getType());
  }

  @Test
  void getFieldsUnknownTargetTypeFallsBackToStringForNewColumn() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("id");
    meta.setTargetField("out");
    meta.setTargetType("bogus_type_xyz");
    meta.getValues().add(new Values("a", "b"));
    IRowMeta in = new RowMeta();
    in.addValueMeta(new ValueMetaString("id"));
    IRowMeta out = in.clone();
    meta.getFields(out, "vm", null, new TransformMeta(), null, null);
    assertEquals(IValueMeta.TYPE_STRING, out.searchValueMeta("out").getType());
  }

  @Test
  void inPlaceWithExplicitTargetTypeDoesNotCoerceColumnInGetFields() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType("String");
    meta.getValues().add(new Values("1", "one"));
    IRowMeta in = new RowMeta();
    in.addValueMeta(new ValueMetaInteger("code"));
    IRowMeta out = in.clone();
    meta.getFields(out, "vm", null, new TransformMeta(), null, null);
    assertEquals(IValueMeta.TYPE_INTEGER, out.searchValueMeta("code").getType());
  }
}
