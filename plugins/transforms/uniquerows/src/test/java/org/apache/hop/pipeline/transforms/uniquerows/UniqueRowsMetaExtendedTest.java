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

package org.apache.hop.pipeline.transforms.uniquerows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class UniqueRowsMetaExtendedTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testDefaultConstructor() {
    UniqueRowsMeta meta = new UniqueRowsMeta();

    assertNotNull(meta, "UniqueRowsMeta instance should be created successfully.");
    assertFalse(meta.isCountRows(), "countRows should be false by default");
    assertNull(meta.getCountField(), "countField should be null by default");
    assertNotNull(meta.getCompareFields(), "compareFields should not be null");
    assertTrue(meta.getCompareFields().isEmpty(), "compareFields should be empty by default");
    assertFalse(meta.isRejectDuplicateRow(), "rejectDuplicateRow should be false by default");
    assertNull(meta.getErrorDescription(), "errorDescription should be null by default");
  }

  @Test
  void testCopyConstructor() {
    // Create original meta with some values
    UniqueRowsMeta original = new UniqueRowsMeta();
    original.setCountRows(true);
    original.setCountField("countField");
    original.setRejectDuplicateRow(true);
    original.setErrorDescription("Test error");

    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("field1", true));
    compareFields.add(new UniqueField("field2", false));
    original.setCompareFields(compareFields);

    // Create copy using copy constructor
    UniqueRowsMeta copy = new UniqueRowsMeta(original);

    assertNotNull(copy, "Copy should be created successfully.");
    assertEquals(original.isCountRows(), copy.isCountRows(), "countRows should be copied");
    assertEquals(original.getCountField(), copy.getCountField(), "countField should be copied");
    assertEquals(
        original.isRejectDuplicateRow(),
        copy.isRejectDuplicateRow(),
        "rejectDuplicateRow should be copied");
    assertEquals(
        original.getErrorDescription(),
        copy.getErrorDescription(),
        "errorDescription should be copied");

    // Compare fields should be copied but not the same reference
    assertNotNull(copy.getCompareFields(), "compareFields should not be null");
    assertEquals(
        original.getCompareFields().size(),
        copy.getCompareFields().size(),
        "compareFields size should match");
    assertFalse(
        original.getCompareFields() == copy.getCompareFields(),
        "compareFields should be different instances");

    // Verify individual fields are copied correctly
    for (int i = 0; i < original.getCompareFields().size(); i++) {
      UniqueField originalField = original.getCompareFields().get(i);
      UniqueField copyField = copy.getCompareFields().get(i);
      assertEquals(originalField.getName(), copyField.getName(), "Field name should be copied");
      assertEquals(
          originalField.isCaseInsensitive(),
          copyField.isCaseInsensitive(),
          "Field caseInsensitive should be copied");
    }
  }

  @Test
  void testCopyConstructorWithEmptyCompareFields() {
    UniqueRowsMeta original = new UniqueRowsMeta();
    original.setCountRows(false);
    original.setCountField("");
    original.setRejectDuplicateRow(false);
    original.setErrorDescription(null);
    original.setCompareFields(new ArrayList<>());

    UniqueRowsMeta copy = new UniqueRowsMeta(original);

    assertNotNull(copy, "Copy should be created successfully.");
    assertEquals(original.isCountRows(), copy.isCountRows(), "countRows should be copied");
    assertEquals(original.getCountField(), copy.getCountField(), "countField should be copied");
    assertEquals(
        original.isRejectDuplicateRow(),
        copy.isRejectDuplicateRow(),
        "rejectDuplicateRow should be copied");
    assertEquals(
        original.getErrorDescription(),
        copy.getErrorDescription(),
        "errorDescription should be copied");
    assertNotNull(copy.getCompareFields(), "compareFields should not be null");
    assertTrue(copy.getCompareFields().isEmpty(), "compareFields should be empty");
  }

  @Test
  void testGettersAndSetters() {
    UniqueRowsMeta meta = new UniqueRowsMeta();

    // Test countRows
    meta.setCountRows(true);
    assertTrue(meta.isCountRows(), "setCountRows should set the value correctly");
    meta.setCountRows(false);
    assertFalse(meta.isCountRows(), "setCountRows should set the value correctly");

    // Test countField
    String countField = "testCountField";
    meta.setCountField(countField);
    assertEquals(countField, meta.getCountField(), "setCountField should set the value correctly");
    meta.setCountField(null);
    assertNull(meta.getCountField(), "setCountField with null should work");

    // Test rejectDuplicateRow
    meta.setRejectDuplicateRow(true);
    assertTrue(meta.isRejectDuplicateRow(), "setRejectDuplicateRow should set the value correctly");
    meta.setRejectDuplicateRow(false);
    assertFalse(
        meta.isRejectDuplicateRow(), "setRejectDuplicateRow should set the value correctly");

    // Test errorDescription
    String errorDescription = "Test error description";
    meta.setErrorDescription(errorDescription);
    assertEquals(
        errorDescription,
        meta.getErrorDescription(),
        "setErrorDescription should set the value correctly");
    meta.setErrorDescription(null);
    assertNull(meta.getErrorDescription(), "setErrorDescription with null should work");

    // Test compareFields
    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("field1", true));
    compareFields.add(new UniqueField("field2", false));
    meta.setCompareFields(compareFields);
    assertEquals(
        compareFields, meta.getCompareFields(), "setCompareFields should set the value correctly");
    meta.setCompareFields(null);
    assertNull(meta.getCompareFields(), "setCompareFields with null should work");
  }

  @Test
  void testClone() {
    UniqueRowsMeta original = new UniqueRowsMeta();
    original.setCountRows(true);
    original.setCountField("countField");
    original.setRejectDuplicateRow(true);
    original.setErrorDescription("Test error");

    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("field1", true));
    compareFields.add(new UniqueField("field2", false));
    original.setCompareFields(compareFields);

    UniqueRowsMeta clone = (UniqueRowsMeta) original.clone();

    assertNotNull(clone, "Clone should be created successfully.");
    assertTrue(clone instanceof UniqueRowsMeta, "Clone should be instance of UniqueRowsMeta");
    assertEquals(original.isCountRows(), clone.isCountRows(), "countRows should be cloned");
    assertEquals(original.getCountField(), clone.getCountField(), "countField should be cloned");
    assertEquals(
        original.isRejectDuplicateRow(),
        clone.isRejectDuplicateRow(),
        "rejectDuplicateRow should be cloned");
    assertEquals(
        original.getErrorDescription(),
        clone.getErrorDescription(),
        "errorDescription should be cloned");

    // Compare fields should be cloned but not the same reference
    assertNotNull(clone.getCompareFields(), "compareFields should not be null");
    assertEquals(
        original.getCompareFields().size(),
        clone.getCompareFields().size(),
        "compareFields size should match");
    assertFalse(
        original.getCompareFields() == clone.getCompareFields(),
        "compareFields should be different instances");
  }

  @Test
  void testSetDefault() {
    UniqueRowsMeta meta = new UniqueRowsMeta();

    // Set some values first
    meta.setCountRows(true);
    meta.setCountField("testField");
    meta.setRejectDuplicateRow(true);
    meta.setErrorDescription("Test error");

    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("field1", true));
    meta.setCompareFields(compareFields);

    // Call setDefault
    meta.setDefault();

    // Verify default values
    assertFalse(meta.isCountRows(), "countRows should be false after setDefault");
    assertEquals("", meta.getCountField(), "countField should be empty string after setDefault");
    assertFalse(meta.isRejectDuplicateRow(), "rejectDuplicateRow should be false after setDefault");
    assertNull(meta.getErrorDescription(), "errorDescription should be null after setDefault");
    assertNotNull(meta.getCompareFields(), "compareFields should not be null after setDefault");
    assertTrue(meta.getCompareFields().isEmpty(), "compareFields should be empty after setDefault");
  }

  @Test
  void testGetFieldsWithCountRows() throws Exception {
    UniqueRowsMeta meta = new UniqueRowsMeta();
    meta.setCountRows(true);
    meta.setCountField("countField");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("name"));
    row.addValueMeta(new ValueMetaInteger("age"));

    IHopMetadataProvider metadataProvider = Mockito.mock(IHopMetadataProvider.class);
    Variables variables = new Variables();

    meta.getFields(row, "testTransform", null, null, variables, metadataProvider);

    // Should have original fields plus count field
    assertEquals(3, row.size(), "Row should have 3 fields after adding count field");
    assertEquals("name", row.getValueMeta(0).getName(), "First field should be name");
    assertEquals("age", row.getValueMeta(1).getName(), "Second field should be age");
    assertEquals("countField", row.getValueMeta(2).getName(), "Third field should be countField");
    assertTrue(
        row.getValueMeta(2) instanceof ValueMetaInteger, "Count field should be ValueMetaInteger");
  }

  @Test
  void testGetFieldsWithoutCountRows() throws Exception {
    UniqueRowsMeta meta = new UniqueRowsMeta();
    meta.setCountRows(false);

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("name"));
    row.addValueMeta(new ValueMetaInteger("age"));

    IHopMetadataProvider metadataProvider = Mockito.mock(IHopMetadataProvider.class);
    Variables variables = new Variables();

    meta.getFields(row, "testTransform", null, null, variables, metadataProvider);

    // Should have original fields only
    assertEquals(2, row.size(), "Row should have 2 fields without count field");
    assertEquals("name", row.getValueMeta(0).getName(), "First field should be name");
    assertEquals("age", row.getValueMeta(1).getName(), "Second field should be age");
  }

  @Test
  void testGetFieldsWithCompareFields() throws Exception {
    UniqueRowsMeta meta = new UniqueRowsMeta();
    meta.setCountRows(false);

    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("name", true)); // Case insensitive
    compareFields.add(new UniqueField("age", false)); // Case sensitive
    meta.setCompareFields(compareFields);

    IRowMeta row = new RowMeta();
    ValueMetaString nameMeta = new ValueMetaString("name");
    ValueMetaInteger ageMeta = new ValueMetaInteger("age");
    row.addValueMeta(nameMeta);
    row.addValueMeta(ageMeta);

    IHopMetadataProvider metadataProvider = Mockito.mock(IHopMetadataProvider.class);
    Variables variables = new Variables();

    meta.getFields(row, "testTransform", null, null, variables, metadataProvider);

    // Verify case insensitive settings
    assertTrue(row.getValueMeta(0).isCaseInsensitive(), "name field should be case insensitive");
    assertFalse(row.getValueMeta(1).isCaseInsensitive(), "age field should be case sensitive");
  }

  @Test
  void testCheckWithInput() {
    UniqueRowsMeta meta = new UniqueRowsMeta();
    PipelineMeta pipelineMeta = Mockito.mock(PipelineMeta.class);
    TransformMeta transformMeta = Mockito.mock(TransformMeta.class);
    IRowMeta prev = new RowMeta();
    String[] input = {"input1", "input2"};
    String[] output = {"output1"};
    IRowMeta info = new RowMeta();
    Variables variables = new Variables();
    IHopMetadataProvider metadataProvider = Mockito.mock(IHopMetadataProvider.class);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prev,
        input,
        output,
        info,
        variables,
        metadataProvider);

    assertEquals(1, remarks.size(), "Should have one check result");
    ICheckResult result = remarks.get(0);
    assertEquals(ICheckResult.TYPE_RESULT_OK, result.getType(), "Check result should be OK");
  }

  @Test
  void testCheckWithoutInput() {
    UniqueRowsMeta meta = new UniqueRowsMeta();
    PipelineMeta pipelineMeta = Mockito.mock(PipelineMeta.class);
    TransformMeta transformMeta = Mockito.mock(TransformMeta.class);
    IRowMeta prev = new RowMeta();
    String[] input = {}; // No input
    String[] output = {"output1"};
    IRowMeta info = new RowMeta();
    Variables variables = new Variables();
    IHopMetadataProvider metadataProvider = Mockito.mock(IHopMetadataProvider.class);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prev,
        input,
        output,
        info,
        variables,
        metadataProvider);

    assertEquals(1, remarks.size(), "Should have one check result");
    ICheckResult result = remarks.get(0);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, result.getType(), "Check result should be ERROR");
  }

  @Test
  void testSupportsErrorHandling() {
    UniqueRowsMeta meta = new UniqueRowsMeta();

    // Test with rejectDuplicateRow = true
    meta.setRejectDuplicateRow(true);
    assertTrue(
        meta.supportsErrorHandling(),
        "Should support error handling when rejectDuplicateRow is true");

    // Test with rejectDuplicateRow = false
    meta.setRejectDuplicateRow(false);
    assertFalse(
        meta.supportsErrorHandling(),
        "Should not support error handling when rejectDuplicateRow is false");
  }

  @Test
  void testCompareFieldsModification() {
    UniqueRowsMeta meta = new UniqueRowsMeta();

    // Initially empty
    assertTrue(meta.getCompareFields().isEmpty(), "compareFields should be empty initially");

    // Add fields
    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("field1", true));
    compareFields.add(new UniqueField("field2", false));
    meta.setCompareFields(compareFields);

    assertEquals(2, meta.getCompareFields().size(), "compareFields should have 2 fields");
    assertEquals(
        "field1", meta.getCompareFields().get(0).getName(), "First field name should be field1");
    assertTrue(
        meta.getCompareFields().get(0).isCaseInsensitive(),
        "First field should be case insensitive");
    assertEquals(
        "field2", meta.getCompareFields().get(1).getName(), "Second field name should be field2");
    assertFalse(
        meta.getCompareFields().get(1).isCaseInsensitive(),
        "Second field should be case sensitive");

    // Modify existing field
    meta.getCompareFields().get(0).setName("modifiedField1");
    assertEquals(
        "modifiedField1",
        meta.getCompareFields().get(0).getName(),
        "Field name should be modified");

    // Add new field
    meta.getCompareFields().add(new UniqueField("field3", true));
    assertEquals(
        3, meta.getCompareFields().size(), "compareFields should have 3 fields after addition");
  }

  @Test
  void testErrorDescriptionWithVariables() {
    UniqueRowsMeta meta = new UniqueRowsMeta();
    String errorDescription = "Error in ${TRANSFORM_NAME}";
    meta.setErrorDescription(errorDescription);
    assertEquals(
        errorDescription, meta.getErrorDescription(), "Error description should be set correctly");
  }

  @Test
  void testCountFieldWithVariables() {
    UniqueRowsMeta meta = new UniqueRowsMeta();
    String countField = "count_${TRANSFORM_NAME}";
    meta.setCountField(countField);
    assertEquals(countField, meta.getCountField(), "Count field should be set correctly");
  }
}
