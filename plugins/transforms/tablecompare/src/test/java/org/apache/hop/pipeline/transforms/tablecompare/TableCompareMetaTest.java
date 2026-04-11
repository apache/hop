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
package org.apache.hop.pipeline.transforms.tablecompare;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link TableCompareMeta} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class TableCompareMetaTest {
  LoadSaveTester<TableCompareMeta> loadSaveTester;
  Class<TableCompareMeta> testMetaClass = TableCompareMeta.class;

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes =
        Arrays.asList(
            "referenceConnection",
            "referenceSchemaField",
            "referenceTableField",
            "referenceCteField",
            "compareConnection",
            "compareSchemaField",
            "compareTableField",
            "compareCteField",
            "keyFieldsField",
            "excludeFieldsField",
            "nrErrorsField",
            "nrRecordsReferenceField",
            "nrRecordsCompareField",
            "nrErrorsLeftJoinField",
            "nrErrorsInnerJoinField",
            "nrErrorsRightJoinField",
            "keyDescriptionField",
            "valueReferenceField",
            "valueCompareField",
            "subjectFieldName");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester<>(
            testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap);
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  void setDefault_setsStatisticOutputFieldNames() {
    TableCompareMeta meta = new TableCompareMeta();
    meta.setDefault();
    assertEquals("nrErrors", meta.getNrErrorsField());
    assertEquals("nrRecordsReferenceTable", meta.getNrRecordsReferenceField());
    assertEquals("nrRecordsCompareTable", meta.getNrRecordsCompareField());
    assertEquals("nrErrorsLeftJoin", meta.getNrErrorsLeftJoinField());
    assertEquals("nrErrorsInnerJoin", meta.getNrErrorsInnerJoinField());
    assertEquals("nrErrorsRightJoin", meta.getNrErrorsRightJoinField());
  }

  @Test
  void getFields_appendsSixIntegerFieldsWithLength9() throws HopTransformException {
    TableCompareMeta meta = new TableCompareMeta();
    meta.setNrErrorsField("e");
    meta.setNrRecordsReferenceField("rr");
    meta.setNrRecordsCompareField("rc");
    meta.setNrErrorsLeftJoinField("lj");
    meta.setNrErrorsInnerJoinField("ij");
    meta.setNrErrorsRightJoinField("rj");

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "origin", null, null, new Variables(), null);

    assertEquals(6, rowMeta.size());
    String[] expected = {"e", "rr", "rc", "lj", "ij", "rj"};
    for (int i = 0; i < expected.length; i++) {
      IValueMeta vm = rowMeta.getValueMeta(i);
      assertEquals(expected[i], vm.getName());
      assertEquals(IValueMeta.TYPE_INTEGER, vm.getType());
      assertEquals(9, vm.getLength());
      assertEquals("origin", vm.getOrigin());
    }
  }

  @Test
  void getFields_throwsWhenAnyRequiredOutputFieldIsEmpty() {
    assertThrowsForEmptyField(meta -> meta.setNrErrorsField(""));
    assertThrowsForEmptyField(meta -> meta.setNrRecordsReferenceField(""));
    assertThrowsForEmptyField(meta -> meta.setNrRecordsCompareField(""));
    assertThrowsForEmptyField(meta -> meta.setNrErrorsLeftJoinField(""));
    assertThrowsForEmptyField(meta -> meta.setNrErrorsInnerJoinField(""));
    assertThrowsForEmptyField(meta -> meta.setNrErrorsRightJoinField(""));
    assertThrowsForEmptyField(meta -> meta.setNrErrorsField(null));
    assertThrowsForEmptyField(meta -> meta.setNrRecordsReferenceField(null));
    assertThrowsForEmptyField(meta -> meta.setNrRecordsCompareField(null));
    assertThrowsForEmptyField(meta -> meta.setNrErrorsLeftJoinField(null));
    assertThrowsForEmptyField(meta -> meta.setNrErrorsInnerJoinField(null));
    assertThrowsForEmptyField(meta -> meta.setNrErrorsRightJoinField(null));
  }

  private void assertThrowsForEmptyField(Consumer<TableCompareMeta> clearOneField) {
    TableCompareMeta meta = new TableCompareMeta();
    meta.setDefault();
    meta.setNrErrorsField("e");
    meta.setNrRecordsReferenceField("rr");
    meta.setNrRecordsCompareField("rc");
    meta.setNrErrorsLeftJoinField("lj");
    meta.setNrErrorsInnerJoinField("ij");
    meta.setNrErrorsRightJoinField("rj");
    clearOneField.accept(meta);
    assertThrows(
        HopTransformException.class,
        () -> meta.getFields(new RowMeta(), "o", null, null, new Variables(), null));
  }

  @Test
  void supportsErrorHandling_isTrue() {
    assertTrue(new TableCompareMeta().supportsErrorHandling());
  }

  @Test
  void createTransformData_returnsTableCompareData() {
    ITransformData data = new TableCompareMeta().createTransformData();
    assertInstanceOf(TableCompareData.class, data);
  }

  @Test
  void check_emptyPrev_emitsWarningAndInputErrorWhenNoInput() {
    TableCompareMeta meta = new TableCompareMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        null,
        new String[0],
        new String[0],
        null,
        new Variables(),
        metadataProvider);

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(1).getType());
  }

  @Test
  void check_withPrevAndInput_emitsOkRemarks() {
    TableCompareMeta meta = new TableCompareMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    RowMeta prev = new RowMeta();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        mock(IHopMetadataProvider.class));

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(1).getType());
  }
}
