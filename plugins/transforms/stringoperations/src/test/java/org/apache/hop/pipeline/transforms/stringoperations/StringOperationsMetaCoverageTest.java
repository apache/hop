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

package org.apache.hop.pipeline.transforms.stringoperations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Additional unit tests for {@link StringOperationsMeta}. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class StringOperationsMetaCoverageTest {

  @Test
  void cloneCreatesDeepCopyOfOperations() {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream("in");
    op.setFieldOutStream("out");
    op.setTrimType(StringOperationsMeta.TrimType.BOTH);
    op.setLowerUpper(StringOperationsMeta.LowerUpper.UPPER);
    meta.getOperations().add(op);

    StringOperationsMeta clone = (StringOperationsMeta) meta.clone();

    assertNotSame(meta, clone);
    assertNotSame(meta.getOperations(), clone.getOperations());
    assertEquals(1, clone.getOperations().size());
    assertEquals("in", clone.getOperations().getFirst().getFieldInStream());
    assertEquals("out", clone.getOperations().getFirst().getFieldOutStream());
    assertEquals(
        StringOperationsMeta.TrimType.BOTH, clone.getOperations().getFirst().getTrimType());
    assertEquals(
        StringOperationsMeta.LowerUpper.UPPER, clone.getOperations().getFirst().getLowerUpper());
  }

  @Test
  void stringOperationCopyConstructorCopiesAllFields() {
    StringOperationsMeta.StringOperation source = new StringOperationsMeta.StringOperation();
    source.setFieldInStream("in");
    source.setFieldOutStream("out");
    source.setTrimType(StringOperationsMeta.TrimType.LEFT);
    source.setLowerUpper(StringOperationsMeta.LowerUpper.LOWER);
    source.setInitCap(StringOperationsMeta.InitCap.YES);
    source.setMaskXml(StringOperationsMeta.MaskXml.ESCAPE_HTML);
    source.setDigits(StringOperationsMeta.Digits.DIGITS_ONLY);
    source.setRemoveSpecialChars(StringOperationsMeta.RemoveSpecialChars.TAB);
    source.setPaddingType(StringOperationsMeta.Padding.RIGHT);
    source.setPadChar("*");
    source.setPadLen("10");

    StringOperationsMeta.StringOperation copy = new StringOperationsMeta.StringOperation(source);

    assertEquals(source.getFieldInStream(), copy.getFieldInStream());
    assertEquals(source.getFieldOutStream(), copy.getFieldOutStream());
    assertEquals(source.getTrimType(), copy.getTrimType());
    assertEquals(source.getLowerUpper(), copy.getLowerUpper());
    assertEquals(source.getInitCap(), copy.getInitCap());
    assertEquals(source.getMaskXml(), copy.getMaskXml());
    assertEquals(source.getDigits(), copy.getDigits());
    assertEquals(source.getRemoveSpecialChars(), copy.getRemoveSpecialChars());
    assertEquals(source.getPaddingType(), copy.getPaddingType());
    assertEquals(source.getPadChar(), copy.getPadChar());
    assertEquals(source.getPadLen(), copy.getPadLen());
  }

  @Test
  void stringOperationDefaultsAreInitialized() {
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();

    assertEquals(StringOperationsMeta.TrimType.NONE, op.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.NONE, op.getLowerUpper());
    assertEquals(StringOperationsMeta.InitCap.NO, op.getInitCap());
    assertEquals(StringOperationsMeta.MaskXml.NONE, op.getMaskXml());
    assertEquals(StringOperationsMeta.Padding.NONE, op.getPaddingType());
    assertEquals(StringOperationsMeta.RemoveSpecialChars.NONE, op.getRemoveSpecialChars());
    assertEquals(StringOperationsMeta.Digits.NONE, op.getDigits());
  }

  @Test
  void supportsErrorHandling() {
    assertTrue(new StringOperationsMeta().supportsErrorHandling());
  }

  @Test
  void getFieldsAddsNewOutputField() throws HopTransformException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream("in_field");
    op.setFieldOutStream("out_field");
    meta.getOperations().add(op);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("in_field"));

    meta.getFields(rowMeta, "StringOperations", null, new TransformMeta(), new Variables(), null);

    assertNotNull(rowMeta.searchValueMeta("out_field"));
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.searchValueMeta("out_field").getType());
  }

  @Test
  void getFieldsUpdatesLengthWhenPaddingExceedsExistingLength() throws HopTransformException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream("in_field");
    op.setPaddingType(StringOperationsMeta.Padding.LEFT);
    op.setPadLen("50");
    meta.getOperations().add(op);

    ValueMetaString valueMeta = new ValueMetaString("in_field");
    valueMeta.setLength(10, -1);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(valueMeta);

    meta.getFields(rowMeta, "StringOperations", null, new TransformMeta(), new Variables(), null);

    assertEquals(50, rowMeta.searchValueMeta("in_field").getLength());
    assertEquals(
        IValueMeta.STORAGE_TYPE_NORMAL, rowMeta.searchValueMeta("in_field").getStorageType());
  }

  @Test
  void getFieldsSkipsMissingInputFieldWhenUpdatingInPlace() throws HopTransformException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream("missing");
    meta.getOperations().add(op);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("other"));

    meta.getFields(rowMeta, "StringOperations", null, new TransformMeta(), new Variables(), null);

    assertEquals(1, rowMeta.size());
  }

  @Test
  void checkReportsErrorWhenNoInputReceived() {
    StringOperationsMeta meta = new StringOperationsMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        null,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        null);

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.getFirst().getType());
  }

  @Test
  void checkReportsErrorForMissingInputFieldName() {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream("");
    meta.getOperations().add(op);

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("in_field"));

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        prev,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && remark.getText().contains("1")));
  }

  @Test
  void checkReportsErrorForDuplicateInputFields() {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op1 = new StringOperationsMeta.StringOperation();
    op1.setFieldInStream("dup");
    StringOperationsMeta.StringOperation op2 = new StringOperationsMeta.StringOperation();
    op2.setFieldInStream("dup");
    meta.getOperations().add(op1);
    meta.getOperations().add(op2);

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("dup"));

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        prev,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && remark.getText().contains("dup")));
  }

  @Test
  void checkReportsErrorForNonStringField() {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream("number");
    meta.getOperations().add(op);

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("number"));

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        prev,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && remark.getText().contains("number")));
  }

  @Test
  void checkReportsOkWhenAllFieldsAreStrings() {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream("text");
    meta.getOperations().add(op);

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("text"));

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        prev,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_OK
                        && remark
                            .getText()
                            .contains(
                                BaseMessages.getString(
                                    StringOperationsMeta.class,
                                    "StringOperationsMeta.CheckResult.AllOperationsOnStringFields"))));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.TrimType.class)
  void trimTypeLookupDescription(StringOperationsMeta.TrimType trimType) {
    assertEquals(
        trimType, StringOperationsMeta.TrimType.lookupDescription(trimType.getDescription()));
    assertTrue(StringOperationsMeta.TrimType.getDescriptions().length > 0);
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.LowerUpper.class)
  void lowerUpperLookupDescription(StringOperationsMeta.LowerUpper lowerUpper) {
    assertEquals(
        lowerUpper, StringOperationsMeta.LowerUpper.lookupDescription(lowerUpper.getDescription()));
    assertTrue(StringOperationsMeta.LowerUpper.getDescriptions().length > 0);
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.InitCap.class)
  void initCapLookupDescription(StringOperationsMeta.InitCap initCap) {
    assertEquals(initCap, StringOperationsMeta.InitCap.lookupDescription(initCap.getDescription()));
    assertTrue(StringOperationsMeta.InitCap.getDescriptions().length > 0);
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.Digits.class)
  void digitsLookupDescription(StringOperationsMeta.Digits digits) {
    assertEquals(digits, StringOperationsMeta.Digits.lookupDescription(digits.getDescription()));
    assertTrue(StringOperationsMeta.Digits.getDescriptions().length > 0);
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.MaskXml.class)
  void maskXmlLookupDescription(StringOperationsMeta.MaskXml maskXml) {
    assertEquals(maskXml, StringOperationsMeta.MaskXml.lookupDescription(maskXml.getDescription()));
    assertTrue(StringOperationsMeta.MaskXml.getDescriptions().length > 0);
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.RemoveSpecialChars.class)
  void removeSpecialCharsLookupDescription(
      StringOperationsMeta.RemoveSpecialChars removeSpecialChars) {
    assertEquals(
        removeSpecialChars,
        StringOperationsMeta.RemoveSpecialChars.lookupDescription(
            removeSpecialChars.getDescription()));
    assertTrue(StringOperationsMeta.RemoveSpecialChars.getDescriptions().length > 0);
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.Padding.class)
  void paddingLookupDescription(StringOperationsMeta.Padding padding) {
    assertEquals(padding, StringOperationsMeta.Padding.lookupDescription(padding.getDescription()));
    assertTrue(StringOperationsMeta.Padding.getDescriptions().length > 0);
  }

  @Test
  void enumLookupDescriptionReturnsDefaultForUnknownValue() {
    assertEquals(
        StringOperationsMeta.TrimType.NONE,
        StringOperationsMeta.TrimType.lookupDescription("unknown"));
    assertEquals(
        StringOperationsMeta.LowerUpper.NONE,
        StringOperationsMeta.LowerUpper.lookupDescription("unknown"));
    assertEquals(
        StringOperationsMeta.InitCap.NO, StringOperationsMeta.InitCap.lookupDescription("unknown"));
    assertEquals(
        StringOperationsMeta.Digits.NONE, StringOperationsMeta.Digits.lookupDescription("unknown"));
    assertEquals(
        StringOperationsMeta.MaskXml.NONE,
        StringOperationsMeta.MaskXml.lookupDescription("unknown"));
    assertEquals(
        StringOperationsMeta.RemoveSpecialChars.NONE,
        StringOperationsMeta.RemoveSpecialChars.lookupDescription("unknown"));
    assertEquals(
        StringOperationsMeta.Padding.NONE,
        StringOperationsMeta.Padding.lookupDescription("unknown"));
  }
}
