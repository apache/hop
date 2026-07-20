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

package org.apache.hop.pipeline.transforms.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link Validator} */
class ValidatorTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<ValidatorMeta, ValidatorData> helper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    helper = new TransformMockHelper<>("Validator", ValidatorMeta.class, ValidatorData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    when(helper.transformMeta.getName()).thenReturn("Validator");
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void testValidRowIsPassedThrough() throws Exception {
    ValidatorMeta meta = createMeta(nullNotAllowed());
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"ok"}).doReturn(null).when(validator).getRow();
    doNothing().when(validator).putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(validator.processRow());
    verify(validator).putRow(any(IRowMeta.class), eq(new Object[] {"ok"}));
    verify(validator, never())
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            anyString(),
            anyString());
    assertFalse(validator.processRow());
  }

  @Test
  void testNullNotAllowedPutsError() throws Exception {
    ValidatorMeta meta = createMeta(nullNotAllowed());
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {null}).doReturn(null).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD001"));
  }

  @Test
  void testNullNotAllowedWithoutErrorHandlingThrows() throws Exception {
    ValidatorMeta meta = createMeta(nullNotAllowed());
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, false);

    doReturn(new Object[] {null}).when(validator).getRow();

    HopException exception = assertThrows(HopException.class, validator::processRow);
    assertTrue(exception.getMessage().contains("null"));
  }

  @Test
  void testMaximumLengthViolation() throws Exception {
    Validation rule = new Validation();
    rule.setName("max length");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setMaximumLength("3");

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"toolong"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            contains("toolong"),
            eq("value"),
            eq("KVD002"));
  }

  @Test
  void testMinimumLengthViolation() throws Exception {
    Validation rule = new Validation();
    rule.setName("min length");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setMinimumLength("5");

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"abc"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            contains("abc"),
            eq("value"),
            eq("KVD003"));
  }

  @Test
  void testValueNotInAllowedList() throws Exception {
    Validation rule = new Validation();
    rule.setName("list");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setDataType("String");
    rule.setAllowedValues(List.of("A", "B"));

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"C"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            contains("C"),
            eq("value"),
            eq("KVD007"));
  }

  @Test
  void testOnlyNumericAllowed() throws Exception {
    Validation rule = new Validation();
    rule.setName("numeric");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setOnlyNumericAllowed(true);

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"12a"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD008"));
  }

  @Test
  void testRegularExpressionExpected() throws Exception {
    Validation rule = new Validation();
    rule.setName("regex");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setRegularExpression("^[0-9]+$");

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"abc"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD013"));
  }

  @Test
  void testMinimumValueViolation() throws Exception {
    Validation rule = new Validation();
    rule.setName("min value");
    rule.setFieldName("amount");
    rule.setNullAllowed(true);
    rule.setDataType("Integer");
    rule.setMinimumValue("10");

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {5L}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("amount"),
            eq("KVD005"));
  }

  @Test
  void testValidateAllConcatenatesErrorsIntoOneRow() throws Exception {
    Validation lengthRule = new Validation();
    lengthRule.setName("length");
    lengthRule.setFieldName("value");
    lengthRule.setNullAllowed(true);
    lengthRule.setMaximumLength("1");

    Validation onlyNull = new Validation();
    onlyNull.setName("only null");
    onlyNull.setFieldName("value");
    onlyNull.setNullAllowed(true);
    onlyNull.setOnlyNullAllowed(true);

    ValidatorMeta meta = createMeta(lengthRule, onlyNull);
    meta.setValidatingAll(true);
    meta.setConcatenatingErrors(true);
    meta.setConcatenationSeparator(";");

    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"ab"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(2L),
            contains(";"),
            contains(";"),
            contains(";"));
  }

  @Test
  void testCustomErrorCodeAndDescription() throws Exception {
    Validation rule = nullNotAllowed();
    rule.setErrorCode("MY_CODE");
    rule.setErrorDescription("custom description");

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {null}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            eq("custom description"),
            eq("value"),
            eq("MY_CODE"));
  }

  @Test
  void testSuppressLogFailedDataOmitsValuesFromErrorMessages() throws Exception {
    Validation rule = new Validation();
    rule.setName("max length");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setMaximumLength("3");

    ValidatorMeta meta = createMeta(rule);
    meta.setSuppressingLogFailedData(true);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"toolong"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            contains(Validator.OMITTED_VALUE),
            eq("value"),
            eq("KVD002"));
    verify(validator, never())
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            contains("toolong"),
            anyString(),
            anyString());
  }

  @Test
  void testSuppressLogFailedDataStillLogsCustomErrorDescription() throws Exception {
    Validation rule = nullNotAllowed();
    rule.setErrorCode("name_valid_code");
    rule.setErrorDescription("name_valid_desc");

    ValidatorMeta meta = createMeta(rule);
    meta.setSuppressingLogFailedData(true);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {null}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            eq("name_valid_desc"),
            eq("value"),
            eq("name_valid_code"));
  }

  @Test
  void testValueAndRowForMessageRespectSuppressFlag() throws Exception {
    ValidatorMeta meta = new ValidatorMeta();
    ValidatorData data = new ValidatorData();
    Validator validator =
        new Validator(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);

    ValueMetaString valueMeta = new ValueMetaString("value");
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(valueMeta);
    Object[] row = new Object[] {"secret"};

    meta.setSuppressingLogFailedData(false);
    assertEquals("secret", validator.getValueForMessage(valueMeta, "secret"));
    assertEquals(rowMeta.getString(row), validator.getRowForMessage(rowMeta, row));

    meta.setSuppressingLogFailedData(true);
    assertEquals(Validator.OMITTED_VALUE, validator.getValueForMessage(valueMeta, "secret"));
    assertEquals(Validator.OMITTED_VALUE, validator.getRowForMessage(rowMeta, row));
  }

  @Test
  void testAssertNumericAllowsDigitsAndNumericTypes() throws Exception {
    ValidatorMeta meta = new ValidatorMeta();
    ValidatorData data = new ValidatorData();
    Validator validator =
        new Validator(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);

    Validation field = new Validation();
    field.setFieldName("value");
    ValueMetaString stringMeta = new ValueMetaString("value");
    ValueMetaInteger integerMeta = new ValueMetaInteger("value");

    assertNull(validator.assertNumeric(stringMeta, "12345", field));
    assertNull(validator.assertNumeric(integerMeta, 42L, field));
    HopValidatorException exception = validator.assertNumeric(stringMeta, "12a", field);
    assertEquals(HopValidatorException.ERROR_NON_NUMERIC_DATA, exception.getCode());
  }

  @Test
  void testStartAndEndStringRules() throws Exception {
    Validation rule = new Validation();
    rule.setName("prefix");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setStartString("AB");
    rule.setEndString("Z");

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = stringRowMeta();
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {"ABX"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD010"));
  }

  @Test
  void testDoesNotStartWithExpectedString() throws Exception {
    Validation rule = new Validation();
    rule.setName("start");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setStartString("AB");

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"XX"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD009"));
  }

  @Test
  void testMaximumValueViolation() throws Exception {
    Validation rule = new Validation();
    rule.setName("max value");
    rule.setFieldName("amount");
    rule.setNullAllowed(true);
    rule.setDataType("Integer");
    rule.setMaximumValue("10");

    ValidatorMeta meta = createMeta(rule);
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));
    Validator validator = createInitializedValidator(meta, rowMeta, true);

    doReturn(new Object[] {11L}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("amount"),
            eq("KVD006"));
  }

  @Test
  void testOnlyNullAllowedViolation() throws Exception {
    Validation rule = new Validation();
    rule.setName("only null");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setOnlyNullAllowed(true);

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"x"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD015"));
  }

  @Test
  void testUnexpectedDataTypeViolation() throws Exception {
    Validation rule = new Validation();
    rule.setName("type");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setDataType("Integer");
    rule.setDataTypeVerified(true);

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"abc"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD004"));
  }

  @Test
  void testAllowedValueInListPasses() throws Exception {
    Validation rule = new Validation();
    rule.setName("list");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setDataType("String");
    rule.setAllowedValues(List.of("A", "B"));

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"A"}).doReturn(null).when(validator).getRow();
    doNothing().when(validator).putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(validator.processRow());
    verify(validator).putRow(any(IRowMeta.class), eq(new Object[] {"A"}));
    verify(validator, never())
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            anyString(),
            anyString());
  }

  @Test
  void testDisallowedRegularExpression() throws Exception {
    Validation rule = new Validation();
    rule.setName("bad regex");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setRegularExpressionNotAllowed("^[0-9]+$");

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"123"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD014"));
  }

  @Test
  void testStartsWithNotAllowedString() throws Exception {
    Validation rule = new Validation();
    rule.setName("bad start");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setStartStringNotAllowed("XX");

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"XXY"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD011"));
  }

  @Test
  void testEndsWithNotAllowedString() throws Exception {
    Validation rule = new Validation();
    rule.setName("bad end");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setEndStringNotAllowed("YY");

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"aYY"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            eq("KVD012"));
  }

  @Test
  void testValidateAllWithoutConcatenateEmitsMultipleErrors() throws Exception {
    Validation lengthRule = new Validation();
    lengthRule.setName("length");
    lengthRule.setFieldName("value");
    lengthRule.setNullAllowed(true);
    lengthRule.setMaximumLength("1");

    Validation onlyNull = new Validation();
    onlyNull.setName("only null");
    onlyNull.setFieldName("value");
    onlyNull.setNullAllowed(true);
    onlyNull.setOnlyNullAllowed(true);

    ValidatorMeta meta = createMeta(lengthRule, onlyNull);
    meta.setValidatingAll(true);
    meta.setConcatenatingErrors(false);

    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);
    doReturn(new Object[] {"ab"}).when(validator).getRow();
    stubPutError(validator);

    assertTrue(validator.processRow());
    verify(validator, times(2))
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            eq(1L),
            anyString(),
            eq("value"),
            anyString());
  }

  @Test
  void testNullInputEndsProcessing() throws Exception {
    ValidatorMeta meta = createMeta(nullNotAllowed());
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(null).when(validator).getRow();

    assertFalse(validator.processRow());
  }

  @Test
  void testMissingFieldNameFailsInitCalculation() throws Exception {
    Validation rule = new Validation();
    rule.setName("missing field");
    rule.setFieldName("does_not_exist");
    rule.setNullAllowed(true);

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"x"}).when(validator).getRow();

    HopException exception = assertThrows(HopException.class, validator::processRow);
    assertTrue(exception.getMessage().contains("does_not_exist"));
  }

  @Test
  void testEmptyFieldNameFailsOnProcess() throws Exception {
    Validation rule = new Validation();
    rule.setName("empty field name");
    rule.setFieldName("");
    rule.setNullAllowed(true);

    ValidatorMeta meta = createMeta(rule);
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    when(helper.pipelineMeta.getTransformFields(any(IVariables.class), anyString()))
        .thenReturn(stringRowMeta());

    ValidatorData data = new ValidatorData();
    Validator validator =
        spy(
            new Validator(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    validator.setInputRowMeta(stringRowMeta());
    assertTrue(validator.init());

    doReturn(new Object[] {"x"}).when(validator).getRow();

    HopException exception = assertThrows(HopException.class, validator::processRow);
    assertTrue(exception.getMessage().toLowerCase().contains("no name"));
  }

  @Test
  void testInitFailsForInvalidMinimumLength() throws Exception {
    Validation rule = new Validation();
    rule.setName("bad length");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setMinimumLength("abc");

    ValidatorMeta meta = createMeta(rule);
    when(helper.pipelineMeta.getTransformFields(any(IVariables.class), anyString()))
        .thenReturn(stringRowMeta());

    Validator validator =
        new Validator(
            helper.transformMeta,
            meta,
            new ValidatorData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);

    assertFalse(validator.init());
  }

  @Test
  void testValidStartAndEndStringPasses() throws Exception {
    Validation rule = new Validation();
    rule.setName("prefix-suffix");
    rule.setFieldName("value");
    rule.setNullAllowed(true);
    rule.setStartString("AB");
    rule.setEndString("Z");

    ValidatorMeta meta = createMeta(rule);
    Validator validator = createInitializedValidator(meta, stringRowMeta(), true);

    doReturn(new Object[] {"ABZ"}).doReturn(null).when(validator).getRow();
    doNothing().when(validator).putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(validator.processRow());
    verify(validator).putRow(any(IRowMeta.class), eq(new Object[] {"ABZ"}));
  }

  private Validator createInitializedValidator(
      ValidatorMeta meta, RowMeta rowMeta, boolean errorHandling) throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(errorHandling);
    when(helper.pipelineMeta.getTransformFields(any(IVariables.class), anyString()))
        .thenReturn(rowMeta);

    ValidatorData data = new ValidatorData();
    Validator validator =
        spy(
            new Validator(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    validator.setInputRowMeta(rowMeta);

    assertTrue(validator.init());
    return validator;
  }

  private static void stubPutError(Validator validator) throws HopException {
    doNothing()
        .when(validator)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            nullable(String.class),
            anyString());
  }

  private static ValidatorMeta createMeta(Validation... validations) {
    ValidatorMeta meta = new ValidatorMeta();
    meta.setValidations(new ArrayList<>(List.of(validations)));
    return meta;
  }

  private static Validation nullNotAllowed() {
    Validation rule = new Validation();
    rule.setName("value" + " null check");
    rule.setFieldName("value");
    rule.setNullAllowed(false);
    return rule;
  }

  private static RowMeta stringRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("value"));
    return rowMeta;
  }
}
