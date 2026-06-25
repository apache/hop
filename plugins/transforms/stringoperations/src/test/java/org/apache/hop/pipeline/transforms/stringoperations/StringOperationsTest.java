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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hop.core.Const;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Unit tests for {@link StringOperations}. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class StringOperationsTest {
  private TransformMockHelper<StringOperationsMeta, StringOperationsData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "STRING_OPERATIONS", StringOperationsMeta.class, StringOperationsData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  private static StringOperationsMeta.StringOperation operation(
      String inField, String outField, StringOperationsMeta.TrimType trimType) {
    StringOperationsMeta.StringOperation op = new StringOperationsMeta.StringOperation();
    op.setFieldInStream(inField);
    op.setFieldOutStream(outField);
    op.setTrimType(trimType);
    return op;
  }

  private static StringOperationsMeta.StringOperation operationWithDefaults(String inField) {
    return operation(inField, "", StringOperationsMeta.TrimType.NONE);
  }

  private Object[] processRows(StringOperationsMeta meta, RowMeta inputRowMeta, Object[] inputRow)
      throws HopException {
    StringOperationsData data = new StringOperationsData();
    StringOperations transform =
        new StringOperations(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.setInputRowMeta(inputRowMeta);
    transform.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    boolean hasMoreRows;
    do {
      hasMoreRows = transform.processRow();
    } while (hasMoreRows);

    return outputRowSet.getRow();
  }

  @Test
  void processRowReturnsFalseWhenNoInput() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsData data = new StringOperationsData();
    StringOperations transform =
        new StringOperations(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.addRowSetToInputRowSets(mockHelper.getMockInputRowSet());

    assertFalse(transform.processRow());
  }

  @Test
  void trimsBothSidesInPlace() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    meta.getOperations().add(operation("text", "", StringOperationsMeta.TrimType.BOTH));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"  hello  "});

    assertEquals("hello", output[0]);
  }

  @Test
  void convertsToUpperCaseInNewField() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    op.setLowerUpper(StringOperationsMeta.LowerUpper.UPPER);
    op.setFieldOutStream("upper_text");
    meta.getOperations().add(op);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"hello"});

    assertEquals("hello", output[0]);
    assertEquals("HELLO", output[1]);
  }

  @Test
  void leftPadsToConfiguredLengthInNewField() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    op.setPaddingType(StringOperationsMeta.Padding.LEFT);
    op.setPadChar("#");
    op.setPadLen("8");
    op.setFieldOutStream("padded");
    meta.getOperations().add(op);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"abc"});

    assertEquals("abc", output[0]);
    assertEquals("#####abc", output[1]);
  }

  @Test
  void appliesInitCapInPlace() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    op.setInitCap(StringOperationsMeta.InitCap.YES);
    meta.getOperations().add(op);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"hello world"});

    assertEquals(ValueDataUtil.initCap("hello world"), output[0]);
  }

  @Test
  void escapesXmlInNewField() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    op.setMaskXml(StringOperationsMeta.MaskXml.ESCAPE_XML);
    op.setFieldOutStream("escaped");
    meta.getOperations().add(op);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"a<b"});

    assertEquals("a<b", output[0]);
    assertEquals(Const.escapeXml("a<b"), output[1]);
  }

  @Test
  void keepsDigitsOnlyInNewField() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    op.setDigits(StringOperationsMeta.Digits.DIGITS_ONLY);
    op.setFieldOutStream("digits");
    meta.getOperations().add(op);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"a1b2c3"});

    assertEquals("123", output[1]);
  }

  @Test
  void removesCarriageReturnInPlace() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    op.setRemoveSpecialChars(StringOperationsMeta.RemoveSpecialChars.CR);
    meta.getOperations().add(op);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"a\rb"});

    assertEquals("ab", output[0]);
  }

  @Test
  void appliesMultipleOperationsToDifferentFields() throws HopException {
    StringOperationsMeta meta = new StringOperationsMeta();

    StringOperationsMeta.StringOperation trimOp =
        operation("trimmed", "out_trim", StringOperationsMeta.TrimType.BOTH);
    meta.getOperations().add(trimOp);

    StringOperationsMeta.StringOperation upperOp = operationWithDefaults("upper_src");
    upperOp.setLowerUpper(StringOperationsMeta.LowerUpper.UPPER);
    upperOp.setFieldOutStream("out_upper");
    meta.getOperations().add(upperOp);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("trimmed"));
    inputRowMeta.addValueMeta(new ValueMetaString("upper_src"));

    Object[] output = processRows(meta, inputRowMeta, new Object[] {"  hello  ", "world"});

    assertEquals("  hello  ", output[0]);
    assertEquals("world", output[1]);
    assertEquals("hello", output[2]);
    assertEquals("WORLD", output[3]);
  }

  @Test
  void throwsWhenConfiguredFieldIsMissing() {
    StringOperationsMeta meta = new StringOperationsMeta();
    meta.getOperations().add(operation("missing", "", StringOperationsMeta.TrimType.BOTH));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    assertThrows(
        HopTransformException.class, () -> processRows(meta, inputRowMeta, new Object[] {"value"}));
  }

  @Test
  void throwsWhenConfiguredFieldIsNotString() {
    StringOperationsMeta meta = new StringOperationsMeta();
    meta.getOperations().add(operation("number", "", StringOperationsMeta.TrimType.BOTH));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("number"));

    assertThrows(
        HopTransformException.class, () -> processRows(meta, inputRowMeta, new Object[] {1L}));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.TrimType.class)
  void processStringTrimHandlesAllTrimTypes(StringOperationsMeta.TrimType trimType)
      throws Exception {
    String input = "  value  ";
    String expected =
        switch (trimType) {
          case RIGHT -> Const.rtrim(input);
          case LEFT -> Const.ltrim(input);
          case BOTH -> Const.trim(input);
          case NONE -> input;
        };
    assertEquals(expected, invokeStaticTrim(trimType, input));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.LowerUpper.class)
  void processStringLowerUpperHandlesAllCases(StringOperationsMeta.LowerUpper lowerUpper)
      throws Exception {
    String input = "AbC";
    String expected =
        switch (lowerUpper) {
          case LOWER -> "abc";
          case UPPER -> "ABC";
          case NONE -> input;
        };
    assertEquals(expected, invokeStaticLowerUpper(lowerUpper, input));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.Padding.class)
  void processStringPaddingHandlesAllCases(StringOperationsMeta.Padding padding) throws Exception {
    String input = "abc";
    String expected =
        switch (padding) {
          case LEFT -> Const.lpad(input, "*", 5);
          case RIGHT -> Const.rpad(input, "*", 5);
          case NONE -> input;
        };
    assertEquals(expected, invokeStaticPadding(padding, input));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.InitCap.class)
  void processStringInitCapHandlesAllCases(StringOperationsMeta.InitCap initCap) throws Exception {
    String input = "hello world";
    String expected =
        initCap == StringOperationsMeta.InitCap.YES ? ValueDataUtil.initCap(input) : input;
    assertEquals(expected, invokeStaticInitCap(initCap, input));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.MaskXml.class)
  void processStringMaskXmlHandlesAllCases(StringOperationsMeta.MaskXml maskXml) throws Exception {
    String input = "a<b&\"c";
    String expected =
        switch (maskXml) {
          case NONE -> input;
          case ESCAPE_XML -> Const.escapeXml(input);
          case CDATA -> Const.protectXmlCdata(input);
          case UNESCAPE_XML -> Const.unEscapeXml(input);
          case ESCAPE_HTML -> Const.escapeHtml(input);
          case UNESCAPE_HTML -> Const.unEscapeHtml(input);
          case ESCAPE_SQL -> Const.escapeSql(input);
        };
    assertEquals(expected, invokeStaticMaskXml(maskXml, input));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.Digits.class)
  void processStringDigitsHandlesAllCases(StringOperationsMeta.Digits digits) throws Exception {
    String input = "a1b2";
    String expected =
        switch (digits) {
          case NONE -> input;
          case DIGITS_ONLY -> Const.getDigitsOnly(input);
          case DIGITS_REMOVE -> Const.removeDigits(input);
        };
    assertEquals(expected, invokeStaticDigits(digits, input));
  }

  @ParameterizedTest
  @EnumSource(StringOperationsMeta.RemoveSpecialChars.class)
  void processStringRemoveSpecialCharactersHandlesAllCases(
      StringOperationsMeta.RemoveSpecialChars removeSpecialChars) throws Exception {
    String input = "a \r\n\tb";
    String expected =
        switch (removeSpecialChars) {
          case NONE -> input;
          case CR -> Const.removeCR(input);
          case LF -> Const.removeLF(input);
          case CRLF -> Const.removeCRLF(input);
          case TAB -> Const.removeTAB(input);
          case SPACE -> input.replace(" ", "");
        };
    assertEquals(expected, invokeStaticRemoveSpecialCharacters(removeSpecialChars, input));
  }

  @Test
  void processStringReturnsNullForNullInput() throws Exception {
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    assertNull(invokeProcessString(null, op));
  }

  @Test
  void processStringAppliesOperationsInConfiguredOrder() throws Exception {
    StringOperationsMeta.StringOperation op = operationWithDefaults("text");
    op.setTrimType(StringOperationsMeta.TrimType.BOTH);
    op.setLowerUpper(StringOperationsMeta.LowerUpper.UPPER);
    op.setPaddingType(StringOperationsMeta.Padding.RIGHT);
    op.setPadChar(" ");
    op.setPadLen("10");
    op.setInitCap(StringOperationsMeta.InitCap.YES);
    op.setMaskXml(StringOperationsMeta.MaskXml.NONE);
    op.setDigits(StringOperationsMeta.Digits.DIGITS_REMOVE);
    op.setRemoveSpecialChars(StringOperationsMeta.RemoveSpecialChars.SPACE);

    String input = "  ab12  ";
    String expected =
        Const.removeDigits(
            ValueDataUtil.initCap(Const.rpad(Const.trim(input).toUpperCase(), " ", 10)));

    assertNotNull(expected);
    assertEquals(expected.trim(), invokeProcessString(input, op));
  }

  private String invokeProcessString(String input, StringOperationsMeta.StringOperation operation)
      throws Exception {
    StringOperationsMeta meta = new StringOperationsMeta();
    StringOperationsData data = new StringOperationsData();
    StringOperations transform =
        new StringOperations(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    Method method =
        StringOperations.class.getDeclaredMethod(
            "processString", String.class, StringOperationsMeta.StringOperation.class);
    method.setAccessible(true);
    try {
      return (String) method.invoke(transform, input, operation);
    } catch (InvocationTargetException e) {
      throw new Exception(e.getCause());
    }
  }

  private static String invokeStaticTrim(StringOperationsMeta.TrimType trimType, String input)
      throws Exception {
    return invokeStatic("processStringTrim", trimType, input);
  }

  private static String invokeStaticLowerUpper(
      StringOperationsMeta.LowerUpper lowerUpper, String input) throws Exception {
    return invokeStatic("processStringLowerUpper", lowerUpper, input);
  }

  private static String invokeStaticPadding(StringOperationsMeta.Padding padding, String input)
      throws Exception {
    Method method =
        StringOperations.class.getDeclaredMethod(
            "processStringPadding",
            StringOperationsMeta.Padding.class,
            String.class,
            int.class,
            String.class);
    method.setAccessible(true);
    try {
      return (String) method.invoke(null, padding, "*", 5, input);
    } catch (InvocationTargetException e) {
      throw new Exception(e.getCause());
    }
  }

  private static String invokeStaticInitCap(StringOperationsMeta.InitCap initCap, String input)
      throws Exception {
    return invokeStatic("processStringInitCap", initCap, input);
  }

  private static String invokeStaticMaskXml(StringOperationsMeta.MaskXml maskXml, String input)
      throws Exception {
    return invokeStatic("processStringMaskXml", maskXml, input);
  }

  private static String invokeStaticDigits(StringOperationsMeta.Digits digits, String input)
      throws Exception {
    return invokeStatic("processStringDigits", digits, input);
  }

  private static String invokeStaticRemoveSpecialCharacters(
      StringOperationsMeta.RemoveSpecialChars removeSpecialChars, String input) throws Exception {
    return invokeStatic("processStringRemoveSpecialCharacters", removeSpecialChars, input);
  }

  private static String invokeStatic(String methodName, Object enumValue, String input)
      throws Exception {
    Method method =
        StringOperations.class.getDeclaredMethod(methodName, enumValue.getClass(), String.class);
    method.setAccessible(true);
    try {
      return (String) method.invoke(null, enumValue, input);
    } catch (InvocationTargetException e) {
      throw new Exception(e.getCause());
    }
  }

  @Test
  void nullAndEmptyInputsArePassedThroughByStaticHelpers() throws Exception {
    assertNull(invokeStaticTrim(StringOperationsMeta.TrimType.BOTH, null));
    assertEquals("", invokeStaticTrim(StringOperationsMeta.TrimType.BOTH, ""));
    assertArrayEquals(
        new Object[] {null, "", "   "},
        new Object[] {
          invokeStaticLowerUpper(StringOperationsMeta.LowerUpper.LOWER, null),
          invokeStaticLowerUpper(StringOperationsMeta.LowerUpper.LOWER, ""),
          invokeStaticLowerUpper(StringOperationsMeta.LowerUpper.LOWER, "   ")
        });
  }
}
