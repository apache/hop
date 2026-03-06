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

package org.apache.hop.core.row.value;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link ValueMetaFactory} */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaFactoryTest {

  @BeforeAll
  static void beforeClassSetUp() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void testClone() throws HopException {
    IValueMeta original = new ValueMetaString();
    original.setCollatorLocale(Locale.CANADA);
    original.setCollatorStrength(3);
    IValueMeta cloned = ValueMetaFactory.cloneValueMeta(original);
    assertNotNull(cloned);
    assertNotSame(original, cloned);
    valueMetaDeepEquals(original, cloned);
  }

  private static void valueMetaDeepEquals(IValueMeta expected, IValueMeta actual) {
    if (expected == null && actual == null) {
      return;
    }
    assertNotNull(expected);

    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getLength(), actual.getLength());
    assertEquals(expected.getPrecision(), actual.getPrecision());
    assertEquals(expected.getConversionMask(), actual.getConversionMask());
    assertEquals(expected.getDecimalSymbol(), actual.getDecimalSymbol());
    assertEquals(expected.getGroupingSymbol(), actual.getGroupingSymbol());
    assertEquals(expected.getStorageType(), actual.getStorageType());
    assertEquals(expected.getStringEncoding(), actual.getStringEncoding());
    assertEquals(expected.getTrimType(), actual.getTrimType());
    assertEquals(expected.isDateFormatLenient(), actual.isDateFormatLenient());
    assertEquals(expected.getDateFormatLocale(), actual.getDateFormatLocale());
    assertEquals(expected.getDateFormatTimeZone(), actual.getDateFormatTimeZone());
    assertEquals(expected.isLenientStringToNumber(), actual.isLenientStringToNumber());
    assertEquals(expected.isLargeTextField(), actual.isLargeTextField());
    assertEquals(expected.getComments(), actual.getComments());
    assertEquals(expected.isCaseInsensitive(), actual.isCaseInsensitive());
    assertEquals(expected.isCollatorDisabled(), actual.isCollatorDisabled());
    assertEquals(expected.getCollatorStrength(), actual.getCollatorStrength());
    assertArrayEquals(expected.getIndex(), actual.getIndex());
    assertEquals(expected.getOrigin(), actual.getOrigin());
    assertEquals(expected.isOriginalAutoIncrement(), actual.isOriginalAutoIncrement());
    assertEquals(expected.getOriginalColumnType(), actual.getOriginalColumnType());
    assertEquals(expected.getOriginalColumnTypeName(), actual.getOriginalColumnTypeName());
    assertEquals(expected.isOriginalNullable(), actual.isOriginalNullable());
    assertEquals(expected.getOriginalPrecision(), actual.getOriginalPrecision());
    assertEquals(expected.getOriginalScale(), actual.getOriginalScale());
    assertEquals(expected.isOriginalSigned(), actual.isOriginalSigned());
    valueMetaDeepEquals(expected.getStorageMetadata(), actual.getStorageMetadata());
  }

  @Test
  void testCreateValueMeta() throws HopPluginException {
    IValueMeta testObject;

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_NONE);
    assertInstanceOf(ValueMetaNone.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testNone", IValueMeta.TYPE_NONE);
    assertInstanceOf(ValueMetaNone.class, testObject);
    assertEquals("testNone", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testNone", IValueMeta.TYPE_NONE, 10, 20);
    assertInstanceOf(ValueMetaNone.class, testObject);
    assertEquals("testNone", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(20, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_NUMBER);
    assertInstanceOf(ValueMetaNumber.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testCreateValueMeta_1(testObject);
    testCreateValueMeta_2(testObject);
    testCreateValueMeta_3(testObject);
    testCreateValueMeta_4(testObject);
    testCreateValueMeta_5(testObject);
    testCreateValueMeta_6(testObject);
  }

  private void testCreateValueMeta_1(IValueMeta testObject) throws HopPluginException {
    testObject =
        ValueMetaFactory.createValueMeta("testTimestamp", IValueMeta.TYPE_TIMESTAMP, 10, 20);
    assertInstanceOf(ValueMetaTimestamp.class, testObject);
    assertEquals("testTimestamp", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(20, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_INET);
    assertInstanceOf(ValueMetaInternetAddress.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testInternetAddress", IValueMeta.TYPE_INET);
    assertInstanceOf(ValueMetaInternetAddress.class, testObject);
    assertEquals("testInternetAddress", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject =
        ValueMetaFactory.createValueMeta("testInternetAddress", IValueMeta.TYPE_INET, 10, 20);
    assertInstanceOf(ValueMetaInternetAddress.class, testObject);
    assertEquals("testInternetAddress", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(20, testObject.getPrecision());
  }

  private void testCreateValueMeta_2(IValueMeta testObject) throws HopPluginException {
    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_BINARY);
    assertInstanceOf(ValueMetaBinary.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(0, testObject.getPrecision()); // Special case for Binary

    testObject = ValueMetaFactory.createValueMeta("testBinary", IValueMeta.TYPE_BINARY);
    assertInstanceOf(ValueMetaBinary.class, testObject);
    assertEquals("testBinary", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(0, testObject.getPrecision()); // Special case for Binary

    testObject = ValueMetaFactory.createValueMeta("testBinary", IValueMeta.TYPE_BINARY, 10, 20);
    assertInstanceOf(ValueMetaBinary.class, testObject);
    assertEquals("testBinary", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(0, testObject.getPrecision()); // Special case for Binary

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_TIMESTAMP);
    assertInstanceOf(ValueMetaTimestamp.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testTimestamp", IValueMeta.TYPE_TIMESTAMP);
    assertInstanceOf(ValueMetaTimestamp.class, testObject);
    assertEquals("testTimestamp", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());
  }

  private void testCreateValueMeta_3(IValueMeta testObject) throws HopPluginException {
    testObject = ValueMetaFactory.createValueMeta("testBigNumber", IValueMeta.TYPE_BIGNUMBER);
    assertInstanceOf(ValueMetaBigNumber.class, testObject);
    assertEquals("testBigNumber", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject =
        ValueMetaFactory.createValueMeta("testBigNumber", IValueMeta.TYPE_BIGNUMBER, 10, 20);
    assertInstanceOf(ValueMetaBigNumber.class, testObject);
    assertEquals("testBigNumber", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(20, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_SERIALIZABLE);
    assertInstanceOf(ValueMetaSerializable.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testSerializable", IValueMeta.TYPE_SERIALIZABLE);
    assertInstanceOf(ValueMetaSerializable.class, testObject);
    assertEquals("testSerializable", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject =
        ValueMetaFactory.createValueMeta("testSerializable", IValueMeta.TYPE_SERIALIZABLE, 10, 20);
    assertInstanceOf(ValueMetaSerializable.class, testObject);
    assertEquals("testSerializable", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(20, testObject.getPrecision());
  }

  private void testCreateValueMeta_4(IValueMeta testObject) throws HopPluginException {
    testObject = ValueMetaFactory.createValueMeta("testBoolean", IValueMeta.TYPE_BOOLEAN, 10, 20);
    assertInstanceOf(ValueMetaBoolean.class, testObject);
    assertEquals("testBoolean", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_INTEGER);
    assertInstanceOf(ValueMetaInteger.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(0, testObject.getPrecision()); // Special case for Integer

    testObject = ValueMetaFactory.createValueMeta("testInteger", IValueMeta.TYPE_INTEGER);
    assertInstanceOf(ValueMetaInteger.class, testObject);
    assertEquals("testInteger", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(0, testObject.getPrecision()); // Special case for Integer

    testObject = ValueMetaFactory.createValueMeta("testInteger", IValueMeta.TYPE_INTEGER, 10, 20);
    assertInstanceOf(ValueMetaInteger.class, testObject);
    assertEquals("testInteger", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(0, testObject.getPrecision()); // Special case for Integer

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_BIGNUMBER);
    assertInstanceOf(ValueMetaBigNumber.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());
  }

  private void testCreateValueMeta_5(IValueMeta testObject) throws HopPluginException {
    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_DATE);
    assertInstanceOf(ValueMetaDate.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testDate", IValueMeta.TYPE_DATE);
    assertInstanceOf(ValueMetaDate.class, testObject);
    assertEquals("testDate", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testDate", IValueMeta.TYPE_DATE, 10, 20);
    assertInstanceOf(ValueMetaDate.class, testObject);
    assertEquals("testDate", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(20, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_BOOLEAN);
    assertInstanceOf(ValueMetaBoolean.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testBoolean", IValueMeta.TYPE_BOOLEAN);
    assertInstanceOf(ValueMetaBoolean.class, testObject);
    assertEquals("testBoolean", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());
  }

  private void testCreateValueMeta_6(IValueMeta testObject) throws HopPluginException {
    testObject = ValueMetaFactory.createValueMeta("testNumber", IValueMeta.TYPE_NUMBER);
    assertInstanceOf(ValueMetaNumber.class, testObject);
    assertEquals("testNumber", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testNumber", IValueMeta.TYPE_NUMBER, 10, 20);
    assertInstanceOf(ValueMetaNumber.class, testObject);
    assertEquals("testNumber", testObject.getName());
    assertEquals(10, testObject.getLength());
    assertEquals(20, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING);
    assertInstanceOf(ValueMetaString.class, testObject);
    assertNull(testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testString", IValueMeta.TYPE_STRING);
    assertInstanceOf(ValueMetaString.class, testObject);
    assertEquals("testString", testObject.getName());
    assertEquals(-1, testObject.getLength());
    assertEquals(-1, testObject.getPrecision());

    testObject = ValueMetaFactory.createValueMeta("testString", IValueMeta.TYPE_STRING, 1000, 50);
    assertInstanceOf(ValueMetaString.class, testObject);
    assertEquals("testString", testObject.getName());
    assertEquals(1000, testObject.getLength());
    assertEquals(-1, testObject.getPrecision()); // Special case for String
  }

  @Test
  void testGetValueMetaNames() {
    List<String> dataTypes = Arrays.<String>asList(ValueMetaFactory.getValueMetaNames());

    assertTrue(dataTypes.contains("Number"));
    assertTrue(dataTypes.contains("String"));
    assertTrue(dataTypes.contains("Date"));
    assertTrue(dataTypes.contains("Boolean"));
    assertTrue(dataTypes.contains("Integer"));
    assertTrue(dataTypes.contains("BigNumber"));
    assertFalse(dataTypes.contains("Serializable"));
    assertTrue(dataTypes.contains("Binary"));
    assertTrue(dataTypes.contains("Timestamp"));
    assertTrue(dataTypes.contains("Internet Address"));
  }

  @Test
  void testGetAllValueMetaNames() {
    List<String> dataTypes = Arrays.<String>asList(ValueMetaFactory.getAllValueMetaNames());

    assertTrue(dataTypes.contains("Number"));
    assertTrue(dataTypes.contains("String"));
    assertTrue(dataTypes.contains("Date"));
    assertTrue(dataTypes.contains("Boolean"));
    assertTrue(dataTypes.contains("Integer"));
    assertTrue(dataTypes.contains("BigNumber"));
    assertTrue(dataTypes.contains("Serializable"));
    assertTrue(dataTypes.contains("Binary"));
    assertTrue(dataTypes.contains("Timestamp"));
    assertTrue(dataTypes.contains("Internet Address"));
  }

  @Test
  void testGetValueMetaName() {
    assertEquals("-", ValueMetaFactory.getValueMetaName(Integer.MIN_VALUE));
    assertEquals("None", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NONE));
    assertEquals("Number", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NUMBER));
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals("Date", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_DATE));
    assertEquals("Boolean", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_BOOLEAN));
    assertEquals("Integer", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INTEGER));
    assertEquals("BigNumber", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_BIGNUMBER));
    assertEquals("Serializable", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_SERIALIZABLE));
    assertEquals("Binary", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_BINARY));
    assertEquals("Timestamp", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_TIMESTAMP));
    assertEquals("Internet Address", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INET));
  }

  @Test
  void testGetIdForValueMeta() {
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(null));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(""));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("None"));
    assertEquals(IValueMeta.TYPE_NUMBER, ValueMetaFactory.getIdForValueMeta("Number"));
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("String"));
    assertEquals(IValueMeta.TYPE_DATE, ValueMetaFactory.getIdForValueMeta("Date"));
    assertEquals(IValueMeta.TYPE_BOOLEAN, ValueMetaFactory.getIdForValueMeta("Boolean"));
    assertEquals(IValueMeta.TYPE_INTEGER, ValueMetaFactory.getIdForValueMeta("Integer"));
    assertEquals(IValueMeta.TYPE_BIGNUMBER, ValueMetaFactory.getIdForValueMeta("BigNumber"));
    assertEquals(IValueMeta.TYPE_SERIALIZABLE, ValueMetaFactory.getIdForValueMeta("Serializable"));
    assertEquals(IValueMeta.TYPE_BINARY, ValueMetaFactory.getIdForValueMeta("Binary"));
    assertEquals(IValueMeta.TYPE_TIMESTAMP, ValueMetaFactory.getIdForValueMeta("Timestamp"));
    assertEquals(IValueMeta.TYPE_INET, ValueMetaFactory.getIdForValueMeta("Internet Address"));
  }

  @Test
  void testGetValueMetaPluginClasses() throws HopPluginException {
    List<IValueMeta> dataTypes = ValueMetaFactory.getValueMetaPluginClasses();

    boolean numberExists = false;
    boolean stringExists = false;
    boolean dateExists = false;
    boolean booleanExists = false;
    boolean integerExists = false;
    boolean bignumberExists = false;
    boolean serializableExists = false;
    boolean binaryExists = false;
    boolean timestampExists = false;
    boolean inetExists = false;

    for (IValueMeta obj : dataTypes) {
      if (obj instanceof ValueMetaNumber) {
        numberExists = true;
      }
      if (obj.getClass().equals(ValueMetaString.class)) {
        stringExists = true;
      }
      if (obj.getClass().equals(ValueMetaDate.class)) {
        dateExists = true;
      }
      if (obj.getClass().equals(ValueMetaBoolean.class)) {
        booleanExists = true;
      }
      if (obj.getClass().equals(ValueMetaInteger.class)) {
        integerExists = true;
      }
      if (obj.getClass().equals(ValueMetaBigNumber.class)) {
        bignumberExists = true;
      }
      if (obj.getClass().equals(ValueMetaSerializable.class)) {
        serializableExists = true;
      }
      if (obj.getClass().equals(ValueMetaBinary.class)) {
        binaryExists = true;
      }
      if (obj.getClass().equals(ValueMetaTimestamp.class)) {
        timestampExists = true;
      }
      if (obj.getClass().equals(ValueMetaInternetAddress.class)) {
        inetExists = true;
      }
    }

    assertTrue(numberExists);
    assertTrue(stringExists);
    assertTrue(dateExists);
    assertTrue(booleanExists);
    assertTrue(integerExists);
    assertTrue(bignumberExists);
    assertTrue(serializableExists);
    assertTrue(binaryExists);
    assertTrue(timestampExists);
    assertTrue(inetExists);
  }

  @Test
  void testGuessValueMetaInterface() {
    assertInstanceOf(
        ValueMetaBigNumber.class, ValueMetaFactory.guessValueMetaInterface(new BigDecimal("1.0")));
    assertInstanceOf(ValueMetaNumber.class, ValueMetaFactory.guessValueMetaInterface(1.0));
    assertInstanceOf(ValueMetaInteger.class, ValueMetaFactory.guessValueMetaInterface(1L));
    assertInstanceOf(ValueMetaString.class, ValueMetaFactory.guessValueMetaInterface(""));
    assertInstanceOf(ValueMetaDate.class, ValueMetaFactory.guessValueMetaInterface(new Date()));
    assertInstanceOf(
        ValueMetaBoolean.class, ValueMetaFactory.guessValueMetaInterface(Boolean.FALSE));
    assertInstanceOf(
        ValueMetaBoolean.class, ValueMetaFactory.guessValueMetaInterface(Boolean.TRUE));
    assertInstanceOf(ValueMetaBoolean.class, ValueMetaFactory.guessValueMetaInterface(false));
    assertInstanceOf(ValueMetaBoolean.class, ValueMetaFactory.guessValueMetaInterface(true));
    assertInstanceOf(ValueMetaBinary.class, ValueMetaFactory.guessValueMetaInterface(new byte[10]));

    // Test Unsupported Data Types
    assertNull(ValueMetaFactory.guessValueMetaInterface(null));
    assertNull(ValueMetaFactory.guessValueMetaInterface((short) 1));
    assertNull(ValueMetaFactory.guessValueMetaInterface((byte) 1));
    assertNull(ValueMetaFactory.guessValueMetaInterface(1.0F));
    assertNull(ValueMetaFactory.guessValueMetaInterface(new StringBuilder()));
    assertNull(ValueMetaFactory.guessValueMetaInterface((byte) 1));
  }

  @Test
  void testGetNativeDataTypeClass() throws HopPluginException {
    for (String valueMetaName : ValueMetaFactory.getValueMetaNames()) {
      int valueMetaID = ValueMetaFactory.getIdForValueMeta(valueMetaName);
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(valueMetaID);
      try {
        Class<?> clazz = valueMeta.getNativeDataTypeClass();
        assertNotNull(clazz);
      } catch (HopValueException kve) {
        fail(valueMetaName + " should implement getNativeDataTypeClass()");
      }
    }
  }
}
