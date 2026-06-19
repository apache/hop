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

package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaSerializable;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FieldHelperTest {

  @BeforeEach
  void setUpStaticMocks() {
    // Do nothing
  }

  @AfterEach
  void tearDownStaticMocks() {
    // Do nothing
  }

  @Test
  void getNativeDataTypeSimpleName_Unknown() throws Exception {
    HopValueException e = new HopValueException();

    IValueMeta v = mock(IValueMeta.class);
    doThrow(e).when(v).getNativeDataTypeClass();

    LogChannel log = mock(LogChannel.class);

    assertEquals("Object", FieldHelper.getNativeDataTypeSimpleName(v));
    //    verify(log, times(1)).logDebug("Unable to get name from data type");
  }

  @Test
  void getNativeDataTypeSimpleName_String() {
    ValueMetaString v = new ValueMetaString();
    assertEquals("String", FieldHelper.getNativeDataTypeSimpleName(v));
  }

  @Test
  void getNativeDataTypeSimpleName_InetAddress() {
    ValueMetaInternetAddress v = new ValueMetaInternetAddress();
    assertEquals("InetAddress", FieldHelper.getNativeDataTypeSimpleName(v));
  }

  @Test
  void getNativeDataTypeSimpleName_Timestamp() {
    ValueMetaTimestamp v = new ValueMetaTimestamp();
    assertEquals("Timestamp", FieldHelper.getNativeDataTypeSimpleName(v));
  }

  @Test
  void getNativeDataTypeSimpleName_Binary() {
    ValueMetaBinary v = new ValueMetaBinary();
    assertEquals("Binary", FieldHelper.getNativeDataTypeSimpleName(v));
  }

  @Test
  void getGetSignature_String() {
    ValueMetaString v = new ValueMetaString("Name");
    String accessor = FieldHelper.getAccessor(true, "Name");

    assertEquals(
        "String Name = get(Fields.In, \"Name\").getString(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_InetAddress() {
    ValueMetaInternetAddress v = new ValueMetaInternetAddress("IP");
    String accessor = FieldHelper.getAccessor(true, "IP");

    assertEquals(
        "InetAddress IP = get(Fields.In, \"IP\").getInetAddress(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_Timestamp() {
    ValueMetaTimestamp v = new ValueMetaTimestamp("TS");
    String accessor = FieldHelper.getAccessor(true, "TS");

    assertEquals(
        "Timestamp TS = get(Fields.In, \"TS\").getTimestamp(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_Binary() {
    ValueMetaBinary v = new ValueMetaBinary("Data");
    String accessor = FieldHelper.getAccessor(true, "Data");

    assertEquals(
        "byte[] Data = get(Fields.In, \"Data\").getBinary(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_BigNumber() {
    ValueMetaBigNumber v = new ValueMetaBigNumber("Number");
    String accessor = FieldHelper.getAccessor(true, "Number");

    assertEquals(
        "BigDecimal Number = get(Fields.In, \"Number\").getBigDecimal(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_Boolean() {
    ValueMetaBoolean v = new ValueMetaBoolean("Value");
    String accessor = FieldHelper.getAccessor(true, "Value");

    assertEquals(
        "Boolean Value = get(Fields.In, \"Value\").getBoolean(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_Date() {
    ValueMetaDate v = new ValueMetaDate("DT");
    String accessor = FieldHelper.getAccessor(true, "DT");

    assertEquals(
        "Date DT = get(Fields.In, \"DT\").getDate(r);", FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_Integer() {
    ValueMetaInteger v = new ValueMetaInteger("Value");
    String accessor = FieldHelper.getAccessor(true, "Value");

    assertEquals(
        "Long Value = get(Fields.In, \"Value\").getLong(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_Number() {
    ValueMetaNumber v = new ValueMetaNumber("Value");
    String accessor = FieldHelper.getAccessor(true, "Value");

    assertEquals(
        "Double Value = get(Fields.In, \"Value\").getDouble(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getGetSignature_Serializable() throws Exception {
    LogChannel log = mock(LogChannel.class);

    ValueMetaSerializable v = new ValueMetaSerializable("Data");
    String accessor = FieldHelper.getAccessor(true, "Data");

    assertEquals(
        "Object Data = get(Fields.In, \"Data\").getObject(r);",
        FieldHelper.getGetSignature(accessor, v));
  }

  @Test
  void getInetAddress_Test() throws Exception {
    ValueMetaInternetAddress v = new ValueMetaInternetAddress("IP");

    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertEquals(
        InetAddress.getLoopbackAddress(),
        new FieldHelper(row, "IP").getInetAddress(new Object[] {InetAddress.getLoopbackAddress()}));
  }

  @Test
  void getTimestamp_Test() throws Exception {
    ValueMetaTimestamp v = new ValueMetaTimestamp("TS");

    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertEquals(
        Timestamp.valueOf("2018-07-23 12:40:55"),
        new FieldHelper(row, "TS")
            .getTimestamp(new Object[] {Timestamp.valueOf("2018-07-23 12:40:55")}));
  }

  @Test
  void getSerializable_Test() throws Exception {
    ValueMetaSerializable v = new ValueMetaSerializable("Data");

    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertEquals("...", new FieldHelper(row, "Data").getSerializable(new Object[] {"..."}));
  }

  @Test
  void getBinary_Test() throws Exception {
    ValueMetaBinary v = new ValueMetaBinary("Data");

    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertArrayEquals(
        new byte[] {0, 1, 2},
        new FieldHelper(row, "Data").getBinary(new Object[] {new byte[] {0, 1, 2}}));
  }

  @Test
  void setValue_String() {
    ValueMetaString v = new ValueMetaString("Name");

    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    Object[] data = new Object[1];
    new FieldHelper(row, "Name").setValue(data, "Hop");

    assertEquals("Hop", data[0]);
  }

  @Test
  void setValue_InetAddress() throws Exception {
    ValueMetaInternetAddress v = new ValueMetaInternetAddress("IP");

    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    Object[] data = new Object[1];
    new FieldHelper(row, "IP").setValue(data, InetAddress.getLoopbackAddress());

    assertEquals(InetAddress.getLoopbackAddress(), data[0]);
  }

  @Test
  void setValue_ValueMetaBinary() throws Exception {
    ValueMetaBinary v = new ValueMetaBinary("Data");

    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    Object[] data = new Object[1];
    new FieldHelper(row, "Data").setValue(data, new byte[] {0, 1, 2});

    assertArrayEquals(new byte[] {0, 1, 2}, (byte[]) data[0]);
  }

  // ------------------------------------------------------------------ constructor failure

  @Test
  void constructor_fieldNotFound_throwsIllegalArgumentException() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("existing"));
    assertThrows(IllegalArgumentException.class, () -> new FieldHelper(rowMeta, "missing"));
  }

  // ------------------------------------------------------------------ typed getters

  @Test
  void getObject_returnsRawValue() {
    ValueMetaString v = new ValueMetaString("name");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    Object[] data = {"hello"};
    assertEquals("hello", new FieldHelper(row, "name").getObject(data));
  }

  @Test
  void getBoolean_returnsValue() throws HopValueException {
    ValueMetaBoolean v = new ValueMetaBoolean("flag");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertEquals(
        Boolean.TRUE, new FieldHelper(row, "flag").getBoolean(new Object[] {Boolean.TRUE}));
  }

  @Test
  void getLong_returnsValue() throws HopValueException {
    ValueMetaInteger v = new ValueMetaInteger("num");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertEquals(42L, new FieldHelper(row, "num").getLong(new Object[] {42L}));
  }

  @Test
  void getDouble_returnsValue() throws HopValueException {
    ValueMetaNumber v = new ValueMetaNumber("dbl");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertEquals(3.14, new FieldHelper(row, "dbl").getDouble(new Object[] {3.14}));
  }

  @Test
  void getString_returnsValue() throws HopValueException {
    ValueMetaString v = new ValueMetaString("s");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertEquals("world", new FieldHelper(row, "s").getString(new Object[] {"world"}));
  }

  @Test
  void getBigDecimal_returnsValue() throws HopValueException {
    ValueMetaBigNumber v = new ValueMetaBigNumber("bd");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    BigDecimal bd = new BigDecimal("123.456");
    assertEquals(bd, new FieldHelper(row, "bd").getBigDecimal(new Object[] {bd}));
  }

  @Test
  void getDate_returnsValue() throws HopValueException {
    ValueMetaDate v = new ValueMetaDate("dt");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    Date d = new Date(0L);
    assertEquals(d, new FieldHelper(row, "dt").getDate(new Object[] {d}));
  }

  // ------------------------------------------------------------------ getValueMeta / indexOfValue

  @Test
  void getValueMeta_returnsStoredMeta() {
    ValueMetaString v = new ValueMetaString("col");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(0).when(row).indexOfValue(anyString());

    assertNotNull(new FieldHelper(row, "col").getValueMeta());
    assertEquals(v, new FieldHelper(row, "col").getValueMeta());
  }

  @Test
  void indexOfValue_returnsStoredIndex() {
    ValueMetaString v = new ValueMetaString("col");
    IRowMeta row = mock(IRowMeta.class);
    doReturn(v).when(row).searchValueMeta(anyString());
    doReturn(2).when(row).indexOfValue(anyString());

    assertEquals(2, new FieldHelper(row, "col").indexOfValue());
  }

  // ------------------------------------------------------------------ getAccessor Out variant

  @Test
  void getAccessor_outVariant_containsFieldsOut() {
    String accessor = FieldHelper.getAccessor(false, "myField");
    assertEquals("get(Fields.Out, \"myField\")", accessor);
  }

  // ------------------------------------------------------------------ getGetSignature: invalid
  // Java identifier → "value"

  @Test
  void getGetSignature_invalidJavaIdentifier_usesValueAsLocalName() {
    ValueMetaString v = new ValueMetaString("123invalid");
    String accessor = FieldHelper.getAccessor(true, "123invalid");
    String sig = FieldHelper.getGetSignature(accessor, v);
    // Local variable name should be "value" when name is not a valid Java identifier
    assertEquals("String value = get(Fields.In, \"123invalid\").getString(r);", sig);
  }

  // ------------------------------------------------------------------ getNativeDataTypeSimpleName:
  // remaining types

  @Test
  void getNativeDataTypeSimpleName_Boolean() {
    ValueMetaBoolean v = new ValueMetaBoolean();
    assertEquals("Boolean", FieldHelper.getNativeDataTypeSimpleName(v));
  }

  @Test
  void getNativeDataTypeSimpleName_Integer() {
    ValueMetaInteger v = new ValueMetaInteger();
    assertEquals("Long", FieldHelper.getNativeDataTypeSimpleName(v));
  }

  @Test
  void getNativeDataTypeSimpleName_Number() {
    ValueMetaNumber v = new ValueMetaNumber();
    assertEquals("Double", FieldHelper.getNativeDataTypeSimpleName(v));
  }

  @Test
  void getNativeDataTypeSimpleName_BigNumber() {
    ValueMetaBigNumber v = new ValueMetaBigNumber();
    assertEquals("BigDecimal", FieldHelper.getNativeDataTypeSimpleName(v));
  }
}
