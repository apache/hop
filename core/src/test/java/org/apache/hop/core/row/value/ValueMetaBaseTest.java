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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.IHopLoggingEventListener;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.owasp.encoder.Encode;
import org.w3c.dom.Node;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaBaseTest {
  protected static class StoreLoggingEventListener implements IHopLoggingEventListener {
    private final List<HopLoggingEvent> events = new ArrayList<>();

    @Override
    public void eventAdded(HopLoggingEvent event) {
      events.add(event);
    }

    List<HopLoggingEvent> getEvents() {
      return events;
    }
  }

  protected static final String TEST_NAME = "TEST_NAME";
  protected static final String LOG_FIELD = "LOG_FIELD";
  protected static final int MAX_TEXT_FIELD_LEN = 5;

  // Get PKG from class under test
  protected static final Class<?> PKG = ValueMetaBase.PKG;
  protected StoreLoggingEventListener listener;

  @Spy protected DatabaseMeta databaseMetaSpy = spy(new DatabaseMeta());
  protected PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
  protected ResultSet resultSet;
  protected DatabaseMeta dbMeta;
  protected ValueMetaBase valueMetaBase;
  protected IVariables variables;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    listener = new StoreLoggingEventListener();
    HopLogStore.getAppender().addLoggingEventListener(listener);

    valueMetaBase = new ValueMetaBase();
    dbMeta = spy(new DatabaseMeta());
    resultSet = mock(ResultSet.class);

    variables = Variables.getADefaultVariableSpace();
  }

  @AfterEach
  void tearDown() {
    HopLogStore.getAppender().removeLoggingEventListener(listener);
    listener = new StoreLoggingEventListener();
  }

  protected void initValueMeta(BaseDatabaseMeta dbMeta, int length, Object data)
      throws HopDatabaseException {
    IValueMeta valueMetaString = new ValueMetaString(LOG_FIELD, length, 0);
    databaseMetaSpy.setIDatabase(dbMeta);
    valueMetaString.setPreparedStatementValue(databaseMetaSpy, preparedStatementMock, 0, data);
  }

  @Test
  void testDefaultCtor() {
    ValueMetaBase base = new ValueMetaBase();
    assertNotNull(base);
    assertNull(base.getName());
    assertEquals(IValueMeta.TYPE_NONE, base.getType());
  }

  @Test
  void testCtorName() {
    ValueMetaBase base = new ValueMetaBase("myValueMeta");
    assertEquals("myValueMeta", base.getName());
    assertEquals(IValueMeta.TYPE_NONE, base.getType());
    assertNotNull(base.getTypeDesc());
  }

  @Test
  void testCtorNameAndType() {
    IValueMeta base = new ValueMetaString("myStringType");
    assertEquals("myStringType", base.getName());
    assertEquals(IValueMeta.TYPE_STRING, base.getType());
    assertEquals("String", base.getTypeDesc());
  }

  @Test
  void test4ArgCtor() {
    ValueMetaBase base = new ValueMetaBoolean("Hello, is it me you're looking for?", 4, 9);
    assertEquals("Hello, is it me you're looking for?", base.getName());
    assertEquals(IValueMeta.TYPE_BOOLEAN, base.getType());
    assertEquals(4, base.getLength());
    assertEquals(-1, base.getPrecision());
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, base.getStorageType());
  }

  @Test
  void testGetDataXML() throws IOException {
    BigDecimal bigDecimal = BigDecimal.ONE;
    ValueMetaBase valueDoubleMetaBase =
        new ValueMetaBase(String.valueOf(bigDecimal), IValueMeta.TYPE_BIGNUMBER);
    assertEquals(
        "<value-data>"
            + Encode.forXml(String.valueOf(bigDecimal))
            + "</value-data>"
            + SystemUtils.LINE_SEPARATOR,
        valueDoubleMetaBase.getDataXml(bigDecimal));

    boolean valueBoolean = Boolean.TRUE;
    ValueMetaBase valueBooleanMetaBase =
        new ValueMetaBase(String.valueOf(valueBoolean), IValueMeta.TYPE_BOOLEAN);
    assertEquals(
        "<value-data>"
            + Encode.forXml(String.valueOf(valueBoolean))
            + "</value-data>"
            + SystemUtils.LINE_SEPARATOR,
        valueBooleanMetaBase.getDataXml(valueBoolean));

    Date date = new Date(0);
    ValueMetaBase dateMetaBase = new ValueMetaBase(date.toString(), IValueMeta.TYPE_DATE);
    SimpleDateFormat formaterData = new SimpleDateFormat(ValueMetaBase.DEFAULT_DATE_FORMAT_MASK);
    assertEquals(
        "<value-data>"
            + Encode.forXml(formaterData.format(date))
            + "</value-data>"
            + SystemUtils.LINE_SEPARATOR,
        dateMetaBase.getDataXml(date));

    InetAddress inetAddress = InetAddress.getByName("127.0.0.1");
    ValueMetaBase inetAddressMetaBase =
        new ValueMetaBase(inetAddress.toString(), IValueMeta.TYPE_INET);
    assertEquals(
        "<value-data>"
            + Encode.forXml(inetAddress.toString())
            + "</value-data>"
            + SystemUtils.LINE_SEPARATOR,
        inetAddressMetaBase.getDataXml(inetAddress));

    long value = Long.MAX_VALUE;
    ValueMetaBase integerMetaBase =
        new ValueMetaBase(String.valueOf(value), IValueMeta.TYPE_INTEGER);
    assertEquals(
        "<value-data>"
            + Encode.forXml(String.valueOf(value))
            + "</value-data>"
            + SystemUtils.LINE_SEPARATOR,
        integerMetaBase.getDataXml(value));

    String stringValue = "TEST_STRING";
    ValueMetaBase valueMetaBase = new ValueMetaString(stringValue);
    assertEquals(
        "<value-data>" + Encode.forXml(stringValue) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
        valueMetaBase.getDataXml(stringValue));

    Timestamp timestamp = new Timestamp(0);
    ValueMetaBase valueMetaBaseTimeStamp = new ValueMetaTimestamp(timestamp.toString());
    SimpleDateFormat formater = new SimpleDateFormat(ValueMetaBase.DEFAULT_TIMESTAMP_FORMAT_MASK);
    assertEquals(
        "<value-data>"
            + Encode.forXml(formater.format(timestamp))
            + "</value-data>"
            + SystemUtils.LINE_SEPARATOR,
        valueMetaBaseTimeStamp.getDataXml(timestamp));

    byte[] byteTestValues = {0, 1, 2, 3};
    ValueMetaBase valueMetaBaseByteArray = new ValueMetaString(byteTestValues.toString());
    valueMetaBaseByteArray.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    assertEquals(
        "<value-data><binary-string>"
            + Encode.forXml(XmlHandler.encodeBinaryData(byteTestValues))
            + "</binary-string>"
            + Const.CR
            + "</value-data>",
        valueMetaBaseByteArray.getDataXml(byteTestValues));
  }

  @Test
  void testGetValueFromSqlTypeTypeOverride() throws Exception {
    final int varbinaryColumnIndex = 2;

    ValueMetaBase valueMetaBase = new ValueMetaBase(), valueMetaBaseSpy = spy(valueMetaBase);
    DatabaseMeta dbMeta = mock(DatabaseMeta.class);
    IDatabase iDatabase = mock(IDatabase.class);
    doReturn(iDatabase).when(dbMeta).getIDatabase();

    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    valueMetaBaseSpy.getValueFromSqlType(
        variables, dbMeta, TEST_NAME, metaData, varbinaryColumnIndex, false, false);

    verify(iDatabase, times(1))
        .customizeValueFromSqlType(any(IValueMeta.class), any(ResultSetMetaData.class), anyInt());
  }

  @Test
  void testConvertStringToBoolean() {
    assertNull(ValueMetaBase.convertStringToBoolean(null));
    assertNull(ValueMetaBase.convertStringToBoolean(""));
    assertTrue(ValueMetaBase.convertStringToBoolean("Y"));
    assertTrue(ValueMetaBase.convertStringToBoolean("y"));
    assertTrue(ValueMetaBase.convertStringToBoolean("Yes"));
    assertTrue(ValueMetaBase.convertStringToBoolean("YES"));
    assertTrue(ValueMetaBase.convertStringToBoolean("yES"));
    assertTrue(ValueMetaBase.convertStringToBoolean("TRUE"));
    assertTrue(ValueMetaBase.convertStringToBoolean("True"));
    assertTrue(ValueMetaBase.convertStringToBoolean("true"));
    assertTrue(ValueMetaBase.convertStringToBoolean("tRuE"));
    assertTrue(ValueMetaBase.convertStringToBoolean("Y"));
    assertFalse(ValueMetaBase.convertStringToBoolean("N"));
    assertFalse(ValueMetaBase.convertStringToBoolean("No"));
    assertFalse(ValueMetaBase.convertStringToBoolean("no"));
    assertFalse(ValueMetaBase.convertStringToBoolean("Yeah"));
    assertFalse(ValueMetaBase.convertStringToBoolean("False"));
    assertFalse(ValueMetaBase.convertStringToBoolean("NOT false"));
  }

  @Test
  void testConvertDataFromStringToString() throws HopValueException {
    ValueMetaBase inValueMetaString = new ValueMetaString();
    ValueMetaBase outValueMetaString = new ValueMetaString();
    String inputValueEmptyString = StringUtils.EMPTY;
    String inputValueNullString = null;
    String nullIf = null;
    String ifNull = null;
    int trimType = 0;
    Object result;

    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N");
    result =
        outValueMetaString.convertDataFromString(
            inputValueEmptyString, inValueMetaString, nullIf, ifNull, trimType);
    assertEquals(
        StringUtils.EMPTY,
        result,
        "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = N: "
            + "Conversion from empty string to string must return empty string");

    result =
        outValueMetaString.convertDataFromString(
            inputValueNullString, inValueMetaString, nullIf, ifNull, trimType);
    assertEquals(
        null,
        result,
        "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = N: "
            + "Conversion from null string must return null");

    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y");
    result =
        outValueMetaString.convertDataFromString(
            inputValueEmptyString, inValueMetaString, nullIf, ifNull, trimType);
    assertEquals(
        StringUtils.EMPTY,
        result,
        "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = Y: "
            + "Conversion from empty string to string must return empty string");

    result =
        outValueMetaString.convertDataFromString(
            inputValueNullString, inValueMetaString, nullIf, ifNull, trimType);
    assertEquals(
        StringUtils.EMPTY,
        result,
        "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = Y: "
            + "Conversion from null string must return empty string");
  }

  @Test
  void testConvertDataFromStringToDate() throws HopValueException {
    ValueMetaBase inValueMetaString = new ValueMetaString();
    ValueMetaBase outValueMetaDate = new ValueMetaDate();
    String inputValueEmptyString = StringUtils.EMPTY;
    String nullIf = null;
    String ifNull = null;
    int trimType = 0;
    Object result;

    result =
        outValueMetaDate.convertDataFromString(
            inputValueEmptyString, inValueMetaString, nullIf, ifNull, trimType);
    assertNull(result, "Conversion from empty string to date must return null");
  }

  @Test
  void testConvertDataFromStringForNullMeta() {
    IValueMeta valueMetaBase = new ValueMetaNone();
    String inputValueEmptyString = StringUtils.EMPTY;
    IValueMeta iValueMeta = null;
    String nullIf = null;
    String ifNull = null;
    int trimType = 0;

    assertThrows(
        HopValueException.class,
        () ->
            valueMetaBase.convertDataFromString(
                inputValueEmptyString, iValueMeta, nullIf, ifNull, trimType));
  }

  @Test
  void testGetBigDecimalThrowsHopValueException() {
    ValueMetaBase valueMeta = new ValueMetaBigNumber();
    assertThrows(HopValueException.class, () -> valueMeta.getBigNumber("1234567890"));
  }

  @Test
  void testGetIntegerThrowsHopValueException() {
    ValueMetaBase valueMeta = new ValueMetaInteger();
    assertThrows(HopValueException.class, () -> valueMeta.getInteger("1234567890"));
  }

  @Test
  void testGetNumberThrowsHopValueException() {
    ValueMetaBase valueMeta = new ValueMetaNumber();

    assertThrows(HopValueException.class, () -> valueMeta.getNumber("1234567890"));
  }

  @Test
  void testIsNumeric() {
    int[] numTypes = {IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER};
    for (int type : numTypes) {
      assertTrue(ValueMetaBase.isNumeric(type), Integer.toString(type));
    }

    int[] notNumTypes = {
      IValueMeta.TYPE_INET,
      IValueMeta.TYPE_BOOLEAN,
      IValueMeta.TYPE_BINARY,
      IValueMeta.TYPE_DATE,
      IValueMeta.TYPE_STRING
    };
    for (int type : notNumTypes) {
      assertFalse(ValueMetaBase.isNumeric(type), Integer.toString(type));
    }
  }

  @Test
  void testGetAllTypes() {
    assertArrayEquals(ValueMetaBase.getAllTypes(), ValueMetaFactory.getAllValueMetaNames());
  }

  @Test
  void testGetTrimTypeByCode() {
    assertEquals(IValueMeta.TRIM_TYPE_NONE, ValueMetaBase.getTrimTypeByCode("none"));
    assertEquals(IValueMeta.TRIM_TYPE_LEFT, ValueMetaBase.getTrimTypeByCode("left"));
    assertEquals(IValueMeta.TRIM_TYPE_RIGHT, ValueMetaBase.getTrimTypeByCode("right"));
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, ValueMetaBase.getTrimTypeByCode("both"));
    assertEquals(IValueMeta.TRIM_TYPE_NONE, ValueMetaBase.getTrimTypeByCode(null));
    assertEquals(IValueMeta.TRIM_TYPE_NONE, ValueMetaBase.getTrimTypeByCode(""));
    assertEquals(IValueMeta.TRIM_TYPE_NONE, ValueMetaBase.getTrimTypeByCode("fake"));
  }

  @Test
  void testGetTrimTypeCode() {
    assertEquals("none", ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE));
    assertEquals("left", ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_LEFT));
    assertEquals("right", ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_RIGHT));
    assertEquals("both", ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_BOTH));
  }

  @Test
  void testGetTrimTypeByDesc() {
    assertEquals(
        IValueMeta.TRIM_TYPE_NONE,
        ValueMetaBase.getTrimTypeByDesc(BaseMessages.getString(PKG, "ValueMeta.TrimType.None")));
    assertEquals(
        IValueMeta.TRIM_TYPE_LEFT,
        ValueMetaBase.getTrimTypeByDesc(BaseMessages.getString(PKG, "ValueMeta.TrimType.Left")));
    assertEquals(
        IValueMeta.TRIM_TYPE_RIGHT,
        ValueMetaBase.getTrimTypeByDesc(BaseMessages.getString(PKG, "ValueMeta.TrimType.Right")));
    assertEquals(
        IValueMeta.TRIM_TYPE_BOTH,
        ValueMetaBase.getTrimTypeByDesc(BaseMessages.getString(PKG, "ValueMeta.TrimType.Both")));
    assertEquals(IValueMeta.TRIM_TYPE_NONE, ValueMetaBase.getTrimTypeByDesc(null));
    assertEquals(IValueMeta.TRIM_TYPE_NONE, ValueMetaBase.getTrimTypeByDesc(""));
    assertEquals(IValueMeta.TRIM_TYPE_NONE, ValueMetaBase.getTrimTypeByDesc("fake"));
  }

  @Test
  void testGetTrimTypeDesc() {
    assertEquals(
        ValueMetaBase.getTrimTypeDesc(IValueMeta.TRIM_TYPE_NONE),
        BaseMessages.getString(PKG, "ValueMeta.TrimType.None"));
    assertEquals(
        ValueMetaBase.getTrimTypeDesc(IValueMeta.TRIM_TYPE_LEFT),
        BaseMessages.getString(PKG, "ValueMeta.TrimType.Left"));
    assertEquals(
        ValueMetaBase.getTrimTypeDesc(IValueMeta.TRIM_TYPE_RIGHT),
        BaseMessages.getString(PKG, "ValueMeta.TrimType.Right"));
    assertEquals(
        ValueMetaBase.getTrimTypeDesc(IValueMeta.TRIM_TYPE_BOTH),
        BaseMessages.getString(PKG, "ValueMeta.TrimType.Both"));
    assertEquals(
        ValueMetaBase.getTrimTypeDesc(-1), BaseMessages.getString(PKG, "ValueMeta.TrimType.None"));
    assertEquals(
        ValueMetaBase.getTrimTypeDesc(10000),
        BaseMessages.getString(PKG, "ValueMeta.TrimType.None"));
  }

  @Test
  void testOrigin() {
    ValueMetaBase base = new ValueMetaBase();
    base.setOrigin("myOrigin");
    assertEquals("myOrigin", base.getOrigin());
    base.setOrigin(null);
    assertNull(base.getOrigin());
    base.setOrigin("");
    assertEquals("", base.getOrigin());
  }

  @Test
  void testName() {
    ValueMetaBase base = new ValueMetaBase();
    base.setName("myName");
    assertEquals("myName", base.getName());
    base.setName(null);
    assertNull(base.getName());
    base.setName("");
    assertEquals("", base.getName());
  }

  @Test
  void testLength() {
    ValueMetaBase base = new ValueMetaBase();
    base.setLength(6);
    assertEquals(6, base.getLength());
    base.setLength(-1);
    assertEquals(-1, base.getLength());
  }

  @Test
  void testPrecision() {
    ValueMetaBase base = new ValueMetaBase();
    base.setPrecision(6);
    assertEquals(6, base.getPrecision());
    base.setPrecision(-1);
    assertEquals(-1, base.getPrecision());
  }

  @Test
  void testCompareIntegers() throws HopValueException {
    ValueMetaBase intMeta = new ValueMetaInteger("int");
    Long int1 = 6223372036854775804L;
    Long int2 = -6223372036854775804L;
    assertEquals(1, intMeta.compare(int1, int2));
    assertEquals(-1, intMeta.compare(int2, int1));
    assertEquals(0, intMeta.compare(int1, int1));
    assertEquals(0, intMeta.compare(int2, int2));

    int1 = 9223372036854775804L;
    int2 = -9223372036854775804L;
    assertEquals(1, intMeta.compare(int1, int2));
    assertEquals(-1, intMeta.compare(int2, int1));
    assertEquals(0, intMeta.compare(int1, int1));
    assertEquals(0, intMeta.compare(int2, int2));

    int1 = 6223372036854775804L;
    int2 = -9223372036854775804L;
    assertEquals(1, intMeta.compare(int1, int2));
    assertEquals(-1, intMeta.compare(int2, int1));
    assertEquals(0, intMeta.compare(int1, int1));

    int1 = 9223372036854775804L;
    int2 = -6223372036854775804L;
    assertEquals(1, intMeta.compare(int1, int2));
    assertEquals(-1, intMeta.compare(int2, int1));
    assertEquals(0, intMeta.compare(int1, int1));

    int1 = null;
    int2 = 6223372036854775804L;
    assertEquals(-1, intMeta.compare(int1, int2));
    intMeta.setSortedDescending(true);
    assertEquals(1, intMeta.compare(int1, int2));
  }

  @Test
  void testCompareIntegerToDouble() throws HopValueException {
    IValueMeta intMeta = new ValueMetaInteger("int");
    Long int1 = 2L;
    IValueMeta numberMeta = new ValueMetaNumber("number");
    Double double2 = 1.5;
    assertEquals(1, intMeta.compare(int1, numberMeta, double2));
  }

  @Test
  void testCompareDate() throws HopValueException {
    IValueMeta dateMeta = new ValueMetaDate("int");
    Date date1 = new Date(6223372036854775804L);
    Date date2 = new Date(-6223372036854775804L);
    assertEquals(1, dateMeta.compare(date1, date2));
    assertEquals(-1, dateMeta.compare(date2, date1));
    assertEquals(0, dateMeta.compare(date1, date1));
  }

  @Test
  void testCompareDateWithStorageMask() throws HopValueException {
    IValueMeta storageMeta = new ValueMetaString("string");
    storageMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    storageMeta.setConversionMask("MM/dd/yyyy HH:mm");

    IValueMeta dateMeta = new ValueMetaDate("date");
    dateMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    dateMeta.setStorageMetadata(storageMeta);
    dateMeta.setConversionMask("yyyy-MM-dd");

    IValueMeta targetDateMeta = new ValueMetaDate("date");
    targetDateMeta.setConversionMask("yyyy-MM-dd");
    targetDateMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

    String date = "2/24/2017 0:00";

    Date equalDate = new GregorianCalendar(2017, Calendar.FEBRUARY, 24).getTime();
    assertEquals(0, dateMeta.compare(date.getBytes(), targetDateMeta, equalDate));

    Date pastDate = new GregorianCalendar(2017, Calendar.JANUARY, 24).getTime();
    assertEquals(1, dateMeta.compare(date.getBytes(), targetDateMeta, pastDate));

    Date futureDate = new GregorianCalendar(2017, Calendar.MARCH, 24).getTime();
    assertEquals(-1, dateMeta.compare(date.getBytes(), targetDateMeta, futureDate));
  }

  @Test
  void testCompareDateNoStorageMask() throws HopValueException {
    IValueMeta storageMeta = new ValueMetaString("string");
    storageMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    storageMeta.setConversionMask(
        null); // explicit set to null, to make sure test condition are met

    IValueMeta dateMeta = new ValueMetaDate("date");
    dateMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    dateMeta.setStorageMetadata(storageMeta);
    dateMeta.setConversionMask("yyyy-MM-dd");

    IValueMeta targetDateMeta = new ValueMetaDate("date");
    // targetDateMeta.setConversionMask( "yyyy-MM-dd" ); by not setting a maks, the default one is
    // used
    // and since this is a date of normal storage it should work
    targetDateMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

    String date = "2017/02/24 00:00:00.000";

    Date equalDate = new GregorianCalendar(2017, Calendar.FEBRUARY, 24).getTime();
    assertEquals(0, dateMeta.compare(date.getBytes(), targetDateMeta, equalDate));

    Date pastDate = new GregorianCalendar(2017, Calendar.JANUARY, 24).getTime();
    assertEquals(1, dateMeta.compare(date.getBytes(), targetDateMeta, pastDate));

    Date futureDate = new GregorianCalendar(2017, Calendar.MARCH, 24).getTime();
    assertEquals(-1, dateMeta.compare(date.getBytes(), targetDateMeta, futureDate));
  }

  @Test
  void testCompareBinary() throws HopValueException {
    IValueMeta dateMeta = new ValueMetaBinary("int");
    byte[] value1 = new byte[] {0, 1, 0, 0, 0, 1};
    byte[] value2 = new byte[] {0, 1, 0, 0, 0, 0};
    assertEquals(1, dateMeta.compare(value1, value2));
    assertEquals(-1, dateMeta.compare(value2, value1));
    assertEquals(0, dateMeta.compare(value1, value1));
  }

  @Test
  void testDateParsing8601() throws Exception {
    ValueMetaDate dateMeta = new ValueMetaDate("date");
    dateMeta.setDateFormatLenient(false);

    // try to convert date by 'start-of-date' make - old behavior
    dateMeta.setConversionMask("yyyy-MM-dd");
    assertEquals(
        local(1918, 3, 25, 0, 0, 0, 0),
        dateMeta.convertStringToDate("1918-03-25T07:40:03.012+03:00"));

    // convert ISO-8601 date - supported since Java 7
    dateMeta.setConversionMask("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    assertEquals(
        utc(1918, 3, 25, 5, 10, 3, 12),
        dateMeta.convertStringToDate("1918-03-25T07:40:03.012+02:30"));
    assertEquals(
        utc(1918, 3, 25, 7, 40, 3, 12), dateMeta.convertStringToDate("1918-03-25T07:40:03.012Z"));

    // convert date
    dateMeta.setConversionMask("yyyy-MM-dd");
    assertEquals(local(1918, 3, 25, 0, 0, 0, 0), dateMeta.convertStringToDate("1918-03-25"));
    // convert date with spaces at the end
    assertEquals(local(1918, 3, 25, 0, 0, 0, 0), dateMeta.convertStringToDate("1918-03-25  \n"));
  }

  @Test
  void testDateToStringParse() throws Exception {
    ValueMetaBase dateMeta = new ValueMetaString("date");
    dateMeta.setDateFormatLenient(false);

    // try to convert date by 'start-of-date' make - old behavior
    dateMeta.setConversionMask("yyyy-MM-dd");
    assertEquals(
        local(1918, 3, 25, 0, 0, 0, 0),
        dateMeta.convertStringToDate("1918-03-25T07:40:03.012+03:00"));
  }

  @Test
  void testSetPreparedStatementStringValueDontLogTruncated() throws HopDatabaseException {
    ValueMetaBase valueMetaString = new ValueMetaString("LOG_FIELD", LOG_FIELD.length(), 0);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(databaseMeta.getMaxTextFieldLength()).thenReturn(LOG_FIELD.length());
    List<HopLoggingEvent> events = listener.getEvents();
    assertEquals(0, events.size());

    valueMetaString.setPreparedStatementValue(databaseMeta, preparedStatement, 0, LOG_FIELD);

    // no logging occurred as max string length equals to logging text length
    assertEquals(0, events.size());
  }

  @Test
  void testValueMetaBaseOnlyHasOneLogger() throws NoSuchFieldException, IllegalAccessException {
    Field log = ValueMetaBase.class.getDeclaredField("log");
    assertTrue(Modifier.isStatic(log.getModifiers()));
    assertTrue(Modifier.isFinal(log.getModifiers()));
    log.setAccessible(true);
    try {
      assertEquals(
          LoggingRegistry.getInstance()
              .findExistingLoggingSource(new LoggingObject("ValueMetaBase"))
              .getLogChannelId(),
          ((ILogChannel) log.get(null)).getLogChannelId());
    } finally {
      log.setAccessible(false);
    }
  }

  Date local(int year, int month, int dat, int hrs, int min, int sec, int ms) {
    GregorianCalendar cal = new GregorianCalendar(year, month - 1, dat, hrs, min, sec);
    cal.set(Calendar.MILLISECOND, ms);
    return cal.getTime();
  }

  Date utc(int year, int month, int dat, int hrs, int min, int sec, int ms) {
    GregorianCalendar cal = new GregorianCalendar(year, month - 1, dat, hrs, min, sec);
    cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    cal.set(Calendar.MILLISECOND, ms);
    return cal.getTime();
  }

  @Test
  void testGetNativeDataTypeClass() {
    IValueMeta base = new ValueMetaBase();
    Class<?> clazz = null;
    try {
      clazz = base.getNativeDataTypeClass();
      fail();
    } catch (HopValueException expected) {
      // ValueMetaBase should throw an exception, as all sub-classes should override
      assertNull(clazz);
    }
  }

  @Test
  void testConvertDataUsingConversionMetaDataForCustomMeta() {
    ValueMetaBase baseMeta = new ValueMetaString("CUSTOM_VALUEMETA_STRING");
    baseMeta.setConversionMetadata(new ValueMetaBase("CUSTOM", 999));
    Object customData = new Object();
    try {
      baseMeta.convertDataUsingConversionMetaData(customData);
      fail("Should have thrown a Hop Value Exception with a proper message. Not a NPE stack trace");
    } catch (HopValueException e) {
      String expectedMessage =
          "CUSTOM_VALUEMETA_STRING String : I can't convert the specified value to data type : 999";
      assertEquals(expectedMessage, e.getMessage().trim());
    }
  }

  @Test
  void testConvertDataUsingConversionMetaData() throws HopValueException {
    ValueMetaString base = new ValueMetaString();
    double delta = 1e-15;

    base.setConversionMetadata(new ValueMetaString("STRING"));
    Object defaultStringData = "STRING DATA";
    String convertedStringData =
        (String) base.convertDataUsingConversionMetaData(defaultStringData);
    assertEquals("STRING DATA", convertedStringData);

    base.setConversionMetadata(new ValueMetaInteger("INTEGER"));
    Object defaultIntegerData = "1";
    long convertedIntegerData = (long) base.convertDataUsingConversionMetaData(defaultIntegerData);
    assertEquals(1, convertedIntegerData);

    base.setConversionMetadata(new ValueMetaNumber("NUMBER"));
    Object defaultNumberData = "1.999";
    double convertedNumberData =
        (double) base.convertDataUsingConversionMetaData(defaultNumberData);
    assertEquals(1.999, convertedNumberData, delta);

    IValueMeta dateConversionMeta = new ValueMetaDate("DATE");
    dateConversionMeta.setDateFormatTimeZone(TimeZone.getTimeZone("CST"));
    base.setConversionMetadata(dateConversionMeta);
    Object defaultDateData = "1990/02/18 00:00:00.000";
    Date date1 = new Date(635320800000L);
    Date convertedDateData = (Date) base.convertDataUsingConversionMetaData(defaultDateData);
    assertEquals(date1, convertedDateData);

    base.setConversionMetadata(new ValueMetaBigNumber("BIG_NUMBER"));
    Object defaultBigNumber = String.valueOf(BigDecimal.ONE);
    BigDecimal convertedBigNumber =
        (BigDecimal) base.convertDataUsingConversionMetaData(defaultBigNumber);
    assertEquals(BigDecimal.ONE, convertedBigNumber);

    base.setConversionMetadata(new ValueMetaBoolean("BOOLEAN"));
    Object defaultBoolean = "true";
    boolean convertedBoolean = (boolean) base.convertDataUsingConversionMetaData(defaultBoolean);
    assertEquals(true, convertedBoolean);
  }

  @Test
  void testGetCompatibleString() throws HopValueException {
    ValueMetaInteger valueMetaInteger = new ValueMetaInteger("INTEGER");
    valueMetaInteger.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);

    assertEquals("2", valueMetaInteger.getCompatibleString(2L)); // BACKLOG-15750
  }

  @Test
  void testReadDataInet() throws Exception {
    InetAddress localhost = InetAddress.getByName("127.0.0.1");
    byte[] address = localhost.getAddress();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dataOutputStream.writeBoolean(false);
    dataOutputStream.writeInt(address.length);
    dataOutputStream.write(address);

    DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    ValueMetaBase vm = new ValueMetaInternetAddress();
    assertEquals(localhost, vm.readData(dis));
  }

  @Test
  void testWriteDataInet() throws Exception {
    InetAddress localhost = InetAddress.getByName("127.0.0.1");
    byte[] address = localhost.getAddress();

    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
    DataOutputStream dos1 = new DataOutputStream(out1);
    dos1.writeBoolean(false);
    dos1.writeInt(address.length);
    dos1.write(address);
    byte[] expected = out1.toByteArray();

    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
    DataOutputStream dos2 = new DataOutputStream(out2);
    ValueMetaBase vm = new ValueMetaInternetAddress();
    vm.writeData(dos2, localhost);
    byte[] actual = out2.toByteArray();

    assertArrayEquals(expected, actual);
  }

  @Test
  void testConvertBigNumberToBoolean() {
    ValueMetaBase vmb = new ValueMetaBase();
    assertTrue(vmb.convertBigNumberToBoolean(new BigDecimal("-234")));
    assertTrue(vmb.convertBigNumberToBoolean(new BigDecimal("234")));
    assertFalse(vmb.convertBigNumberToBoolean(new BigDecimal("0")));
    assertTrue(vmb.convertBigNumberToBoolean(new BigDecimal("1.7976E308")));
  }

  @Test
  void testGetValueFromNode() throws Exception {

    ValueMetaBase valueMetaBase = null;
    Node xmlNode = null;

    valueMetaBase = new ValueMetaString("test");
    xmlNode = XmlHandler.loadXmlString("<value-data>String val</value-data>").getFirstChild();
    assertEquals("String val", valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaNumber("test");
    xmlNode = XmlHandler.loadXmlString("<value-data>689.2</value-data>").getFirstChild();
    assertEquals(689.2, valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaNumber("test");
    xmlNode = XmlHandler.loadXmlString("<value-data>689.2</value-data>").getFirstChild();
    assertEquals(689.2, valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaInteger("test");
    xmlNode = XmlHandler.loadXmlString("<value-data>68933</value-data>").getFirstChild();
    assertEquals(68933l, valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaDate("test");
    xmlNode =
        XmlHandler.loadXmlString("<value-data>2017/11/27 08:47:10.000</value-data>")
            .getFirstChild();
    assertEquals(
        XmlHandler.stringToDate("2017/11/27 08:47:10.000"), valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaTimestamp("test");
    xmlNode =
        XmlHandler.loadXmlString("<value-data>2017/11/27 08:47:10.123456789</value-data>")
            .getFirstChild();
    assertEquals(
        XmlHandler.stringToTimestamp("2017/11/27 08:47:10.123456789"),
        valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaBoolean("test");
    xmlNode = XmlHandler.loadXmlString("<value-data>Y</value-data>").getFirstChild();
    assertEquals(true, valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaBinary("test");
    byte[] bytes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    String s = XmlHandler.encodeBinaryData(bytes);
    xmlNode =
        XmlHandler.loadXmlString(
                "<value-data>test<binary-value>" + s + "</binary-value></value-data>")
            .getFirstChild();
    assertArrayEquals(bytes, (byte[]) valueMetaBase.getValue(xmlNode));

    valueMetaBase = new ValueMetaString("test");
    xmlNode = XmlHandler.loadXmlString("<value-data></value-data>").getFirstChild();
    assertNull(valueMetaBase.getValue(xmlNode));
  }

  @Test
  void testGetValueUnknownType() throws HopXmlException {
    ValueMetaBase metaBase = new ValueMetaNone("test");
    Node node = XmlHandler.loadXmlString("<value-data>not empty</value-data>").getFirstChild();
    assertThrows(HopException.class, () -> metaBase.getValue(node));
  }

  @Test
  void testConvertStringToTimestampType() throws HopValueException {
    String timestampStringRepresentation = "2018/04/11 16:45:15.000000000";
    Timestamp expectedTimestamp = Timestamp.valueOf("2018-04-11 16:45:15.000000000");

    ValueMetaBase base = new ValueMetaString("ValueMetaStringColumn");
    base.setConversionMetadata(new ValueMetaTimestamp("ValueMetaTimestamp"));
    Timestamp timestamp =
        (Timestamp) base.convertDataUsingConversionMetaData(timestampStringRepresentation);
    assertEquals(expectedTimestamp, timestamp);
  }

  @Test
  void testConvertNumberToString() throws HopValueException {
    String expectedStringRepresentation = "123.123";
    Number numberToTest = Double.valueOf("123.123");

    ValueMetaBase base = new ValueMetaNumber("ValueMetaNumber");
    base.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

    ValueMetaString valueMetaString = new ValueMetaString("ValueMetaString");
    base.setConversionMetadata(valueMetaString);

    String convertedNumber = base.convertNumberToString((Double) numberToTest);
    assertEquals(expectedStringRepresentation, convertedNumber);
  }

  @Test
  void testNullHashCodes() throws Exception {
    ValueMetaBase valueMetaString = new ValueMetaBase();

    valueMetaString.type = IValueMeta.TYPE_BOOLEAN;
    assertEquals(0 ^ 1, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_DATE;
    assertEquals(0 ^ 2, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_NUMBER;
    assertEquals(0 ^ 4, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_STRING;
    assertEquals(0 ^ 8, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_INTEGER;
    assertEquals(0 ^ 16, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_BIGNUMBER;
    assertEquals(0 ^ 32, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_BINARY;
    assertEquals(0 ^ 64, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_TIMESTAMP;
    assertEquals(0 ^ 128, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_INET;
    assertEquals(0 ^ 256, valueMetaString.hashCode(null));

    valueMetaString.type = IValueMeta.TYPE_NONE;
    assertEquals(0, valueMetaString.hashCode(null));
  }

  @Test
  void testHashCodes() throws Exception {
    ValueMetaBase valueMetaString = new ValueMetaBase();

    valueMetaString.type = IValueMeta.TYPE_BOOLEAN;
    assertEquals(1231, valueMetaString.hashCode(true));

    SimpleDateFormat sdf = new SimpleDateFormat("dd/M/yyyy");
    String dateInString = "1/1/2018";
    Date dateObj = sdf.parse(dateInString);
    valueMetaString.type = IValueMeta.TYPE_DATE;
    assertEquals(-1358655136, valueMetaString.hashCode(dateObj));

    Double numberObj = 5.1;
    valueMetaString.type = IValueMeta.TYPE_NUMBER;
    assertEquals(645005312, valueMetaString.hashCode(numberObj));

    valueMetaString.type = IValueMeta.TYPE_STRING;
    assertEquals(3556498, valueMetaString.hashCode("test"));

    Long longObj = 123L;
    valueMetaString.type = IValueMeta.TYPE_INTEGER;
    assertEquals(123, valueMetaString.hashCode(longObj));

    BigDecimal bDecimalObj = new BigDecimal(123.1);
    valueMetaString.type = IValueMeta.TYPE_BIGNUMBER;
    assertEquals(465045870, valueMetaString.hashCode(bDecimalObj));

    byte[] bBinary = new byte[2];
    bBinary[0] = 1;
    bBinary[1] = 0;
    valueMetaString.type = IValueMeta.TYPE_BINARY;
    assertEquals(992, valueMetaString.hashCode(bBinary));

    Timestamp timestampObj = Timestamp.valueOf("2018-01-01 10:10:10.000000000");
    valueMetaString.type = IValueMeta.TYPE_TIMESTAMP;
    assertEquals(-1322045776, valueMetaString.hashCode(timestampObj));

    byte[] ipAddr = new byte[] {127, 0, 0, 1};
    InetAddress addrObj = InetAddress.getByAddress(ipAddr);
    valueMetaString.type = IValueMeta.TYPE_INET;
    assertEquals(2130706433, valueMetaString.hashCode(addrObj));

    valueMetaString.type = IValueMeta.TYPE_NONE;
    assertEquals(0, valueMetaString.hashCode("any"));
  }

  @Test
  void testMetdataPreviewSqlCharToHopString() throws SQLException, HopDatabaseException {
    doReturn(Types.CHAR).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isString());
  }

  @Test
  void testMetdataPreviewSqlVarcharToHopString() throws SQLException, HopDatabaseException {
    doReturn(Types.VARCHAR).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isString());
  }

  @Test
  void testMetdataPreviewSqlNVarcharToHopString() throws SQLException, HopDatabaseException {
    doReturn(Types.NVARCHAR).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isString());
  }

  @Test
  void testMetdataPreviewSqlLongVarcharToHopString() throws SQLException, HopDatabaseException {
    doReturn(Types.LONGVARCHAR).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isString());
  }

  @Test
  void testMetdataPreviewSqlClobToHopString() throws SQLException, HopDatabaseException {
    doReturn(Types.CLOB).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isString());
    assertEquals(DatabaseMeta.CLOB_LENGTH, valueMeta.getLength());
    assertTrue(valueMeta.isLargeTextField());
  }

  @Test
  void testMetdataPreviewSqlNClobToHopString() throws SQLException, HopDatabaseException {
    doReturn(Types.NCLOB).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isString());
    assertEquals(DatabaseMeta.CLOB_LENGTH, valueMeta.getLength());
    assertTrue(valueMeta.isLargeTextField());
  }

  @Test
  void testMetdataPreviewSqlBigIntToHopInteger() throws SQLException, HopDatabaseException {
    doReturn(Types.BIGINT).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isInteger());
    assertEquals(0, valueMeta.getPrecision());
    assertEquals(15, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlIntegerToHopInteger() throws SQLException, HopDatabaseException {
    doReturn(Types.INTEGER).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isInteger());
    assertEquals(0, valueMeta.getPrecision());
    assertEquals(9, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlSmallIntToHopInteger() throws SQLException, HopDatabaseException {
    doReturn(Types.SMALLINT).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isInteger());
    assertEquals(0, valueMeta.getPrecision());
    assertEquals(4, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlTinyIntToHopInteger() throws SQLException, HopDatabaseException {
    doReturn(Types.TINYINT).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isInteger());
    assertEquals(0, valueMeta.getPrecision());
    assertEquals(2, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlDecimalToHopBigNumber() throws SQLException, HopDatabaseException {
    doReturn(Types.DECIMAL).when(resultSet).getInt("DATA_TYPE");
    doReturn(20).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(5).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBigNumber());
    assertEquals(5, valueMeta.getPrecision());
    assertEquals(20, valueMeta.getLength());

    doReturn(Types.DECIMAL).when(resultSet).getInt("DATA_TYPE");
    doReturn(20).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(0).when(resultSet).getInt("DECIMAL_DIGITS");
    valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBigNumber());
    assertEquals(0, valueMeta.getPrecision());
    assertEquals(20, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlDecimalToHopInteger() throws SQLException, HopDatabaseException {
    doReturn(Types.DECIMAL).when(resultSet).getInt("DATA_TYPE");
    doReturn(2).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(0).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isInteger());
    assertEquals(0, valueMeta.getPrecision());
    assertEquals(2, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlDoubleToHopNumber() throws SQLException, HopDatabaseException {
    doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
    doReturn(3).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(2).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isNumber());
    assertEquals(2, valueMeta.getPrecision());
    assertEquals(3, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlDoubleWithoutDecimalDigits() throws SQLException, HopDatabaseException {
    doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
    doReturn(3).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(0).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isNumber());
    assertEquals(-1, valueMeta.getPrecision());
    assertEquals(3, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlDoubleToHopBigNumber() throws SQLException, HopDatabaseException {
    doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
    doReturn(20).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(15).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBigNumber());
    assertEquals(15, valueMeta.getPrecision());
    assertEquals(20, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlFloatToHopNumber() throws SQLException, HopDatabaseException {
    doReturn(Types.FLOAT).when(resultSet).getInt("DATA_TYPE");
    doReturn(3).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(2).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isNumber());
    assertEquals(2, valueMeta.getPrecision());
    assertEquals(3, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlRealToHopNumber() throws SQLException, HopDatabaseException {
    doReturn(Types.REAL).when(resultSet).getInt("DATA_TYPE");
    doReturn(3).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(2).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isNumber());
    assertEquals(2, valueMeta.getPrecision());
    assertEquals(3, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewUnsupportedSqlTimestamp() throws SQLException, HopDatabaseException {
    doReturn(Types.TIMESTAMP).when(resultSet).getInt("DATA_TYPE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(19).when(resultSet).getInt("DECIMAL_DIGITS");
    doReturn(false).when(dbMeta).supportsTimestampDataType();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(!valueMeta.isDate());
  }

  @Test
  void testMetdataPreviewSqlTimeToHopDate() throws SQLException, HopDatabaseException {
    doReturn(Types.TIME).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isDate());
  }

  @Test
  void testMetdataPreviewSqlBooleanToHopBoolean() throws SQLException, HopDatabaseException {
    doReturn(Types.BOOLEAN).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBoolean());
  }

  @Test
  void testMetdataPreviewSqlBitToHopBoolean() throws SQLException, HopDatabaseException {
    doReturn(Types.BIT).when(resultSet).getInt("DATA_TYPE");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBoolean());
  }
}
