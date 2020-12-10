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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
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
import java.text.ParseException;
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
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
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
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.*;
import org.mockito.Spy;
import org.owasp.encoder.Encode;
import org.w3c.dom.Node;

public class ValueMetaBaseTest  {
	 protected class StoreLoggingEventListener implements IHopLoggingEventListener {

		    private List<HopLoggingEvent> events = new ArrayList<>();

		    @Override
		    public void eventAdded( HopLoggingEvent event ) {
		      events.add( event );
		    }

		    public List<HopLoggingEvent> getEvents() {
		      return events;
		    }
	  }
	
	  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

	  protected static final String TEST_NAME = "TEST_NAME";
	  protected static final String LOG_FIELD = "LOG_FIELD";
	  protected static final int MAX_TEXT_FIELD_LEN = 5;

	  // Get PKG from class under test
	  protected Class<?> PKG = ValueMetaBase.PKG;
	  protected StoreLoggingEventListener listener;

	  @Spy
	  protected DatabaseMeta databaseMetaSpy = spy( new DatabaseMeta() );
	  protected PreparedStatement preparedStatementMock = mock( PreparedStatement.class );
	  protected ResultSet resultSet;
	  protected DatabaseMeta dbMeta;
	  protected ValueMetaBase valueMetaBase;
	  protected IVariables variables;

	  @BeforeClass
	  public static void setUpBeforeClass() throws HopException {
	    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
	    PluginRegistry.addPluginType( DatabasePluginType.getInstance() );
	    PluginRegistry.init();
	    HopLogStore.init();
	  }

	  @Before
	  public void setUp() {
	    listener = new StoreLoggingEventListener();
	    HopLogStore.getAppender().addLoggingEventListener( listener );

	    valueMetaBase = new ValueMetaBase();
	    dbMeta = spy( new DatabaseMeta() );
	    resultSet = mock( ResultSet.class );

	    variables = Variables.getADefaultVariableSpace();
	  }

	  @After
	  public void tearDown() {
	    HopLogStore.getAppender().removeLoggingEventListener( listener );
	    listener = new StoreLoggingEventListener();
	  }
	  
	  protected void initValueMeta( BaseDatabaseMeta dbMeta, int length, Object data ) throws HopDatabaseException {
		    IValueMeta valueMetaString = new ValueMetaString( LOG_FIELD, length, 0 );
		    databaseMetaSpy.setIDatabase( dbMeta );
		    valueMetaString.setPreparedStatementValue( databaseMetaSpy, preparedStatementMock, 0, data );
	  }
  @Test
  public void testDefaultCtor() {
    ValueMetaBase base = new ValueMetaBase();
    assertNotNull( base );
    assertNull( base.getName() );
    assertEquals( base.getType(), IValueMeta.TYPE_NONE );
  }

  @Test
  public void testCtorName() {
    ValueMetaBase base = new ValueMetaBase( "myValueMeta" );
    assertEquals( base.getName(), "myValueMeta" );
    assertEquals( base.getType(), IValueMeta.TYPE_NONE );
    assertNotNull( base.getTypeDesc() );
  }

  @Test
  public void testCtorNameAndType() {
    IValueMeta base = new ValueMetaString( "myStringType" );
    assertEquals( base.getName(), "myStringType" );
    assertEquals( base.getType(), IValueMeta.TYPE_STRING );
    assertEquals( base.getTypeDesc(), "String" );
  }

  @Test
  public void test4ArgCtor() {
    ValueMetaBase base =
      new ValueMetaBoolean( "Hello, is it me you're looking for?", 4, 9 );
    assertEquals( base.getName(), "Hello, is it me you're looking for?" );
    assertEquals( base.getType(), IValueMeta.TYPE_BOOLEAN );
    assertEquals( base.getLength(), 4 );
    assertEquals( base.getPrecision(), -1 );
    assertEquals( base.getStorageType(), IValueMeta.STORAGE_TYPE_NORMAL );
  }

  @Test
  public void testGetDataXML() throws IOException {
    BigDecimal bigDecimal = BigDecimal.ONE;
    ValueMetaBase valueDoubleMetaBase =
      new ValueMetaBase( String.valueOf( bigDecimal ), IValueMeta.TYPE_BIGNUMBER );
    assertEquals(
      "<value-data>" + Encode.forXml( String.valueOf( bigDecimal ) ) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
      valueDoubleMetaBase.getDataXml( bigDecimal ) );

    boolean valueBoolean = Boolean.TRUE;
    ValueMetaBase valueBooleanMetaBase =
      new ValueMetaBase( String.valueOf( valueBoolean ), IValueMeta.TYPE_BOOLEAN );
    assertEquals(
      "<value-data>" + Encode.forXml( String.valueOf( valueBoolean ) ) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
      valueBooleanMetaBase.getDataXml( valueBoolean ) );

    Date date = new Date( 0 );
    ValueMetaBase dateMetaBase =
      new ValueMetaBase( date.toString(), IValueMeta.TYPE_DATE );
    SimpleDateFormat formaterData = new SimpleDateFormat( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
    assertEquals(
      "<value-data>" + Encode.forXml( formaterData.format( date ) ) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
      dateMetaBase.getDataXml( date ) );

    InetAddress inetAddress = InetAddress.getByName( "127.0.0.1" );
    ValueMetaBase inetAddressMetaBase =
      new ValueMetaBase( inetAddress.toString(), IValueMeta.TYPE_INET );
    assertEquals( "<value-data>" + Encode.forXml( inetAddress.toString() ) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
      inetAddressMetaBase.getDataXml( inetAddress ) );

    long value = Long.MAX_VALUE;
    ValueMetaBase integerMetaBase = new ValueMetaBase( String.valueOf( value ), IValueMeta.TYPE_INTEGER );
    assertEquals( "<value-data>" + Encode.forXml( String.valueOf( value ) ) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
      integerMetaBase.getDataXml( value ) );

    String stringValue = "TEST_STRING";
    ValueMetaBase valueMetaBase = new ValueMetaString( stringValue );
    assertEquals( "<value-data>" + Encode.forXml( stringValue ) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
      valueMetaBase.getDataXml( stringValue ) );

    Timestamp timestamp = new Timestamp( 0 );
    ValueMetaBase valueMetaBaseTimeStamp = new ValueMetaTimestamp( timestamp.toString() );
    SimpleDateFormat formater = new SimpleDateFormat( ValueMetaBase.DEFAULT_TIMESTAMP_FORMAT_MASK );
    assertEquals(
      "<value-data>" + Encode.forXml( formater.format( timestamp ) ) + "</value-data>" + SystemUtils.LINE_SEPARATOR,
      valueMetaBaseTimeStamp.getDataXml( timestamp ) );

    byte[] byteTestValues = { 0, 1, 2, 3 };
    ValueMetaBase valueMetaBaseByteArray = new ValueMetaString( byteTestValues.toString() );
    valueMetaBaseByteArray.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    assertEquals(
      "<value-data><binary-string>" + Encode.forXml( XmlHandler.encodeBinaryData( byteTestValues ) )
        + "</binary-string>" + Const.CR + "</value-data>",
      valueMetaBaseByteArray.getDataXml( byteTestValues ) );
  }

  @Test
  public void testGetValueFromSqlTypeTypeOverride() throws Exception {
    final int varbinaryColumnIndex = 2;

    ValueMetaBase valueMetaBase = new ValueMetaBase(),
      valueMetaBaseSpy = spy( valueMetaBase );
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    IDatabase iDatabase = mock( IDatabase.class );
    doReturn( iDatabase ).when( dbMeta ).getIDatabase();

    ResultSetMetaData metaData = mock( ResultSetMetaData.class );
    valueMetaBaseSpy.getValueFromSqlType( variables, dbMeta, TEST_NAME, metaData, varbinaryColumnIndex, false, false );

    verify( iDatabase, times( 1 ) ).customizeValueFromSqlType( any( IValueMeta.class ),
      any( ResultSetMetaData.class ), anyInt() );
  }

  @Test
  public void testConvertStringToBoolean() {
    assertNull( ValueMetaBase.convertStringToBoolean( null ) );
    assertNull( ValueMetaBase.convertStringToBoolean( "" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "Y" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "y" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "Yes" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "YES" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "yES" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "TRUE" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "True" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "true" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "tRuE" ) );
    assertTrue( ValueMetaBase.convertStringToBoolean( "Y" ) );
    assertFalse( ValueMetaBase.convertStringToBoolean( "N" ) );
    assertFalse( ValueMetaBase.convertStringToBoolean( "No" ) );
    assertFalse( ValueMetaBase.convertStringToBoolean( "no" ) );
    assertFalse( ValueMetaBase.convertStringToBoolean( "Yeah" ) );
    assertFalse( ValueMetaBase.convertStringToBoolean( "False" ) );
    assertFalse( ValueMetaBase.convertStringToBoolean( "NOT false" ) );
  }

  @Test
  public void testConvertDataFromStringToString() throws HopValueException {
    ValueMetaBase inValueMetaString = new ValueMetaString();
    ValueMetaBase outValueMetaString = new ValueMetaString();
    String inputValueEmptyString = StringUtils.EMPTY;
    String inputValueNullString = null;
    String nullIf = null;
    String ifNull = null;
    int trimType = 0;
    Object result;

    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N" );
    result =
      outValueMetaString.convertDataFromString( inputValueEmptyString, inValueMetaString, nullIf, ifNull, trimType );
    assertEquals( "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = N: "
      + "Conversion from empty string to string must return empty string", StringUtils.EMPTY, result );

    result =
      outValueMetaString.convertDataFromString( inputValueNullString, inValueMetaString, nullIf, ifNull, trimType );
    assertEquals( "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = N: "
      + "Conversion from null string must return null", null, result );

    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y" );
    result =
      outValueMetaString.convertDataFromString( inputValueEmptyString, inValueMetaString, nullIf, ifNull, trimType );
    assertEquals( "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = Y: "
      + "Conversion from empty string to string must return empty string", StringUtils.EMPTY, result );

    result =
      outValueMetaString.convertDataFromString( inputValueNullString, inValueMetaString, nullIf, ifNull, trimType );
    assertEquals( "HOP_EMPTY_STRING_DIFFERS_FROM_NULL = Y: "
      + "Conversion from null string must return empty string", StringUtils.EMPTY, result );
  }

  @Test
  public void testConvertDataFromStringToDate() throws HopValueException {
    ValueMetaBase inValueMetaString = new ValueMetaString();
    ValueMetaBase outValueMetaDate = new ValueMetaDate();
    String inputValueEmptyString = StringUtils.EMPTY;
    String nullIf = null;
    String ifNull = null;
    int trimType = 0;
    Object result;

    result =
      outValueMetaDate.convertDataFromString( inputValueEmptyString, inValueMetaString, nullIf, ifNull, trimType );
    assertEquals( "Conversion from empty string to date must return null", result, null );
  }

  @Test( expected = HopValueException.class )
  public void testConvertDataFromStringForNullMeta() throws HopValueException {
    IValueMeta valueMetaBase = new ValueMetaNone();
    String inputValueEmptyString = StringUtils.EMPTY;
    IValueMeta iValueMeta = null;
    String nullIf = null;
    String ifNull = null;
    int trimType = 0;

    valueMetaBase.convertDataFromString( inputValueEmptyString, iValueMeta, nullIf, ifNull, trimType );
  }

  @Test( expected = HopValueException.class )
  public void testGetBigDecimalThrowsHopValueException() throws HopValueException {
    ValueMetaBase valueMeta = new ValueMetaBigNumber();
    valueMeta.getBigNumber( "1234567890" );
  }

  @Test( expected = HopValueException.class )
  public void testGetIntegerThrowsHopValueException() throws HopValueException {
    ValueMetaBase valueMeta = new ValueMetaInteger();
    valueMeta.getInteger( "1234567890" );
  }

  @Test( expected = HopValueException.class )
  public void testGetNumberThrowsHopValueException() throws HopValueException {
    ValueMetaBase valueMeta = new ValueMetaNumber();
    valueMeta.getNumber( "1234567890" );
  }

  @Test
  public void testIsNumeric() {
    int[] numTypes = { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER };
    for ( int type : numTypes ) {
      assertTrue( Integer.toString( type ), ValueMetaBase.isNumeric( type ) );
    }

    int[] notNumTypes = { IValueMeta.TYPE_INET, IValueMeta.TYPE_BOOLEAN, IValueMeta.TYPE_BINARY, IValueMeta.TYPE_DATE, IValueMeta.TYPE_STRING };
    for ( int type : notNumTypes ) {
      assertFalse( Integer.toString( type ), ValueMetaBase.isNumeric( type ) );
    }
  }

  @Test
  public void testGetAllTypes() {
    assertArrayEquals( ValueMetaBase.getAllTypes(), ValueMetaFactory.getAllValueMetaNames() );
  }

  @Test
  public void testGetTrimTypeByCode() {
    assertEquals( ValueMetaBase.getTrimTypeByCode( "none" ), IValueMeta.TRIM_TYPE_NONE );
    assertEquals( ValueMetaBase.getTrimTypeByCode( "left" ), IValueMeta.TRIM_TYPE_LEFT );
    assertEquals( ValueMetaBase.getTrimTypeByCode( "right" ), IValueMeta.TRIM_TYPE_RIGHT );
    assertEquals( ValueMetaBase.getTrimTypeByCode( "both" ), IValueMeta.TRIM_TYPE_BOTH );
    assertEquals( ValueMetaBase.getTrimTypeByCode( null ), IValueMeta.TRIM_TYPE_NONE );
    assertEquals( ValueMetaBase.getTrimTypeByCode( "" ), IValueMeta.TRIM_TYPE_NONE );
    assertEquals( ValueMetaBase.getTrimTypeByCode( "fake" ), IValueMeta.TRIM_TYPE_NONE );
  }

  @Test
  public void testGetTrimTypeCode() {
    assertEquals( ValueMetaBase.getTrimTypeCode( IValueMeta.TRIM_TYPE_NONE ), "none" );
    assertEquals( ValueMetaBase.getTrimTypeCode( IValueMeta.TRIM_TYPE_LEFT ), "left" );
    assertEquals( ValueMetaBase.getTrimTypeCode( IValueMeta.TRIM_TYPE_RIGHT ), "right" );
    assertEquals( ValueMetaBase.getTrimTypeCode( IValueMeta.TRIM_TYPE_BOTH ), "both" );
  }

  @Test
  public void testGetTrimTypeByDesc() {
    assertEquals( ValueMetaBase.getTrimTypeByDesc( BaseMessages.getString( PKG, "ValueMeta.TrimType.None" ) ),
      IValueMeta.TRIM_TYPE_NONE );
    assertEquals( ValueMetaBase.getTrimTypeByDesc( BaseMessages.getString( PKG, "ValueMeta.TrimType.Left" ) ),
      IValueMeta.TRIM_TYPE_LEFT );
    assertEquals( ValueMetaBase.getTrimTypeByDesc( BaseMessages.getString( PKG, "ValueMeta.TrimType.Right" ) ),
      IValueMeta.TRIM_TYPE_RIGHT );
    assertEquals( ValueMetaBase.getTrimTypeByDesc( BaseMessages.getString( PKG, "ValueMeta.TrimType.Both" ) ),
      IValueMeta.TRIM_TYPE_BOTH );
    assertEquals( ValueMetaBase.getTrimTypeByDesc( null ), IValueMeta.TRIM_TYPE_NONE );
    assertEquals( ValueMetaBase.getTrimTypeByDesc( "" ), IValueMeta.TRIM_TYPE_NONE );
    assertEquals( ValueMetaBase.getTrimTypeByDesc( "fake" ), IValueMeta.TRIM_TYPE_NONE );
  }

  @Test
  public void testGetTrimTypeDesc() {
    assertEquals( ValueMetaBase.getTrimTypeDesc( IValueMeta.TRIM_TYPE_NONE ), BaseMessages.getString( PKG,
      "ValueMeta.TrimType.None" ) );
    assertEquals( ValueMetaBase.getTrimTypeDesc( IValueMeta.TRIM_TYPE_LEFT ), BaseMessages.getString( PKG,
      "ValueMeta.TrimType.Left" ) );
    assertEquals( ValueMetaBase.getTrimTypeDesc( IValueMeta.TRIM_TYPE_RIGHT ), BaseMessages.getString( PKG,
      "ValueMeta.TrimType.Right" ) );
    assertEquals( ValueMetaBase.getTrimTypeDesc( IValueMeta.TRIM_TYPE_BOTH ), BaseMessages.getString( PKG,
      "ValueMeta.TrimType.Both" ) );
    assertEquals( ValueMetaBase.getTrimTypeDesc( -1 ), BaseMessages.getString( PKG, "ValueMeta.TrimType.None" ) );
    assertEquals( ValueMetaBase.getTrimTypeDesc( 10000 ), BaseMessages.getString( PKG, "ValueMeta.TrimType.None" ) );
  }

  @Test
  public void testOrigin() {
    ValueMetaBase base = new ValueMetaBase();
    base.setOrigin( "myOrigin" );
    assertEquals( base.getOrigin(), "myOrigin" );
    base.setOrigin( null );
    assertNull( base.getOrigin() );
    base.setOrigin( "" );
    assertEquals( base.getOrigin(), "" );
  }

  @Test
  public void testName() {
    ValueMetaBase base = new ValueMetaBase();
    base.setName( "myName" );
    assertEquals( base.getName(), "myName" );
    base.setName( null );
    assertNull( base.getName() );
    base.setName( "" );
    assertEquals( base.getName(), "" );

  }

  @Test
  public void testLength() {
    ValueMetaBase base = new ValueMetaBase();
    base.setLength( 6 );
    assertEquals( base.getLength(), 6 );
    base.setLength( -1 );
    assertEquals( base.getLength(), -1 );
  }

  @Test
  public void testPrecision() {
    ValueMetaBase base = new ValueMetaBase();
    base.setPrecision( 6 );
    assertEquals( base.getPrecision(), 6 );
    base.setPrecision( -1 );
    assertEquals( base.getPrecision(), -1 );
  }

  @Test
  public void testCompareIntegers() throws HopValueException {
    ValueMetaBase intMeta = new ValueMetaInteger( "int" );
    Long int1 = new Long( 6223372036854775804L );
    Long int2 = new Long( -6223372036854775804L );
    assertEquals( 1, intMeta.compare( int1, int2 ) );
    assertEquals( -1, intMeta.compare( int2, int1 ) );
    assertEquals( 0, intMeta.compare( int1, int1 ) );
    assertEquals( 0, intMeta.compare( int2, int2 ) );

    int1 = new Long( 9223372036854775804L );
    int2 = new Long( -9223372036854775804L );
    assertEquals( 1, intMeta.compare( int1, int2 ) );
    assertEquals( -1, intMeta.compare( int2, int1 ) );
    assertEquals( 0, intMeta.compare( int1, int1 ) );
    assertEquals( 0, intMeta.compare( int2, int2 ) );

    int1 = new Long( 6223372036854775804L );
    int2 = new Long( -9223372036854775804L );
    assertEquals( 1, intMeta.compare( int1, int2 ) );
    assertEquals( -1, intMeta.compare( int2, int1 ) );
    assertEquals( 0, intMeta.compare( int1, int1 ) );

    int1 = new Long( 9223372036854775804L );
    int2 = new Long( -6223372036854775804L );
    assertEquals( 1, intMeta.compare( int1, int2 ) );
    assertEquals( -1, intMeta.compare( int2, int1 ) );
    assertEquals( 0, intMeta.compare( int1, int1 ) );

    int1 = null;
    int2 = new Long( 6223372036854775804L );
    assertEquals( -1, intMeta.compare( int1, int2 ) );
    intMeta.setSortedDescending( true );
    assertEquals( 1, intMeta.compare( int1, int2 ) );

  }

  @Test
  public void testCompareIntegerToDouble() throws HopValueException {
    IValueMeta intMeta = new ValueMetaInteger( "int" );
    Long int1 = new Long( 2L );
    IValueMeta numberMeta = new ValueMetaNumber( "number" );
    Double double2 = new Double( 1.5 );
    assertEquals( 1, intMeta.compare( int1, numberMeta, double2 ) );
  }

  @Test
  public void testCompareDate() throws HopValueException {
    IValueMeta dateMeta = new ValueMetaDate( "int" );
    Date date1 = new Date( 6223372036854775804L );
    Date date2 = new Date( -6223372036854775804L );
    assertEquals( 1, dateMeta.compare( date1, date2 ) );
    assertEquals( -1, dateMeta.compare( date2, date1 ) );
    assertEquals( 0, dateMeta.compare( date1, date1 ) );
  }

  @Test
  public void testCompareDateWithStorageMask() throws HopValueException {
    IValueMeta storageMeta = new ValueMetaString( "string" );
    storageMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
    storageMeta.setConversionMask( "MM/dd/yyyy HH:mm" );

    IValueMeta dateMeta = new ValueMetaDate( "date" );
    dateMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    dateMeta.setStorageMetadata( storageMeta );
    dateMeta.setConversionMask( "yyyy-MM-dd" );

    IValueMeta targetDateMeta = new ValueMetaDate( "date" );
    targetDateMeta.setConversionMask( "yyyy-MM-dd" );
    targetDateMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );

    String date = "2/24/2017 0:00";

    Date equalDate = new GregorianCalendar( 2017, Calendar.FEBRUARY, 24 ).getTime();
    assertEquals( 0, dateMeta.compare( date.getBytes(), targetDateMeta, equalDate ) );

    Date pastDate = new GregorianCalendar( 2017, Calendar.JANUARY, 24 ).getTime();
    assertEquals( 1, dateMeta.compare( date.getBytes(), targetDateMeta, pastDate ) );

    Date futureDate = new GregorianCalendar( 2017, Calendar.MARCH, 24 ).getTime();
    assertEquals( -1, dateMeta.compare( date.getBytes(), targetDateMeta, futureDate ) );
  }

  @Test
  public void testCompareDateNoStorageMask() throws HopValueException {
    IValueMeta storageMeta = new ValueMetaString( "string" );
    storageMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
    storageMeta.setConversionMask( null ); // explicit set to null, to make sure test condition are met

    IValueMeta dateMeta = new ValueMetaDate( "date" );
    dateMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    dateMeta.setStorageMetadata( storageMeta );
    dateMeta.setConversionMask( "yyyy-MM-dd" );

    IValueMeta targetDateMeta = new ValueMetaDate( "date" );
    //targetDateMeta.setConversionMask( "yyyy-MM-dd" ); by not setting a maks, the default one is used
    //and since this is a date of normal storage it should work
    targetDateMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );

    String date = "2017/02/24 00:00:00.000";

    Date equalDate = new GregorianCalendar( 2017, Calendar.FEBRUARY, 24 ).getTime();
    assertEquals( 0, dateMeta.compare( date.getBytes(), targetDateMeta, equalDate ) );

    Date pastDate = new GregorianCalendar( 2017, Calendar.JANUARY, 24 ).getTime();
    assertEquals( 1, dateMeta.compare( date.getBytes(), targetDateMeta, pastDate ) );

    Date futureDate = new GregorianCalendar( 2017, Calendar.MARCH, 24 ).getTime();
    assertEquals( -1, dateMeta.compare( date.getBytes(), targetDateMeta, futureDate ) );
  }

  @Test
  public void testCompareBinary() throws HopValueException {
    IValueMeta dateMeta = new ValueMetaBinary( "int" );
    byte[] value1 = new byte[] { 0, 1, 0, 0, 0, 1 };
    byte[] value2 = new byte[] { 0, 1, 0, 0, 0, 0 };
    assertEquals( 1, dateMeta.compare( value1, value2 ) );
    assertEquals( -1, dateMeta.compare( value2, value1 ) );
    assertEquals( 0, dateMeta.compare( value1, value1 ) );
  }

  @Test
  public void testDateParsing8601() throws Exception {
    ValueMetaDate dateMeta = new ValueMetaDate( "date" );
    dateMeta.setDateFormatLenient( false );

    // try to convert date by 'start-of-date' make - old behavior
    dateMeta.setConversionMask( "yyyy-MM-dd" );
    assertEquals( local( 1918, 3, 25, 0, 0, 0, 0 ), dateMeta.convertStringToDate( "1918-03-25T07:40:03.012+03:00" ) );

    // convert ISO-8601 date - supported since Java 7
    dateMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" );
    assertEquals( utc( 1918, 3, 25, 5, 10, 3, 12 ), dateMeta.convertStringToDate( "1918-03-25T07:40:03.012+02:30" ) );
    assertEquals( utc( 1918, 3, 25, 7, 40, 3, 12 ), dateMeta.convertStringToDate( "1918-03-25T07:40:03.012Z" ) );

    // convert date
    dateMeta.setConversionMask( "yyyy-MM-dd" );
    assertEquals( local( 1918, 3, 25, 0, 0, 0, 0 ), dateMeta.convertStringToDate( "1918-03-25" ) );
    // convert date with spaces at the end
    assertEquals( local( 1918, 3, 25, 0, 0, 0, 0 ), dateMeta.convertStringToDate( "1918-03-25  \n" ) );
  }

  @Test
  public void testDateToStringParse() throws Exception {
    ValueMetaBase dateMeta = new ValueMetaString( "date" );
    dateMeta.setDateFormatLenient( false );

    // try to convert date by 'start-of-date' make - old behavior
    dateMeta.setConversionMask( "yyyy-MM-dd" );
    assertEquals( local( 1918, 3, 25, 0, 0, 0, 0 ), dateMeta.convertStringToDate( "1918-03-25T07:40:03.012+03:00" ) );
  }

  @Test
  public void testSetPreparedStatementStringValueDontLogTruncated() throws HopDatabaseException {
    ValueMetaBase valueMetaString = new ValueMetaString( "LOG_FIELD", LOG_FIELD.length(), 0 );

    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    PreparedStatement preparedStatement = mock( PreparedStatement.class );
    when( databaseMeta.getMaxTextFieldLength() ).thenReturn( LOG_FIELD.length() );
    List<HopLoggingEvent> events = listener.getEvents();
    assertEquals( 0, events.size() );

    valueMetaString.setPreparedStatementValue( databaseMeta, preparedStatement, 0, LOG_FIELD );

    //no logging occurred as max string length equals to logging text length
    assertEquals( 0, events.size() );
  }

  @Test
  public void testValueMetaBaseOnlyHasOneLogger() throws NoSuchFieldException, IllegalAccessException {
    Field log = ValueMetaBase.class.getDeclaredField( "log" );
    assertTrue( Modifier.isStatic( log.getModifiers() ) );
    assertTrue( Modifier.isFinal( log.getModifiers() ) );
    log.setAccessible( true );
    try {
      assertEquals( LoggingRegistry.getInstance().findExistingLoggingSource( new LoggingObject( "ValueMetaBase" ) )
          .getLogChannelId(),
        ( (ILogChannel) log.get( null ) ).getLogChannelId() );
    } finally {
      log.setAccessible( false );
    }
  }

  Date local( int year, int month, int dat, int hrs, int min, int sec, int ms ) {
    GregorianCalendar cal = new GregorianCalendar( year, month - 1, dat, hrs, min, sec );
    cal.set( Calendar.MILLISECOND, ms );
    return cal.getTime();
  }

  Date utc( int year, int month, int dat, int hrs, int min, int sec, int ms ) {
    GregorianCalendar cal = new GregorianCalendar( year, month - 1, dat, hrs, min, sec );
    cal.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
    cal.set( Calendar.MILLISECOND, ms );
    return cal.getTime();
  }

  @Test
  public void testGetNativeDataTypeClass() {
    IValueMeta base = new ValueMetaBase();
    Class<?> clazz = null;
    try {
      clazz = base.getNativeDataTypeClass();
      fail();
    } catch ( HopValueException expected ) {
      // ValueMetaBase should throw an exception, as all sub-classes should override
      assertNull( clazz );
    }
  }

  @Test
  public void testConvertDataUsingConversionMetaDataForCustomMeta() {
    ValueMetaBase baseMeta = new ValueMetaString( "CUSTOM_VALUEMETA_STRING" );
    baseMeta.setConversionMetadata( new ValueMetaBase( "CUSTOM", 999 ) );
    Object customData = new Object();
    try {
      baseMeta.convertDataUsingConversionMetaData( customData );
      fail( "Should have thrown a Hop Value Exception with a proper message. Not a NPE stack trace" );
    } catch ( HopValueException e ) {
      String expectedMessage = "CUSTOM_VALUEMETA_STRING String : I can't convert the specified value to data type : 999";
      assertEquals( expectedMessage, e.getMessage().trim() );
    }
  }

  @Test
  public void testConvertDataUsingConversionMetaData() throws HopValueException, ParseException {
    ValueMetaString base = new ValueMetaString();
    double DELTA = 1e-15;

    base.setConversionMetadata( new ValueMetaString( "STRING" ) );
    Object defaultStringData = "STRING DATA";
    String convertedStringData = (String) base.convertDataUsingConversionMetaData( defaultStringData );
    assertEquals( "STRING DATA", convertedStringData );

    base.setConversionMetadata( new ValueMetaInteger( "INTEGER" ) );
    Object defaultIntegerData = "1";
    long convertedIntegerData = (long) base.convertDataUsingConversionMetaData( defaultIntegerData );
    assertEquals( 1, convertedIntegerData );


    base.setConversionMetadata( new ValueMetaNumber( "NUMBER" ) );
    Object defaultNumberData = "1.999";
    double convertedNumberData = (double) base.convertDataUsingConversionMetaData( defaultNumberData );
    assertEquals( 1.999, convertedNumberData, DELTA );

    IValueMeta dateConversionMeta = new ValueMetaDate( "DATE" );
    dateConversionMeta.setDateFormatTimeZone( TimeZone.getTimeZone( "CST" ) );
    base.setConversionMetadata( dateConversionMeta );
    Object defaultDateData = "1990/02/18 00:00:00.000";
    Date date1 = new Date( 635320800000L );
    Date convertedDateData = (Date) base.convertDataUsingConversionMetaData( defaultDateData );
    assertEquals( date1, convertedDateData );

    base.setConversionMetadata( new ValueMetaBigNumber( "BIG_NUMBER" ) );
    Object defaultBigNumber = String.valueOf( BigDecimal.ONE );
    BigDecimal convertedBigNumber = (BigDecimal) base.convertDataUsingConversionMetaData( defaultBigNumber );
    assertEquals( BigDecimal.ONE, convertedBigNumber );

    base.setConversionMetadata( new ValueMetaBoolean( "BOOLEAN" ) );
    Object defaultBoolean = "true";
    boolean convertedBoolean = (boolean) base.convertDataUsingConversionMetaData( defaultBoolean );
    assertEquals( true, convertedBoolean );
  }

  @Test
  public void testGetCompatibleString() throws HopValueException {
    ValueMetaInteger valueMetaInteger = new ValueMetaInteger( "INTEGER" );
    valueMetaInteger.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );

    assertEquals( "2", valueMetaInteger.getCompatibleString( new Long( 2 ) ) ); //BACKLOG-15750
  }

  @Test
  public void testReadDataInet() throws Exception {
    InetAddress localhost = InetAddress.getByName( "127.0.0.1" );
    byte[] address = localhost.getAddress();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream( byteArrayOutputStream );
    dataOutputStream.writeBoolean( false );
    dataOutputStream.writeInt( address.length );
    dataOutputStream.write( address );

    DataInputStream dis = new DataInputStream( new ByteArrayInputStream( byteArrayOutputStream.toByteArray() ) );
    ValueMetaBase vm = new ValueMetaInternetAddress();
    assertEquals( localhost, vm.readData( dis ) );
  }

  @Test
  public void testWriteDataInet() throws Exception {
    InetAddress localhost = InetAddress.getByName( "127.0.0.1" );
    byte[] address = localhost.getAddress();

    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
    DataOutputStream dos1 = new DataOutputStream( out1 );
    dos1.writeBoolean( false );
    dos1.writeInt( address.length );
    dos1.write( address );
    byte[] expected = out1.toByteArray();

    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
    DataOutputStream dos2 = new DataOutputStream( out2 );
    ValueMetaBase vm = new ValueMetaInternetAddress();
    vm.writeData( dos2, localhost );
    byte[] actual = out2.toByteArray();

    assertArrayEquals( expected, actual );
  }



  @Test
  public void testConvertBigNumberToBoolean() {
    ValueMetaBase vmb = new ValueMetaBase();
    assertTrue( vmb.convertBigNumberToBoolean( new BigDecimal( "-234" ) ) );
    assertTrue( vmb.convertBigNumberToBoolean( new BigDecimal( "234" ) ) );
    assertFalse( vmb.convertBigNumberToBoolean( new BigDecimal( "0" ) ) );
    assertTrue( vmb.convertBigNumberToBoolean( new BigDecimal( "1.7976E308" ) ) );
  }

  @Test
  public void testGetValueFromNode() throws Exception {

    ValueMetaBase valueMetaBase = null;
    Node xmlNode = null;

    valueMetaBase = new ValueMetaString( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data>String val</value-data>" ).getFirstChild();
    assertEquals( "String val", valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaNumber( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data>689.2</value-data>" ).getFirstChild();
    assertEquals( 689.2, valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaNumber( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data>689.2</value-data>" ).getFirstChild();
    assertEquals( 689.2, valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaInteger( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data>68933</value-data>" ).getFirstChild();
    assertEquals( 68933l, valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaDate( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data>2017/11/27 08:47:10.000</value-data>" ).getFirstChild();
    assertEquals( XmlHandler.stringToDate( "2017/11/27 08:47:10.000" ), valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaTimestamp( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data>2017/11/27 08:47:10.123456789</value-data>" ).getFirstChild();
    assertEquals( XmlHandler.stringToTimestamp( "2017/11/27 08:47:10.123456789" ), valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaBoolean( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data>Y</value-data>" ).getFirstChild();
    assertEquals( true, valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaBinary( "test" );
    byte[] bytes = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    String s = XmlHandler.encodeBinaryData( bytes );
    xmlNode = XmlHandler.loadXmlString( "<value-data>test<binary-value>" + s + "</binary-value></value-data>" ).getFirstChild();
    assertArrayEquals( bytes, (byte[]) valueMetaBase.getValue( xmlNode ) );

    valueMetaBase = new ValueMetaString( "test" );
    xmlNode = XmlHandler.loadXmlString( "<value-data></value-data>" ).getFirstChild();
    assertNull( valueMetaBase.getValue( xmlNode ) );
  }

  @Test( expected = HopException.class )
  public void testGetValueUnknownType() throws Exception {
    ValueMetaBase valueMetaBase = new ValueMetaNone( "test" );
    valueMetaBase.getValue( XmlHandler.loadXmlString( "<value-data>not empty</value-data>" ).getFirstChild() );
  }

  @Test
  public void testConvertStringToTimestampType() throws HopValueException {
    String timestampStringRepresentation = "2018/04/11 16:45:15.000000000";
    Timestamp expectedTimestamp = Timestamp.valueOf( "2018-04-11 16:45:15.000000000" );

    ValueMetaBase base = new ValueMetaString( "ValueMetaStringColumn" );
    base.setConversionMetadata( new ValueMetaTimestamp( "ValueMetaTimestamp" ) );
    Timestamp timestamp = (Timestamp) base.convertDataUsingConversionMetaData( timestampStringRepresentation );
    assertEquals( expectedTimestamp, timestamp );
  }

  @Test
  public void testConvertNumberToString() throws HopValueException {
    String expectedStringRepresentation = "123.123";
    Number numberToTest = Double.valueOf( "123.123" );

    ValueMetaBase base = new ValueMetaNumber( "ValueMetaNumber" );
    base.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );

    ValueMetaString valueMetaString = new ValueMetaString( "ValueMetaString" );
    base.setConversionMetadata( valueMetaString );

    String convertedNumber = base.convertNumberToString( (Double) numberToTest );
    assertEquals( expectedStringRepresentation, convertedNumber );
  }

  @Test
  public void testNullHashCodes() throws Exception {
    ValueMetaBase valueMetaString = new ValueMetaBase();

    valueMetaString.type = IValueMeta.TYPE_BOOLEAN;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 1 );

    valueMetaString.type = IValueMeta.TYPE_DATE;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 2 );

    valueMetaString.type = IValueMeta.TYPE_NUMBER;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 4 );

    valueMetaString.type = IValueMeta.TYPE_STRING;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 8 );

    valueMetaString.type = IValueMeta.TYPE_INTEGER;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 16 );

    valueMetaString.type = IValueMeta.TYPE_BIGNUMBER;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 32 );

    valueMetaString.type = IValueMeta.TYPE_BINARY;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 64 );

    valueMetaString.type = IValueMeta.TYPE_TIMESTAMP;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 128 );

    valueMetaString.type = IValueMeta.TYPE_INET;
    assertEquals( valueMetaString.hashCode( null ), 0 ^ 256 );

    valueMetaString.type = IValueMeta.TYPE_NONE;
    assertEquals( valueMetaString.hashCode( null ), 0 );
  }

  @Test
  public void testHashCodes() throws Exception {
    ValueMetaBase valueMetaString = new ValueMetaBase();

    valueMetaString.type = IValueMeta.TYPE_BOOLEAN;
    assertEquals( valueMetaString.hashCode( true ), 1231 );

    SimpleDateFormat sdf = new SimpleDateFormat( "dd/M/yyyy" );
    String dateInString = "1/1/2018";
    Date dateObj = sdf.parse( dateInString );
    valueMetaString.type = IValueMeta.TYPE_DATE;
    assertEquals( valueMetaString.hashCode( dateObj ), -1358655136 );

    Double numberObj = Double.valueOf( 5.1 );
    valueMetaString.type = IValueMeta.TYPE_NUMBER;
    assertEquals( valueMetaString.hashCode( numberObj ), 645005312 );

    valueMetaString.type = IValueMeta.TYPE_STRING;
    assertEquals( valueMetaString.hashCode( "test" ), 3556498 );

    Long longObj = 123L;
    valueMetaString.type = IValueMeta.TYPE_INTEGER;
    assertEquals( valueMetaString.hashCode( longObj ), 123 );

    BigDecimal bDecimalObj = new BigDecimal( 123.1 );
    valueMetaString.type = IValueMeta.TYPE_BIGNUMBER;
    assertEquals( valueMetaString.hashCode( bDecimalObj ), 465045870 );

    byte[] bBinary = new byte[ 2 ];
    bBinary[ 0 ] = 1;
    bBinary[ 1 ] = 0;
    valueMetaString.type = IValueMeta.TYPE_BINARY;
    assertEquals( valueMetaString.hashCode( bBinary ), 992 );

    Timestamp timestampObj = Timestamp.valueOf( "2018-01-01 10:10:10.000000000" );
    valueMetaString.type = IValueMeta.TYPE_TIMESTAMP;
    assertEquals( valueMetaString.hashCode( timestampObj ), -1322045776 );

    byte[] ipAddr = new byte[] { 127, 0, 0, 1 };
    InetAddress addrObj = InetAddress.getByAddress( ipAddr );
    valueMetaString.type = IValueMeta.TYPE_INET;
    assertEquals( valueMetaString.hashCode( addrObj ), 2130706433 );

    valueMetaString.type = IValueMeta.TYPE_NONE;
    assertEquals( valueMetaString.hashCode( "any" ), 0 );
  }

  @Test
  public void testMetdataPreviewSqlCharToPentahoString() throws SQLException, HopDatabaseException {
    doReturn( Types.CHAR ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
  }

  @Test
  public void testMetdataPreviewSqlVarcharToPentahoString() throws SQLException, HopDatabaseException {
    doReturn( Types.VARCHAR ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
  }

  @Test
  public void testMetdataPreviewSqlNVarcharToPentahoString() throws SQLException, HopDatabaseException {
    doReturn( Types.NVARCHAR ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
  }

  @Test
  public void testMetdataPreviewSqlLongVarcharToPentahoString() throws SQLException, HopDatabaseException {
    doReturn( Types.LONGVARCHAR ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
  }

  @Test
  public void testMetdataPreviewSqlClobToPentahoString() throws SQLException, HopDatabaseException {
    doReturn( Types.CLOB ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
    assertEquals( DatabaseMeta.CLOB_LENGTH, valueMeta.getLength() );
    assertTrue( valueMeta.isLargeTextField() );
  }

  @Test
  public void testMetdataPreviewSqlNClobToPentahoString() throws SQLException, HopDatabaseException {
    doReturn( Types.NCLOB ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
    assertEquals( DatabaseMeta.CLOB_LENGTH, valueMeta.getLength() );
    assertTrue( valueMeta.isLargeTextField() );
  }

  @Test
  public void testMetdataPreviewSqlBigIntToPentahoInteger() throws SQLException, HopDatabaseException {
    doReturn( Types.BIGINT ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isInteger() );
    assertEquals( 0, valueMeta.getPrecision() );
    assertEquals( 15, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlIntegerToPentahoInteger() throws SQLException, HopDatabaseException {
    doReturn( Types.INTEGER ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isInteger() );
    assertEquals( 0, valueMeta.getPrecision() );
    assertEquals( 9, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlSmallIntToPentahoInteger() throws SQLException, HopDatabaseException {
    doReturn( Types.SMALLINT ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isInteger() );
    assertEquals( 0, valueMeta.getPrecision() );
    assertEquals( 4, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlTinyIntToPentahoInteger() throws SQLException, HopDatabaseException {
    doReturn( Types.TINYINT ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isInteger() );
    assertEquals( 0, valueMeta.getPrecision() );
    assertEquals( 2, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlDecimalToPentahoBigNumber() throws SQLException, HopDatabaseException {
    doReturn( Types.DECIMAL ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 20 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 5 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isBigNumber() );
    assertEquals( 5, valueMeta.getPrecision() );
    assertEquals( 20, valueMeta.getLength() );

    doReturn( Types.DECIMAL ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 20 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 0 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isBigNumber() );
    assertEquals( 0, valueMeta.getPrecision() );
    assertEquals( 20, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlDecimalToPentahoInteger() throws SQLException, HopDatabaseException {
    doReturn( Types.DECIMAL ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 2 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 0 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isInteger() );
    assertEquals( 0, valueMeta.getPrecision() );
    assertEquals( 2, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlDoubleToPentahoNumber() throws SQLException, HopDatabaseException {
    doReturn( Types.DOUBLE ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 3 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 2 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isNumber() );
    assertEquals( 2, valueMeta.getPrecision() );
    assertEquals( 3, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlDoubleWithoutDecimalDigits() throws SQLException, HopDatabaseException {
    doReturn( Types.DOUBLE ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 3 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 0 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isNumber() );
    assertEquals( -1, valueMeta.getPrecision() );
    assertEquals( 3, valueMeta.getLength() );
  }


  @Ignore //TODO: test was based on oracleVariant does not change type to bignumber for generic Database
  @Test
  public void testMetdataPreviewSqlDoubleWithTooBigLengthAndPrecision() throws SQLException, HopDatabaseException {
    doReturn( Types.DOUBLE ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 128 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 127 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isBigNumber() );
    assertEquals( -1, valueMeta.getPrecision() );
    assertEquals( -1, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlDoubleToPentahoBigNumber() throws SQLException, HopDatabaseException {
    doReturn( Types.DOUBLE ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 20 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 15 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isBigNumber() );
    assertEquals( 15, valueMeta.getPrecision() );
    assertEquals( 20, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlFloatToPentahoNumber() throws SQLException, HopDatabaseException {
    doReturn( Types.FLOAT ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 3 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 2 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isNumber() );
    assertEquals( 2, valueMeta.getPrecision() );
    assertEquals( 3, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlRealToPentahoNumber() throws SQLException, HopDatabaseException {
    doReturn( Types.REAL ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 3 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 2 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isNumber() );
    assertEquals( 2, valueMeta.getPrecision() );
    assertEquals( 3, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewUnsupportedSqlTimestamp() throws SQLException, HopDatabaseException {
    doReturn( Types.TIMESTAMP ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 19 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    doReturn( false ).when( dbMeta ).supportsTimestampDataType();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( !valueMeta.isDate() );
  }

  @Test
  public void testMetdataPreviewSqlTimeToPentahoDate() throws SQLException, HopDatabaseException {
    doReturn( Types.TIME ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isDate() );
  }

  @Test
  public void testMetdataPreviewSqlBooleanToPentahoBoolean() throws SQLException, HopDatabaseException {
    doReturn( Types.BOOLEAN ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isBoolean() );
  }

  @Test
  public void testMetdataPreviewSqlBitToPentahoBoolean() throws SQLException, HopDatabaseException {
    doReturn( Types.BIT ).when( resultSet ).getInt( "DATA_TYPE" );
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( variables, dbMeta, resultSet );
    assertTrue( valueMeta.isBoolean() );
  }
}
