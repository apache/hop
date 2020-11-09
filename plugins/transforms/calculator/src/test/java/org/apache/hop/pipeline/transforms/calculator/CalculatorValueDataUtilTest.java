/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.*;
import org.apache.hop.core.util.Utils;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CalculatorValueDataUtilTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  private static String yyyy_MM_dd = "yyyy-MM-dd";

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  // private enum DateCalc {WORKING_DAYS, DATE_DIFF};

  /**
   * @throws HopValueException
   * @deprecated Use {@link Const#ltrim(String)} instead
   */
  @Deprecated
  @Test
  public void testLeftTrim() throws HopValueException {
    assertEquals( "", ValueDataUtil.leftTrim( "" ) );
    assertEquals( "string", ValueDataUtil.leftTrim( "string" ) );
    assertEquals( "string", ValueDataUtil.leftTrim( " string" ) );
    assertEquals( "string", ValueDataUtil.leftTrim( "  string" ) );
    assertEquals( "string", ValueDataUtil.leftTrim( "   string" ) );
    assertEquals( "string", ValueDataUtil.leftTrim( "     string" ) );

    assertEquals( "string ", ValueDataUtil.leftTrim( " string " ) );
    assertEquals( "string  ", ValueDataUtil.leftTrim( "  string  " ) );
    assertEquals( "string   ", ValueDataUtil.leftTrim( "   string   " ) );
    assertEquals( "string    ", ValueDataUtil.leftTrim( "    string    " ) );

    assertEquals( "", ValueDataUtil.leftTrim( " " ) );
    assertEquals( "", ValueDataUtil.leftTrim( "  " ) );
    assertEquals( "", ValueDataUtil.leftTrim( "   " ) );
  }

  /**
   * @throws HopValueException
   * @deprecated Use {@link Const#rtrim(String)} instead
   */
  @Deprecated
  @Test
  public void testRightTrim() throws HopValueException {
    assertEquals( "", ValueDataUtil.rightTrim( "" ) );
    assertEquals( "string", ValueDataUtil.rightTrim( "string" ) );
    assertEquals( "string", ValueDataUtil.rightTrim( "string " ) );
    assertEquals( "string", ValueDataUtil.rightTrim( "string  " ) );
    assertEquals( "string", ValueDataUtil.rightTrim( "string   " ) );
    assertEquals( "string", ValueDataUtil.rightTrim( "string    " ) );

    assertEquals( " string", ValueDataUtil.rightTrim( " string " ) );
    assertEquals( "  string", ValueDataUtil.rightTrim( "  string  " ) );
    assertEquals( "   string", ValueDataUtil.rightTrim( "   string   " ) );
    assertEquals( "    string", ValueDataUtil.rightTrim( "    string    " ) );

    assertEquals( "", ValueDataUtil.rightTrim( " " ) );
    assertEquals( "", ValueDataUtil.rightTrim( "  " ) );
    assertEquals( "", ValueDataUtil.rightTrim( "   " ) );
  }

  /**
   * @throws HopValueException
   * @deprecated Use {@link Const#isSpace(char)} instead
   */
  @Deprecated
  @Test
  public void testIsSpace() throws HopValueException {
    assertTrue( ValueDataUtil.isSpace( ' ' ) );
    assertTrue( ValueDataUtil.isSpace( '\t' ) );
    assertTrue( ValueDataUtil.isSpace( '\r' ) );
    assertTrue( ValueDataUtil.isSpace( '\n' ) );

    assertFalse( ValueDataUtil.isSpace( 'S' ) );
    assertFalse( ValueDataUtil.isSpace( 'b' ) );
  }

  /**
   * @throws HopValueException
   * @deprecated Use {@link Const#trim(String)} instead
   */
  @Deprecated
  @Test
  public void testTrim() throws HopValueException {
    assertEquals( "", ValueDataUtil.trim( "" ) );
    assertEquals( "string", ValueDataUtil.trim( "string" ) );
    assertEquals( "string", ValueDataUtil.trim( "string " ) );
    assertEquals( "string", ValueDataUtil.trim( "string  " ) );
    assertEquals( "string", ValueDataUtil.trim( "string   " ) );
    assertEquals( "string", ValueDataUtil.trim( "string    " ) );

    assertEquals( "string", ValueDataUtil.trim( " string " ) );
    assertEquals( "string", ValueDataUtil.trim( "  string  " ) );
    assertEquals( "string", ValueDataUtil.trim( "   string   " ) );
    assertEquals( "string", ValueDataUtil.trim( "    string    " ) );

    assertEquals( "string", ValueDataUtil.trim( " string" ) );
    assertEquals( "string", ValueDataUtil.trim( "  string" ) );
    assertEquals( "string", ValueDataUtil.trim( "   string" ) );
    assertEquals( "string", ValueDataUtil.trim( "    string" ) );

    assertEquals( "", ValueDataUtil.rightTrim( " " ) );
    assertEquals( "", ValueDataUtil.rightTrim( "  " ) );
    assertEquals( "", ValueDataUtil.rightTrim( "   " ) );
  }

  @Test
  public void testDateDiff_A_GT_B() {
    Object daysDiff =
      calculate( "2010-05-12", "2010-01-01", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CALC_DATE_DIFF );
    assertEquals( new Long( 131 ), daysDiff );
  }

  @Test
  public void testDateDiff_A_LT_B() {
    Object daysDiff =
      calculate( "2010-12-31", "2011-02-10", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CALC_DATE_DIFF );
    assertEquals( new Long( -41 ), daysDiff );
  }

  @Test
  public void testWorkingDaysDays_A_GT_B() {
    Object daysDiff =
      calculate( "2010-05-12", "2010-01-01", IValueMeta.TYPE_DATE,
        CalculatorMetaFunction.CALC_DATE_WORKING_DIFF );
    assertEquals( new Long( 94 ), daysDiff );
  }

  @Test
  public void testWorkingDaysDays_A_LT_B() {
    Object daysDiff =
      calculate( "2010-12-31", "2011-02-10", IValueMeta.TYPE_DATE,
        CalculatorMetaFunction.CALC_DATE_WORKING_DIFF );
    assertEquals( new Long( -30 ), daysDiff );
  }

  @Test
  public void testPlus() throws HopValueException {

    long longValue = 1;

    assertEquals( longValue, ValueDataUtil.plus( new ValueMetaInteger(), longValue, new ValueMetaString(),
      StringUtils.EMPTY ) );

  }

  @Test
  public void checksumTest() {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    String checksum = ValueDataUtil.createChecksum( new ValueMetaString(), path, "MD5" );
    assertEquals( "098f6bcd4621d373cade4e832627b4f6", checksum );
  }

  @Test
  public void checksumMissingFileTest() {
    String nonExistingFile = "nonExistingFile";
    String checksum = ValueDataUtil.createChecksum( new ValueMetaString(), nonExistingFile, "MD5" );
    assertNull( checksum );
  }

  @Test
  public void checksumNullPathTest() {
    String nonExistingFile = "nonExistingFile";
    String checksum = ValueDataUtil.createChecksum( new ValueMetaString(), nonExistingFile, "MD5" );
    assertNull( checksum );
  }

  @Test
  public void checksumWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    String checksum = ValueDataUtil.createChecksum( new ValueMetaString(), path, "MD5", true );
    assertEquals( "098f6bcd4621d373cade4e832627b4f6", checksum );
  }

  @Test
  public void checksumWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    String checksum = ValueDataUtil.createChecksum( new ValueMetaString(), path, "MD5", false );
    assertEquals( "098f6bcd4621d373cade4e832627b4f6", checksum );
  }

  @Test
  public void checksumNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingFile = "nonExistingFile";
    String checksum = ValueDataUtil.createChecksum( new ValueMetaString(), nonExistingFile, "MD5", false );
    assertNull( checksum );
  }

  @Test( expected = HopFileNotFoundException.class )
  public void checksumFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.createChecksum( new ValueMetaString(), nonExistingPath, "MD5", true );
  }

  @Test
  public void checksumNullPathNoFailTest() throws HopFileNotFoundException {
    assertNull( ValueDataUtil.createChecksum( new ValueMetaString(), null, "MD5", false ) );
  }

  @Test
  public void checksumNullPathFailTest() throws HopFileNotFoundException {
    assertNull( ValueDataUtil.createChecksum( new ValueMetaString(), null, "MD5", true ) );
  }

  @Test
  public void checksumCRC32Test() {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    long checksum = ValueDataUtil.ChecksumCRC32( new ValueMetaString(), path );
    assertEquals( 3632233996l, checksum );
  }

  @Test
  public void checksumCRC32MissingFileTest() {
    String nonExistingFile = "nonExistingFile";
    long checksum = ValueDataUtil.ChecksumCRC32( new ValueMetaString(), nonExistingFile );
    assertEquals( 0, checksum );
  }

  @Test
  public void checksumCRC32NullPathTest() throws Exception {
    String nonExistingFile = "nonExistingFile";
    long checksum = ValueDataUtil.ChecksumCRC32( new ValueMetaString(), nonExistingFile );
    assertEquals( 0, checksum );
  }

  @Test
  public void checksumCRC32WithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    long checksum = ValueDataUtil.checksumCRC32( new ValueMetaString(), path, false );
    assertEquals( 3632233996l, checksum );
  }

  @Test
  public void checksumCRC32NoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    long checksum = ValueDataUtil.checksumCRC32( new ValueMetaString(), nonExistingPath, false );
    assertEquals( 0, checksum );
  }

  @Test( expected = HopFileNotFoundException.class )
  public void checksumCRC32FailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.checksumCRC32( new ValueMetaString(), nonExistingPath, true );
  }

  @Test
  public void checksumCRC32NullPathNoFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumCRC32( new ValueMetaString(), null, false );
    assertEquals( 0, checksum );
  }

  @Test
  public void checksumCRC32NullPathFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumCRC32( new ValueMetaString(), null, true );
    assertEquals( 0, checksum );
  }

  @Test
  public void checksumAdlerTest() {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    long checksum = ValueDataUtil.ChecksumAdler32( new ValueMetaString(), path );
    assertEquals( 73204161L, checksum );
  }

  @Test
  public void checksumAdlerMissingFileTest() {
    String nonExistingFile = "nonExistingFile";
    long checksum = ValueDataUtil.ChecksumAdler32( new ValueMetaString(), nonExistingFile );
    assertEquals( 0, checksum );
  }

  @Test
  public void checksumAdlerNullPathTest() {
    String nonExistingFile = "nonExistingFile";
    long checksum = ValueDataUtil.ChecksumAdler32( new ValueMetaString(), nonExistingFile );
    assertEquals( 0, checksum );
  }

  @Test
  public void checksumAdlerWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    long checksum = ValueDataUtil.checksumAdler32( new ValueMetaString(), path, true );
    assertEquals( 73204161L, checksum );
  }

  @Test
  public void checksumAdlerWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    long checksum = ValueDataUtil.checksumAdler32( new ValueMetaString(), path, false );
    assertEquals( 73204161L, checksum );
  }

  @Test
  public void checksumAdlerNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    long checksum = ValueDataUtil.checksumAdler32( new ValueMetaString(), nonExistingPath, false );
    assertEquals( 0, checksum );
  }

  @Test( expected = HopFileNotFoundException.class )
  public void checksumAdlerFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.checksumAdler32( new ValueMetaString(), nonExistingPath, true );
  }

  @Test
  public void checksumAdlerNullPathNoFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumAdler32( new ValueMetaString(), null, false );
    assertEquals( 0, checksum );
  }

  @Test
  public void checksumAdlerNullPathFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumAdler32( new ValueMetaString(), null, true );
    assertEquals( 0, checksum );
  }

  @Test
  public void xmlFileWellFormedTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource( "xml-sample.xml" ).getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), xmlFilePath, true );
    assertTrue( wellFormed );
  }

  @Test
  public void xmlFileBadlyFormedTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource( "invalid-xml-sample.xml" ).getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), invalidXmlFilePath, true );
    assertFalse( wellFormed );
  }

  @Test
  public void xmlFileWellFormedWithFailIfNoFileTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource( "xml-sample.xml" ).getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), xmlFilePath, true );
    assertTrue( wellFormed );
  }

  @Test
  public void xmlFileWellFormedWithoutFailIfNoFileTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource( "xml-sample.xml" ).getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), xmlFilePath, false );
    assertTrue( wellFormed );
  }

  @Test
  public void xmlFileBadlyFormedWithFailIfNoFileTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource( "invalid-xml-sample.xml" ).getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), invalidXmlFilePath, true );
    assertFalse( wellFormed );
  }

  @Test
  public void xmlFileBadlyFormedWithNoFailIfNoFileTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource( "invalid-xml-sample.xml" ).getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), invalidXmlFilePath, false );
    assertFalse( wellFormed );
  }

  @Test
  public void xmlFileWellFormedNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), nonExistingPath, false );
    assertFalse( wellFormed );
  }

  @Test( expected = HopFileNotFoundException.class )
  public void xmlFileWellFormedFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), nonExistingPath, true );
  }

  @Test
  public void xmlFileWellFormedNullPathNoFailTest() throws HopFileNotFoundException {
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), null, false );
    assertFalse( wellFormed );
  }

  @Test
  public void xmlFileWellFormedNullPathFailTest() throws HopFileNotFoundException {
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), null, true );
    assertFalse( wellFormed );
  }

  @Test
  public void loadFileContentInBinary() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    byte[] content = ValueDataUtil.loadFileContentInBinary( new ValueMetaString(), path, true );
    assertTrue( Arrays.equals( "test".getBytes(), content ) );
  }

  @Test
  public void loadFileContentInBinaryNoFailIfNoFileTest() throws Exception {
    String nonExistingPath = "nonExistingPath";
    assertNull( ValueDataUtil.loadFileContentInBinary( new ValueMetaString(), nonExistingPath, false ) );
  }

  @Test( expected = HopFileNotFoundException.class )
  public void loadFileContentInBinaryFailIfNoFileTest() throws HopFileNotFoundException, HopValueException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.loadFileContentInBinary( new ValueMetaString(), nonExistingPath, true );
  }

  @Test
  public void loadFileContentInBinaryNullPathNoFailTest() throws Exception {
    assertNull( ValueDataUtil.loadFileContentInBinary( new ValueMetaString(), null, false ) );
  }

  @Test
  public void loadFileContentInBinaryNullPathFailTest() throws HopFileNotFoundException, HopValueException {
    assertNull( ValueDataUtil.loadFileContentInBinary( new ValueMetaString(), null, true ) );
  }

  @Test
  public void getFileEncodingTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    String encoding = ValueDataUtil.getFileEncoding( new ValueMetaString(), path );
    assertEquals( "US-ASCII", encoding );
  }

  @Test( expected = HopValueException.class )
  public void getFileEncodingMissingFileTest() throws HopValueException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.getFileEncoding( new ValueMetaString(), nonExistingPath );
  }

  @Test
  public void getFileEncodingNullPathTest() throws Exception {
    assertNull( ValueDataUtil.getFileEncoding( new ValueMetaString(), null ) );
  }

  @Test
  public void getFileEncodingWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    String encoding = ValueDataUtil.getFileEncoding( new ValueMetaString(), path, true );
    assertEquals( "US-ASCII", encoding );
  }

  @Test
  public void getFileEncodingWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource( "txt-sample.txt" ).getPath();
    String encoding = ValueDataUtil.getFileEncoding( new ValueMetaString(), path, false );
    assertEquals( "US-ASCII", encoding );
  }

  @Test
  public void getFileEncodingNoFailIfNoFileTest() throws Exception {
    String nonExistingPath = "nonExistingPath";
    String encoding = ValueDataUtil.getFileEncoding( new ValueMetaString(), nonExistingPath, false );
    assertNull( encoding );
  }

  @Test( expected = HopFileNotFoundException.class )
  public void getFileEncodingFailIfNoFileTest() throws HopFileNotFoundException, HopValueException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.getFileEncoding( new ValueMetaString(), nonExistingPath, true );
  }

  @Test
  public void getFileEncodingNullPathNoFailTest() throws Exception {
    String encoding = ValueDataUtil.getFileEncoding( new ValueMetaString(), null, false );
    assertNull( encoding );
  }

  @Test
  public void getFileEncodingNullPathFailTest() throws HopFileNotFoundException, HopValueException {
    String encoding = ValueDataUtil.getFileEncoding( new ValueMetaString(), null, true );
    assertNull( encoding );
  }

  @Test
  public void testAdd() {

    // Test Hop number types
    assertEquals( Double.valueOf( "3.0" ), calculate( "1", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD ) );
    assertEquals( Double.valueOf( "0.0" ), calculate( "2", "-2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD ) );
    assertEquals( Double.valueOf( "30.0" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD ) );
    assertEquals( Double.valueOf( "-50.0" ), calculate( "-100", "50", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "3" ), calculate( "1", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD ) );
    assertEquals( Long.valueOf( "0" ), calculate( "2", "-2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD ) );
    assertEquals( Long.valueOf( "30" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD ) );
    assertEquals( Long.valueOf( "-50" ), calculate( "-100", "50", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD ) );

    // Test Hop big Number types
    assertEquals( 0, new BigDecimal( "2.0" ).compareTo( (BigDecimal) calculate( "1", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD ) ) );
    assertEquals( 0, new BigDecimal( "0.0" ).compareTo( (BigDecimal) calculate( "2", "-2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD ) ) );
    assertEquals( 0, new BigDecimal( "30.0" ).compareTo( (BigDecimal) calculate( "10", "20",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD ) ) );
    assertEquals( 0, new BigDecimal( "-50.0" ).compareTo( (BigDecimal) calculate( "-100", "50",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD ) ) );
  }

  @Test
  public void testAdd3() {

    // Test Hop number types
    assertEquals( Double.valueOf( "6.0" ), calculate( "1", "2", "3", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD3 ) );
    assertEquals( Double.valueOf( "10.0" ), calculate( "2", "-2", "10", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD3 ) );
    assertEquals( Double.valueOf( "27.0" ), calculate( "10", "20", "-3", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD3 ) );
    assertEquals( Double.valueOf( "-55.0" ), calculate( "-100", "50", "-5", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ADD3 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "3" ), calculate( "1", "1", "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD3 ) );
    assertEquals( Long.valueOf( "10" ), calculate( "2", "-2", "10", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD3 ) );
    assertEquals( Long.valueOf( "27" ), calculate( "10", "20", "-3", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD3 ) );
    assertEquals( Long.valueOf( "-55" ), calculate( "-100", "50", "-5", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ADD3 ) );

    // Test Hop big Number types
    assertEquals( 0, new BigDecimal( "6.0" ).compareTo( (BigDecimal) calculate( "1", "2", "3",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD3 ) ) );
    assertEquals( 0, new BigDecimal( "10.0" ).compareTo( (BigDecimal) calculate( "2", "-2", "10",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD3 ) ) );
    assertEquals( 0, new BigDecimal( "27.0" ).compareTo( (BigDecimal) calculate( "10", "20", "-3",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD3 ) ) );
    assertEquals( 0, new BigDecimal( "-55.0" ).compareTo( (BigDecimal) calculate( "-100", "50", "-5",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ADD3 ) ) );
  }

  @Test
  public void testSubtract() {

    // Test Hop number types
    assertEquals( Double.valueOf( "10.0" ), calculate( "20", "10", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_SUBTRACT ) );
    assertEquals( Double.valueOf( "-10.0" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_SUBTRACT ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "10" ), calculate( "20", "10", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_SUBTRACT ) );
    assertEquals( Long.valueOf( "-10" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_SUBTRACT ) );

    // Test Hop big Number types
    assertEquals( 0, new BigDecimal( "10" ).compareTo( (BigDecimal) calculate( "20", "10",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_SUBTRACT ) ) );
    assertEquals( 0, new BigDecimal( "-10" ).compareTo( (BigDecimal) calculate( "10", "20",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_SUBTRACT ) ) );
  }

  @Test
  public void testDivide() {

    // Test Hop number types
    assertEquals( Double.valueOf( "2.0" ), calculate( "2", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( Double.valueOf( "2.0" ), calculate( "4", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( Double.valueOf( "0.5" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( Double.valueOf( "2.0" ), calculate( "100", "50", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_DIVIDE ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "2" ), calculate( "2", "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( Long.valueOf( "2" ), calculate( "4", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( Long.valueOf( "0" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( Long.valueOf( "2" ), calculate( "100", "50", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_DIVIDE ) );

    // Test Hop big Number types
    assertEquals( BigDecimal.valueOf( Long.valueOf( "2" ) ), calculate( "2", "1", IValueMeta.TYPE_BIGNUMBER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "2" ) ), calculate( "4", "2", IValueMeta.TYPE_BIGNUMBER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "0.5" ) ), calculate( "10", "20",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_DIVIDE ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "2" ) ), calculate( "100", "50", IValueMeta.TYPE_BIGNUMBER,
      CalculatorMetaFunction.CALC_DIVIDE ) );
  }

  @Test
  public void testMulitplyBigNumbers() throws Exception {
    BigDecimal field1 = new BigDecimal( "123456789012345678901.1234567890123456789" );
    BigDecimal field2 = new BigDecimal( "1.0" );
    BigDecimal field3 = new BigDecimal( "2.0" );

    BigDecimal expResult1 = new BigDecimal( "123456789012345678901.1234567890123456789" );
    BigDecimal expResult2 = new BigDecimal( "246913578024691357802.2469135780246913578" );

    BigDecimal expResult3 = new BigDecimal( "123456789012345678901.1200000000000000000" );
    BigDecimal expResult4 = new BigDecimal( "246913578024691357802" );

    assertEquals( expResult1, ValueDataUtil.multiplyBigDecimals( field1, field2, null ) );
    assertEquals( expResult2, ValueDataUtil.multiplyBigDecimals( field1, field3, null ) );

    assertEquals( expResult3, ValueDataUtil.multiplyBigDecimals( field1, field2, new MathContext( 23 ) ) );
    assertEquals( expResult4, ValueDataUtil.multiplyBigDecimals( field1, field3, new MathContext( 21 ) ) );
  }

  @Test
  public void testDivisionBigNumbers() throws Exception {
    BigDecimal field1 = new BigDecimal( "123456789012345678901.1234567890123456789" );
    BigDecimal field2 = new BigDecimal( "1.0" );
    BigDecimal field3 = new BigDecimal( "2.0" );

    BigDecimal expResult1 = new BigDecimal( "123456789012345678901.1234567890123456789" );
    BigDecimal expResult2 = new BigDecimal( "61728394506172839450.56172839450617283945" );

    BigDecimal expResult3 = new BigDecimal( "123456789012345678901.12" );
    BigDecimal expResult4 = new BigDecimal( "61728394506172839450.6" );

    assertEquals( expResult1, ValueDataUtil.divideBigDecimals( field1, field2, null ) );
    assertEquals( expResult2, ValueDataUtil.divideBigDecimals( field1, field3, null ) );

    assertEquals( expResult3, ValueDataUtil.divideBigDecimals( field1, field2, new MathContext( 23 ) ) );
    assertEquals( expResult4, ValueDataUtil.divideBigDecimals( field1, field3, new MathContext( 21 ) ) );
  }

  @Test
  public void testRemainderBigNumbers() throws Exception {
    BigDecimal field1 = new BigDecimal( "123456789012345678901.1234567890123456789" );
    BigDecimal field2 = new BigDecimal( "1.0" );
    BigDecimal field3 = new BigDecimal( "2.0" );

    BigDecimal expResult1 = new BigDecimal( "0.1234567890123456789" );
    BigDecimal expResult2 = new BigDecimal( "1.1234567890123456789" );

    assertEquals( expResult1, ValueDataUtil.remainder( new ValueMetaBigNumber(), field1, new ValueMetaBigNumber(), field2 ) );
    assertEquals( expResult2, ValueDataUtil.remainder( new ValueMetaBigNumber(), field1, new ValueMetaBigNumber(), field3 ) );
  }

  @Test
  public void testPercent1() {

    // Test Hop number types
    assertEquals( Double.valueOf( "10.0" ), calculate( "10", "100", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( Double.valueOf( "100.0" ), calculate( "2", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( Double.valueOf( "50.0" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( Double.valueOf( "200.0" ), calculate( "100", "50", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "10" ), calculate( "10", "100", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( Long.valueOf( "100" ), calculate( "2", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( Long.valueOf( "50" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( Long.valueOf( "200" ), calculate( "100", "50", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );

    // Test Hop big Number types
    assertEquals( BigDecimal.valueOf( Long.valueOf( "10" ) ), calculate( "10", "100",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "100" ) ), calculate( "2", "2", IValueMeta.TYPE_BIGNUMBER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "50" ) ), calculate( "10", "20", IValueMeta.TYPE_BIGNUMBER,
      CalculatorMetaFunction.CALC_PERCENT_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "200" ) ), calculate( "100", "50",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_1 ) );
  }

  @Test
  public void testPercent2() {

    // Test Hop number types
    assertEquals( Double.valueOf( "0.99" ), calculate( "1", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( Double.valueOf( "1.96" ), calculate( "2", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( Double.valueOf( "8.0" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( Double.valueOf( "50.0" ), calculate( "100", "50", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "1" ), calculate( "1", "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( Long.valueOf( "2" ), calculate( "2", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( Long.valueOf( "8" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( Long.valueOf( "50" ), calculate( "100", "50", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_2 ) );

    // Test Hop big Number types
    assertEquals( BigDecimal.valueOf( Double.valueOf( "0.99" ) ), calculate( "1", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "1.96" ) ), calculate( "2", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( new BigDecimal( "8.0" ), calculate( "10", "20",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_2 ) );
    assertEquals( new BigDecimal( "50.0" ), calculate( "100", "50",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_2 ) );
  }

  @Test
  public void testPercent3() {

    // Test Hop number types
    assertEquals( Double.valueOf( "1.01" ), calculate( "1", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );
    assertEquals( Double.valueOf( "2.04" ), calculate( "2", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );
    assertEquals( Double.valueOf( "12.0" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );
    assertEquals( Double.valueOf( "150.0" ), calculate( "100", "50", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "1" ), calculate( "1", "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );
    assertEquals( Long.valueOf( "2" ), calculate( "2", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );
    assertEquals( Long.valueOf( "12" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );
    assertEquals( Long.valueOf( "150" ), calculate( "100", "50", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_PERCENT_3 ) );

    // Test Hop big Number types
    assertEquals( 0, new BigDecimal( "1.01" ).compareTo( (BigDecimal) calculate( "1", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_3 ) ) );
    assertEquals( 0, new BigDecimal( "2.04" ).compareTo( (BigDecimal) calculate( "2", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_3 ) ) );
    assertEquals( 0, new BigDecimal( "12" ).compareTo( (BigDecimal) calculate( "10", "20",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_3 ) ) );
    assertEquals( 0, new BigDecimal( "150" ).compareTo( (BigDecimal) calculate( "100", "50",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_PERCENT_3 ) ) );
  }

  @Test
  public void testCombination1() {

    // Test Hop number types
    assertEquals( Double.valueOf( "2.0" ), calculate( "1", "1", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );
    assertEquals( Double.valueOf( "22.0" ), calculate( "2", "2", "10", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );
    assertEquals( Double.valueOf( "70.0" ), calculate( "10", "20", "3", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );
    assertEquals( Double.valueOf( "350" ), calculate( "100", "50", "5", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "2" ), calculate( "1", "1", "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );
    assertEquals( Long.valueOf( "22" ), calculate( "2", "2", "10", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );
    assertEquals( Long.valueOf( "70" ), calculate( "10", "20", "3", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );
    assertEquals( Long.valueOf( "350" ), calculate( "100", "50", "5", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_1 ) );

    // Test Hop big Number types
    assertEquals( 0, new BigDecimal( "2.0" ).compareTo( (BigDecimal) calculate( "1", "1", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_1 ) ) );
    assertEquals( 0, new BigDecimal( "22.0" ).compareTo( (BigDecimal) calculate( "2", "2", "10",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_1 ) ) );
    assertEquals( 0, new BigDecimal( "70.0" ).compareTo( (BigDecimal) calculate( "10", "20", "3",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_1 ) ) );
    assertEquals( 0, new BigDecimal( "350.0" ).compareTo( (BigDecimal) calculate( "100", "50", "5",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_1 ) ) );
  }

  @Test
  public void testCombination2() {

    // Test Hop number types
    assertEquals( Double.valueOf( "1.4142135623730951" ), calculate( "1", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );
    assertEquals( Double.valueOf( "2.8284271247461903" ), calculate( "2", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );
    assertEquals( Double.valueOf( "22.360679774997898" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );
    assertEquals( Double.valueOf( "111.80339887498948" ), calculate( "100", "50", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "1" ), calculate( "1", "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );
    assertEquals( Long.valueOf( "2" ), calculate( "2", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );
    assertEquals( Long.valueOf( "10" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );
    assertEquals( Long.valueOf( "100" ), calculate( "100", "50", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_COMBINATION_2 ) );

    // Test Hop big Number types
    assertEquals( 0, new BigDecimal( "1.4142135623730951" ).compareTo( (BigDecimal) calculate( "1", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_2 ) ) );
    assertEquals( 0, new BigDecimal( "2.8284271247461903" ).compareTo( (BigDecimal) calculate( "2", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_2 ) ) );
    assertEquals( 0, new BigDecimal( "22.360679774997898" ).compareTo( (BigDecimal) calculate( "10", "20",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_2 ) ) );
    assertEquals( 0, new BigDecimal( "111.80339887498948" ).compareTo( (BigDecimal) calculate( "100", "50",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_COMBINATION_2 ) ) );
  }

  @Test
  public void testRound() {

    // Test Hop number types
    assertEquals( Double.valueOf( "1.0" ), calculate( "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( Double.valueOf( "103.0" ), calculate( "103.01", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( Double.valueOf( "1235.0" ), calculate( "1234.6", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    // half
    assertEquals( Double.valueOf( "1235.0" ), calculate( "1234.5", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( Double.valueOf( "1236.0" ), calculate( "1235.5", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( Double.valueOf( "-1234.0" ), calculate( "-1234.5", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( Double.valueOf( "-1235.0" ), calculate( "-1235.5", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "1" ), calculate( "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( Long.valueOf( "2" ), calculate( "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( Long.valueOf( "-103" ), calculate( "-103", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );

    // Test Hop big Number types
    assertEquals( BigDecimal.ONE, calculate( "1", IValueMeta.TYPE_BIGNUMBER,
      CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "103" ) ), calculate( "103.01",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "1235" ) ), calculate( "1234.6",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_1 ) );
    // half
    assertEquals( BigDecimal.valueOf( Long.valueOf( "1235" ) ), calculate( "1234.5",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "1236" ) ), calculate( "1235.5",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "-1234" ) ), calculate( "-1234.5",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_1 ) );
    assertEquals( BigDecimal.valueOf( Long.valueOf( "-1235" ) ), calculate( "-1235.5",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_1 ) );
  }

  @Test
  public void testRound2() {

    // Test Hop number types
    assertEquals( Double.valueOf( "1.0" ), calculate( "1", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Double.valueOf( "2.1" ), calculate( "2.06", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Double.valueOf( "103.0" ), calculate( "103.01", "1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Double.valueOf( "12.35" ), calculate( "12.346", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    // scale < 0
    assertEquals( Double.valueOf( "10.0" ), calculate( "12.0", "-1", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    // half
    assertEquals( Double.valueOf( "12.35" ), calculate( "12.345", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Double.valueOf( "12.36" ), calculate( "12.355", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Double.valueOf( "-12.34" ), calculate( "-12.345", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Double.valueOf( "-12.35" ), calculate( "-12.355", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "1" ), calculate( "1", "1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Long.valueOf( "2" ), calculate( "2", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Long.valueOf( "103" ), calculate( "103", "3", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Long.valueOf( "12" ), calculate( "12", "4", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    // scale < 0
    assertEquals( Long.valueOf( "100" ), calculate( "120", "-2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    // half
    assertEquals( Long.valueOf( "12350" ), calculate( "12345", "-1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Long.valueOf( "12360" ), calculate( "12355", "-1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Long.valueOf( "-12340" ), calculate( "-12345", "-1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( Long.valueOf( "-12350" ), calculate( "-12355", "-1", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_ROUND_2 ) );

    // Test Hop big Number types
    assertEquals( BigDecimal.valueOf( Double.valueOf( "1.0" ) ), calculate( "1", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "2.1" ) ), calculate( "2.06", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "103.0" ) ), calculate( "103.01", "1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "12.35" ) ), calculate( "12.346", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    // scale < 0
    assertEquals( BigDecimal.valueOf( Double.valueOf( "10.0" ) ).setScale( -1 ), calculate( "12.0", "-1",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    // half
    assertEquals( BigDecimal.valueOf( Double.valueOf( "12.35" ) ), calculate( "12.345", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "12.36" ) ), calculate( "12.355", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "-12.34" ) ), calculate( "-12.345", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
    assertEquals( BigDecimal.valueOf( Double.valueOf( "-12.35" ) ), calculate( "-12.355", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_ROUND_2 ) );
  }

  @Test
  public void testNVL() {

    // Test Hop number types
    assertEquals( Double.valueOf( "1.0" ), calculate( "1", "", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( Double.valueOf( "2.0" ), calculate( "", "2", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( Double.valueOf( "10.0" ), calculate( "10", "20", IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( null, calculate( "", "", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_NVL ) );

    // Test Hop string types
    assertEquals( "1", calculate( "1", "", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( "2", calculate( "", "2", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( "10", calculate( "10", "20", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( null, calculate( "", "", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_NVL ) );

    // Test Hop Integer (Java Long) types
    assertEquals( Long.valueOf( "1" ), calculate( "1", "", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( Long.valueOf( "2" ), calculate( "", "2", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( Long.valueOf( "10" ), calculate( "10", "20", IValueMeta.TYPE_INTEGER,
      CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( null, calculate( "", "", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CALC_NVL ) );

    // Test Hop big Number types
    assertEquals( 0, new BigDecimal( "1" ).compareTo( (BigDecimal) calculate( "1", "",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_NVL ) ) );
    assertEquals( 0, new BigDecimal( "2" ).compareTo( (BigDecimal) calculate( "", "2",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_NVL ) ) );
    assertEquals( 0, new BigDecimal( "10" ).compareTo( (BigDecimal) calculate( "10", "20",
      IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_NVL ) ) );
    assertEquals( null, calculate( "", "", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_NVL ) );

    // boolean
    assertEquals( true, calculate( "true", "", IValueMeta.TYPE_BOOLEAN, CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( false, calculate( "", "false", IValueMeta.TYPE_BOOLEAN, CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( false, calculate( "false", "true", IValueMeta.TYPE_BOOLEAN, CalculatorMetaFunction.CALC_NVL ) );
    assertEquals( null, calculate( "", "", IValueMeta.TYPE_BOOLEAN, CalculatorMetaFunction.CALC_NVL ) );

    // Test Hop date
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat( yyyy_MM_dd );

    try {
      assertEquals( simpleDateFormat.parse( "2012-04-11" ), calculate( "2012-04-11", "", IValueMeta.TYPE_DATE,
        CalculatorMetaFunction.CALC_NVL ) );
      assertEquals( simpleDateFormat.parse( "2012-11-04" ), calculate( "", "2012-11-04", IValueMeta.TYPE_DATE,
        CalculatorMetaFunction.CALC_NVL ) );
      assertEquals( simpleDateFormat.parse( "1965-07-01" ), calculate( "1965-07-01", "1967-04-11",
        IValueMeta.TYPE_DATE, CalculatorMetaFunction.CALC_NVL ) );
      assertNull( calculate( "", "", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CALC_NVL ) );

    } catch ( ParseException pe ) {
      fail( pe.getMessage() );
    }
    // assertEquals(0, calculate("", "2012-11-04", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CALC_NVL)));
    // assertEquals(0, calculate("2012-11-04", "2010-04-11", IValueMeta.TYPE_DATE,
    // CalculatorMetaFunction.CALC_NVL)));
    // assertEquals(null, calculate("", "", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CALC_NVL));

    // binary
    IValueMeta stringValueMeta = new ValueMetaString( "string" );
    try {
      byte[] data = stringValueMeta.getBinary( "101" );
      byte[] calculated =
        (byte[]) calculate( "101", "", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CALC_NVL );
      assertTrue( Arrays.equals( data, calculated ) );

      data = stringValueMeta.getBinary( "011" );
      calculated = (byte[]) calculate( "", "011", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CALC_NVL );
      assertTrue( Arrays.equals( data, calculated ) );

      data = stringValueMeta.getBinary( "110" );
      calculated = (byte[]) calculate( "110", "011", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CALC_NVL );
      assertTrue( Arrays.equals( data, calculated ) );

      calculated = (byte[]) calculate( "", "", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CALC_NVL );
      assertNull( calculated );

      // assertEquals(binaryValueMeta.convertData(new ValueMeta("dummy", ValueMeta.TYPE_STRING), "101"),
      // calculate("101", "", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CALC_NVL));
    } catch ( HopValueException kve ) {
      fail( kve.getMessage() );
    }
  }

  @Test
  public void testRemainder() throws Exception {
    assertNull( calculate( null, null, IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertNull( calculate( null, "3", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertNull( calculate( "10", null, IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertEquals( new Long( "1" ),
      calculate( "10", "3", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertEquals( new Long( "-1" ),
      calculate( "-10", "3", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CALC_REMAINDER ) );

    Double comparisonDelta = new Double( "0.0000000000001" );
    assertNull( calculate( null, null, IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertNull( calculate( null, "4.1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertNull( calculate( "17.8", null, IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertEquals( new Double( "1.4" ).doubleValue(),
      ( (Double) calculate( "17.8", "4.1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER )
      ).doubleValue(),
      comparisonDelta.doubleValue() );
    assertEquals( new Double( "1.4" ).doubleValue(),
      ( (Double) calculate( "17.8", "-4.1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER )
      ).doubleValue(),
      comparisonDelta.doubleValue() );

    assertEquals( new Double( "-1.4" ).doubleValue(),
      ( (Double) calculate( "-17.8", "-4.1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER )
      ).doubleValue(),
      comparisonDelta.doubleValue() );

    assertNull( calculate( null, null, IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertNull( calculate( null, "16.12", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertNull( calculate( "-144.144", null, IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );

    assertEquals( new BigDecimal( "-15.184" ),
      calculate( "-144.144", "16.12", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertEquals( new Double( "2.6000000000000005" ).doubleValue(),
      calculate( "12.5", "3.3", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertEquals( new Double( "4.0" ).doubleValue(),
      calculate( "12.5", "4.25", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
    assertEquals( new Long( "1" ).longValue(),
      calculate( "10", "3.3", null,
        IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CALC_REMAINDER ) );
  }

  @Test
  public void testSumWithNullValues() throws Exception {
    IValueMeta metaA = new ValueMetaInteger();
    metaA.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
    IValueMeta metaB = new ValueMetaInteger();
    metaA.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );

    assertNull( ValueDataUtil.sum( metaA, null, metaB, null ) );

    Long valueB = new Long( 2 );
    ValueDataUtil.sum( metaA, null, metaB, valueB );
  }

  @Test
  public void testSumConvertingStorageTypeToNormal() throws Exception {
    IValueMeta metaA = mock( ValueMetaInteger.class );
    metaA.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );

    IValueMeta metaB = new ValueMetaInteger();
    metaB.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    Object valueB = "2";

    when( metaA.convertData( metaB, valueB ) ).thenAnswer( (Answer<Long>) invocation -> new Long( 2 ) );

    Object returnValue = ValueDataUtil.sum( metaA, null, metaB, valueB );
    verify( metaA ).convertData( metaB, valueB );
    assertEquals( 2L, returnValue );
    assertEquals( metaA.getStorageType(), IValueMeta.STORAGE_TYPE_NORMAL );
  }

  @Test
  public void testJaro() {
    assertEquals( new Double( "0.0" ), calculate( "abcd", "defg", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_JARO ) );
    assertEquals( new Double( "0.44166666666666665" ), calculate( "elephant", "hippo", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_JARO ) );
    assertEquals( new Double( "0.8666666666666667" ), calculate( "hello", "hallo", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_JARO ) );
  }

  @Test
  public void testJaroWinkler() {
    assertEquals( new Double( "0.0" ), calculate( "abcd", "defg", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CALC_JARO_WINKLER ) );
  }

  private Object calculate( String string_dataA, int valueMetaInterfaceType, int calculatorMetaFunction ) {
    return calculate( string_dataA, null, null, valueMetaInterfaceType, calculatorMetaFunction );
  }

  private Object calculate( String string_dataA, String string_dataB, int valueMetaInterfaceType,
                            int calculatorMetaFunction ) {
    return calculate( string_dataA, string_dataB, null, valueMetaInterfaceType, calculatorMetaFunction );
  }

  private Object createObject( String stringValue, int valueMetaInterfaceType, IValueMeta parameterValueMeta ) throws HopValueException {
    if ( valueMetaInterfaceType == IValueMeta.TYPE_NUMBER ) {
      return ( !Utils.isEmpty( stringValue ) ? Double.valueOf( stringValue ) : null );
    } else if ( valueMetaInterfaceType == IValueMeta.TYPE_INTEGER ) {
      return ( !Utils.isEmpty( stringValue ) ? Long.valueOf( stringValue ) : null );
    } else if ( valueMetaInterfaceType == IValueMeta.TYPE_DATE ) {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat( yyyy_MM_dd );
      try {
        return ( !Utils.isEmpty( stringValue ) ? simpleDateFormat.parse( stringValue ) : null );
      } catch ( ParseException pe ) {
        fail( pe.getMessage() );
        return null;
      }
    } else if ( valueMetaInterfaceType == IValueMeta.TYPE_BIGNUMBER ) {
      return ( !Utils.isEmpty( stringValue ) ? BigDecimal.valueOf( Double.valueOf( stringValue ) ) : null );
    } else if ( valueMetaInterfaceType == IValueMeta.TYPE_STRING ) {
      return ( !Utils.isEmpty( stringValue ) ? stringValue : null );
    } else if ( valueMetaInterfaceType == IValueMeta.TYPE_BINARY ) {
      IValueMeta binaryValueMeta = new ValueMetaBinary( "binary_data" );
      return
        ( !Utils.isEmpty( stringValue ) ? binaryValueMeta.convertData( parameterValueMeta, stringValue ) : null );
    } else if ( valueMetaInterfaceType == IValueMeta.TYPE_BOOLEAN ) {
      if ( !Utils.isEmpty( stringValue ) ) {
        return ( stringValue.equalsIgnoreCase( "true" ) ? true : false );
      } else {
        return null;
      }
    } else {
      fail( "Invalid IValueMeta type." );
      return null;
    }
  }

  private Object calculate( String string_dataA, String string_dataB, String string_dataC, int valueMetaInterfaceTypeABC,
                            int calculatorMetaFunction ) {
    return calculate( string_dataA, string_dataB, string_dataC,
      valueMetaInterfaceTypeABC, valueMetaInterfaceTypeABC, valueMetaInterfaceTypeABC, calculatorMetaFunction );
  }

  private Object calculate( String string_dataA, String string_dataB, String string_dataC,
                            int valueMetaInterfaceTypeA, int valueMetaInterfaceTypeB, int valueMetaInterfaceTypeC,
                            int calculatorMetaFunction ) {

    try {

      //
      IValueMeta parameterValueMeta = new ValueMetaString( "parameter" );

      // We create the meta information for
      IValueMeta valueMetaA = createValueMeta( "data_A", valueMetaInterfaceTypeA );
      IValueMeta valueMetaB = createValueMeta( "data_B", valueMetaInterfaceTypeB );
      IValueMeta valueMetaC = createValueMeta( "data_C", valueMetaInterfaceTypeC );

      Object dataA = createObject( string_dataA, valueMetaInterfaceTypeA, parameterValueMeta );
      Object dataB = createObject( string_dataB, valueMetaInterfaceTypeB, parameterValueMeta );
      Object dataC = createObject( string_dataC, valueMetaInterfaceTypeC, parameterValueMeta );

      if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_ADD ) {
        return ValueDataUtil.plus( valueMetaA, dataA, valueMetaB, dataB );
      }
      if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_ADD3 ) {
        return ValueDataUtil.plus3( valueMetaA, dataA, valueMetaB, dataB, valueMetaC, dataC );
      }
      if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_SUBTRACT ) {
        return ValueDataUtil.minus( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_DIVIDE ) {
        return ValueDataUtil.divide( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_PERCENT_1 ) {
        return ValueDataUtil.percent1( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_PERCENT_2 ) {
        return ValueDataUtil.percent2( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_PERCENT_3 ) {
        return ValueDataUtil.percent3( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_COMBINATION_1 ) {
        return ValueDataUtil.combination1( valueMetaA, dataA, valueMetaB, dataB, valueMetaC, dataC );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_COMBINATION_2 ) {
        return ValueDataUtil.combination2( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_ROUND_1 ) {
        return ValueDataUtil.round( valueMetaA, dataA );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_ROUND_2 ) {
        return ValueDataUtil.round( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_NVL ) {
        return ValueDataUtil.nvl( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_DATE_DIFF ) {
        return ValueDataUtil.DateDiff( valueMetaA, dataA, valueMetaB, dataB, "" );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_DATE_WORKING_DIFF ) {
        return ValueDataUtil.DateWorkingDiff( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_REMAINDER ) {
        return ValueDataUtil.remainder( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_JARO ) {
        return ValueDataUtil.getJaro_Similitude( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_JARO_WINKLER ) {
        return ValueDataUtil.getJaroWinkler_Similitude( valueMetaA, dataA, valueMetaB, dataB );
      } else if ( calculatorMetaFunction == CalculatorMetaFunction.CALC_MULTIPLY ) {
        return ValueDataUtil.multiply( valueMetaA, dataA, valueMetaB, dataB );
      } else {
        fail( "Invalid CalculatorMetaFunction specified." );
        return null;
      }
    } catch ( HopValueException kve ) {
      fail( kve.getMessage() );
      return null;
    }
  }

  private IValueMeta createValueMeta( String name, int valueType ) {
    try {
      return ValueMetaFactory.createValueMeta( name, valueType );
    } catch ( HopPluginException e ) {
      throw new RuntimeException( e );
    }
  }
}
