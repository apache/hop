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

package org.apache.hop.core.row;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ValueDataUtilTest {
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
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), xmlFilePath , true);
    assertTrue( wellFormed );
  }

  @Test
  public void xmlFileBadlyFormedTest() throws HopFileNotFoundException  {
    String invalidXmlFilePath = getClass().getResource( "invalid-xml-sample.xml" ).getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed( new ValueMetaString(), invalidXmlFilePath , true);
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
}
