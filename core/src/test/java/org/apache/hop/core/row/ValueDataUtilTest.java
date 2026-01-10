/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.row;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class ValueDataUtilTest {

  @Test
  public void testPlus() throws HopValueException {

    long longValue = 1;

    assertEquals(
        longValue,
        ValueDataUtil.plus(
            new ValueMetaInteger(), longValue, new ValueMetaString(), StringUtils.EMPTY));
  }

  @Test
  public void checksumTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String checksum = ValueDataUtil.createChecksum(path, "MD5", false);
    assertEquals("098f6bcd4621d373cade4e832627b4f6", checksum);
  }

  @Test
  public void checksumMissingFileTest() throws Exception {
    String nonExistingFile = "nonExistingFile";
    String checksum = ValueDataUtil.createChecksum(nonExistingFile, "MD5", false);
    assertNull(checksum);
  }

  @Test
  public void checksumWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String checksum = ValueDataUtil.createChecksum(path, "MD5", true);
    assertEquals("098f6bcd4621d373cade4e832627b4f6", checksum);
  }

  @Test(expected = HopFileNotFoundException.class)
  public void checksumFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.createChecksum(nonExistingPath, "MD5", true);
  }

  @Test
  public void checksumNullPathNoFailTest() throws HopFileNotFoundException {
    assertNull(ValueDataUtil.createChecksum(null, "MD5", false));
  }

  @Test
  public void checksumNullPathFailTest() throws HopFileNotFoundException {
    assertNull(ValueDataUtil.createChecksum(null, "MD5", true));
  }

  @Test
  public void checksumCRC32Test() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    long checksum = ValueDataUtil.checksumCRC32(path, false);
    assertEquals(3632233996l, checksum);
  }

  @Test
  public void checksumCRC32MissingFileTest() throws Exception {
    String nonExistingFile = "nonExistingFile";
    long checksum = ValueDataUtil.checksumCRC32(nonExistingFile, false);
    assertEquals(0, checksum);
  }

  @Test
  public void checksumCRC32NoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    long checksum = ValueDataUtil.checksumCRC32(nonExistingPath, false);
    assertEquals(0, checksum);
  }

  @Test(expected = HopFileNotFoundException.class)
  public void checksumCRC32FailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.checksumCRC32(nonExistingPath, true);
  }

  @Test
  public void checksumCRC32NullPathNoFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumCRC32(null, false);
    assertEquals(0, checksum);
  }

  @Test
  public void checksumCRC32NullPathFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumCRC32(null, true);
    assertEquals(0, checksum);
  }

  @Test
  public void checksumAdlerWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    long checksum = ValueDataUtil.checksumAdler32(path, true);
    assertEquals(73204161L, checksum);
  }

  @Test
  public void checksumAdlerWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    long checksum = ValueDataUtil.checksumAdler32(path, false);
    assertEquals(73204161L, checksum);
  }

  @Test
  public void checksumAdlerNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    long checksum = ValueDataUtil.checksumAdler32(nonExistingPath, false);
    assertEquals(0, checksum);
  }

  @Test(expected = HopFileNotFoundException.class)
  public void checksumAdlerFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.checksumAdler32(nonExistingPath, true);
  }

  @Test
  public void checksumAdlerNullPathNoFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumAdler32(null, false);
    assertEquals(0, checksum);
  }

  @Test
  public void checksumAdlerNullPathFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumAdler32(null, true);
    assertEquals(0, checksum);
  }

  @Test
  public void xmlFileWellFormedTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource("xml-sample.xml").getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(xmlFilePath, true);
    assertTrue(wellFormed);
  }

  @Test
  public void xmlFileBadlyFormedTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource("invalid-xml-sample.xml").getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(invalidXmlFilePath, true);
    assertFalse(wellFormed);
  }

  @Test
  public void xmlFileWellFormedWithoutFailIfNoFileTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource("xml-sample.xml").getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(xmlFilePath, false);
    assertTrue(wellFormed);
  }

  @Test
  public void xmlFileBadlyFormedWithNoFailIfNoFileTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource("invalid-xml-sample.xml").getPath();
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(invalidXmlFilePath, false);
    assertFalse(wellFormed);
  }

  @Test
  public void xmlFileWellFormedNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(nonExistingPath, false);
    assertFalse(wellFormed);
  }

  @Test(expected = HopFileNotFoundException.class)
  public void xmlFileWellFormedFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.isXmlFileWellFormed(nonExistingPath, true);
  }

  @Test
  public void xmlFileWellFormedNullPathNoFailTest() throws HopFileNotFoundException {
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(null, false);
    assertFalse(wellFormed);
  }

  @Test
  public void xmlFileWellFormedNullPathFailTest() throws HopFileNotFoundException {
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(null, true);
    assertFalse(wellFormed);
  }

  @Test
  public void loadFileContentInBinary() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    byte[] content = ValueDataUtil.loadFileContentInBinary(path, true);
    assertTrue(Arrays.equals("test".getBytes(), content));
  }

  @Test
  public void loadFileContentInBinaryNoFailIfNoFileTest() throws Exception {
    String nonExistingPath = "nonExistingPath";
    assertNull(ValueDataUtil.loadFileContentInBinary(nonExistingPath, false));
  }

  @Test(expected = HopFileNotFoundException.class)
  public void loadFileContentInBinaryFailIfNoFileTest()
      throws HopFileNotFoundException, HopValueException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.loadFileContentInBinary(nonExistingPath, true);
  }

  @Test
  public void loadFileContentInBinaryNullPathNoFailTest() throws Exception {
    assertNull(ValueDataUtil.loadFileContentInBinary(null, false));
  }

  @Test
  public void loadFileContentInBinaryNullPathFailTest()
      throws HopFileNotFoundException, HopValueException {
    assertNull(ValueDataUtil.loadFileContentInBinary(null, true));
  }

  @Test
  public void getFileEncodingWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), path, true);
    assertEquals("US-ASCII", encoding);
  }

  @Test
  public void getFileEncodingWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), path, false);
    assertEquals("US-ASCII", encoding);
  }

  @Test
  public void getFileEncodingNoFailIfNoFileTest() throws Exception {
    String nonExistingPath = "nonExistingPath";
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), nonExistingPath, false);
    assertNull(encoding);
  }

  @Test(expected = HopFileNotFoundException.class)
  public void getFileEncodingFailIfNoFileTest() throws HopFileNotFoundException, HopValueException {
    String nonExistingPath = "nonExistingPath";
    ValueDataUtil.getFileEncoding(new ValueMetaString(), nonExistingPath, true);
  }

  @Test
  public void getFileEncodingNullPathNoFailTest() throws Exception {
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), null, false);
    assertNull(encoding);
  }

  @Test
  public void getFileEncodingNullPathFailTest() throws HopFileNotFoundException, HopValueException {
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), null, true);
    assertNull(encoding);
  }

  @Test
  public void testMulitplyBigNumbers() {
    BigDecimal field1 = new BigDecimal("123456789012345678901.1234567890123456789");
    BigDecimal field2 = new BigDecimal("1.0");
    BigDecimal field3 = new BigDecimal("2.0");

    BigDecimal expResult1 = new BigDecimal("123456789012345678901.1234567890123456789");
    BigDecimal expResult2 = new BigDecimal("246913578024691357802.2469135780246913578");

    BigDecimal expResult3 = new BigDecimal("123456789012345678901.1200000000000000000");
    BigDecimal expResult4 = new BigDecimal("246913578024691357802");

    assertEquals(expResult1, ValueDataUtil.multiplyBigDecimals(field1, field2, null));
    assertEquals(expResult2, ValueDataUtil.multiplyBigDecimals(field1, field3, null));

    assertEquals(
        expResult3, ValueDataUtil.multiplyBigDecimals(field1, field2, new MathContext(23)));
    assertEquals(
        expResult4, ValueDataUtil.multiplyBigDecimals(field1, field3, new MathContext(21)));
  }

  @Test
  public void testDivisionBigNumbers() {
    BigDecimal field1 = new BigDecimal("123456789012345678901.1234567890123456789");
    BigDecimal field2 = new BigDecimal("1.0");
    BigDecimal field3 = new BigDecimal("2.0");

    BigDecimal expResult1 = new BigDecimal("123456789012345678901.1234567890123456789");
    BigDecimal expResult2 = new BigDecimal("61728394506172839450.56172839450617283945");

    BigDecimal expResult3 = new BigDecimal("123456789012345678901.12");
    BigDecimal expResult4 = new BigDecimal("61728394506172839450.6");

    assertEquals(expResult1, ValueDataUtil.divideBigDecimals(field1, field2, null));
    assertEquals(expResult2, ValueDataUtil.divideBigDecimals(field1, field3, null));

    assertEquals(expResult3, ValueDataUtil.divideBigDecimals(field1, field2, new MathContext(23)));
    assertEquals(expResult4, ValueDataUtil.divideBigDecimals(field1, field3, new MathContext(21)));
  }

  @Test
  public void testRemainderBigNumbers() throws Exception {
    BigDecimal field1 = new BigDecimal("123456789012345678901.1234567890123456789");
    BigDecimal field2 = new BigDecimal("1.0");
    BigDecimal field3 = new BigDecimal("2.0");

    BigDecimal expResult1 = new BigDecimal("0.1234567890123456789");
    BigDecimal expResult2 = new BigDecimal("1.1234567890123456789");

    assertEquals(
        expResult1,
        ValueDataUtil.remainder(
            new ValueMetaBigNumber(), field1, new ValueMetaBigNumber(), field2));
    assertEquals(
        expResult2,
        ValueDataUtil.remainder(
            new ValueMetaBigNumber(), field1, new ValueMetaBigNumber(), field3));
  }

  @Test
  public void testSumWithNullValues() throws Exception {
    IValueMeta metaA = new ValueMetaInteger();
    metaA.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    IValueMeta metaB = new ValueMetaInteger();
    metaA.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

    assertNull(ValueDataUtil.sum(metaA, null, metaB, null));

    Long valueB = 2L;
    ValueDataUtil.sum(metaA, null, metaB, valueB);
  }

  @Test
  public void testSumConvertingStorageTypeToNormal() throws Exception {
    IValueMeta metaA = mock(ValueMetaInteger.class);
    metaA.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);

    IValueMeta metaB = new ValueMetaInteger();
    metaB.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    Object valueB = "2";

    when(metaA.convertData(metaB, valueB)).thenAnswer((Answer<Long>) invocation -> 2L);

    Object returnValue = ValueDataUtil.sum(metaA, null, metaB, valueB);
    verify(metaA).convertData(metaB, valueB);
    assertEquals(2L, returnValue);
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, metaA.getStorageType());
  }

  // String manipulation tests
  @Test
  public void testInitCap() {
    assertEquals("Hello World", ValueDataUtil.initCap("hello world"));
    assertEquals("Apache Hop", ValueDataUtil.initCap("apache hop"));
    assertNull(ValueDataUtil.initCap(null));
  }

  @Test
  public void testUpperCase() {
    assertEquals("HELLO", ValueDataUtil.upperCase("hello"));
    assertEquals("APACHE HOP", ValueDataUtil.upperCase("Apache Hop"));
    assertNull(ValueDataUtil.upperCase(null));
  }

  @Test
  public void testLowerCase() {
    assertEquals("hello", ValueDataUtil.lowerCase("HELLO"));
    assertEquals("apache hop", ValueDataUtil.lowerCase("Apache Hop"));
    assertNull(ValueDataUtil.lowerCase(null));
  }

  @Test
  public void testRemoveCR() {
    assertEquals("helloworld", ValueDataUtil.removeCR("hello\rworld"));
    assertNull(ValueDataUtil.removeCR(null));
  }

  @Test
  public void testRemoveLF() {
    assertEquals("helloworld", ValueDataUtil.removeLF("hello\nworld"));
    assertNull(ValueDataUtil.removeLF(null));
  }

  @Test
  public void testRemoveCRLF() {
    assertEquals("helloworld", ValueDataUtil.removeCRLF("hello\r\nworld"));
    assertNull(ValueDataUtil.removeCRLF(null));
  }

  @Test
  public void testRemoveTAB() {
    assertEquals("helloworld", ValueDataUtil.removeTAB("hello\tworld"));
    assertNull(ValueDataUtil.removeTAB(null));
  }

  @Test
  public void testGetDigits() {
    assertEquals("123", ValueDataUtil.getDigits("abc123def"));
    assertEquals("", ValueDataUtil.getDigits("abcdef"));
    assertNull(ValueDataUtil.getDigits(null));
  }

  @Test
  public void testRemoveDigits() {
    assertEquals("abcdef", ValueDataUtil.removeDigits("abc123def"));
    assertEquals("abcdef", ValueDataUtil.removeDigits("abcdef"));
    assertNull(ValueDataUtil.removeDigits(null));
  }

  @Test
  public void testStringLen() {
    assertEquals(5, ValueDataUtil.stringLen("hello"));
    assertEquals(0, ValueDataUtil.stringLen(""));
    assertEquals(0, ValueDataUtil.stringLen(null));
  }

  // String distance/similarity tests
  @Test
  public void testLevenshteinDistance() {
    Long distance = ValueDataUtil.getLevenshteinDistance("kitten", "sitting");
    assertEquals(Long.valueOf(3), distance);

    assertNull(ValueDataUtil.getLevenshteinDistance(null, "test"));
    assertNull(ValueDataUtil.getLevenshteinDistance("test", null));
  }

  @Test
  public void testDamerauLevenshteinDistance() {
    Long distance = ValueDataUtil.getDamerauLevenshteinDistance("abc", "acb");
    assertEquals(Long.valueOf(1), distance);

    assertNull(ValueDataUtil.getDamerauLevenshteinDistance(null, "test"));
  }

  @Test
  public void testJaroSimilitude() {
    Double similarity = ValueDataUtil.getJaroSimilitude("martha", "marhta");
    assertTrue(similarity > 0.9);

    assertNull(ValueDataUtil.getJaroSimilitude(null, "test"));
  }

  @Test
  public void testJaroWinklerSimilitude() {
    Double similarity = ValueDataUtil.getJaroWinklerSimilitude("martha", "marhta");
    assertTrue(similarity > 0.9);

    assertNull(ValueDataUtil.getJaroWinklerSimilitude(null, "test"));
  }

  // Phonetic algorithms tests
  @Test
  public void testMetaphone() {
    String metaphone = ValueDataUtil.getMetaphone("hello");
    assertEquals("HL", metaphone);
    assertNull(ValueDataUtil.getMetaphone(null));
  }

  @Test
  public void testDoubleMetaphone() {
    String doubleMetaphone = ValueDataUtil.getDoubleMetaphone("hello");
    assertEquals("HL", doubleMetaphone);
    assertNull(ValueDataUtil.getDoubleMetaphone(null));
  }

  @Test
  public void testSoundEx() {
    String soundex = ValueDataUtil.getSoundEx("hello");
    assertEquals("H400", soundex);
    assertNull(ValueDataUtil.getSoundEx(null));
  }

  @Test
  public void testRefinedSoundEx() {
    String refinedSoundex = ValueDataUtil.getRefinedSoundEx("hello");
    assertNotNull(refinedSoundex);
    assertNull(ValueDataUtil.getRefinedSoundEx(null));
  }

  // Math operations tests
  @Test
  public void testAbs() throws HopValueException {
    assertEquals(5L, ValueDataUtil.abs(new ValueMetaInteger(), (long) -5));
    assertEquals(5.5, ValueDataUtil.abs(new ValueMetaNumber(), -5.5));
    assertEquals(
        new BigDecimal("5.5"), ValueDataUtil.abs(new ValueMetaBigNumber(), new BigDecimal("-5.5")));
  }

  @Test
  public void testSqrt() throws HopValueException {
    Object result = ValueDataUtil.sqrt(new ValueMetaNumber(), 16.0);
    assertEquals(4.0, (Double) result, 0.001);

    result = ValueDataUtil.sqrt(new ValueMetaInteger(), 25L);
    assertEquals(5L, result);
  }

  @Test
  public void testCeil() throws HopValueException {
    assertEquals(6.0, ValueDataUtil.ceil(new ValueMetaNumber(), 5.3));
    assertEquals(5L, ValueDataUtil.ceil(new ValueMetaInteger(), 5L));
    assertEquals(
        BigDecimal.valueOf(6.0),
        ValueDataUtil.ceil(new ValueMetaBigNumber(), new BigDecimal("5.3")));
  }

  @Test
  public void testFloor() throws HopValueException {
    assertEquals(5.0, ValueDataUtil.floor(new ValueMetaNumber(), 5.9));
    assertEquals(5L, ValueDataUtil.floor(new ValueMetaInteger(), 5L));
    assertEquals(
        BigDecimal.valueOf(5.0),
        ValueDataUtil.floor(new ValueMetaBigNumber(), new BigDecimal("5.9")));
  }

  @Test
  public void testRoundNoDigits() throws HopValueException {
    assertEquals(5.0, ValueDataUtil.round(new ValueMetaNumber(), 5.4));
    assertEquals(6.0, ValueDataUtil.round(new ValueMetaNumber(), 5.5));
    assertEquals(5L, ValueDataUtil.round(new ValueMetaInteger(), 5L));
  }

  @Test
  public void testRoundWithDigits() throws HopValueException {
    Object result = ValueDataUtil.round(new ValueMetaNumber(), 5.456, new ValueMetaInteger(), 2L);
    assertEquals(5.46, (Double) result, 0.001);
  }

  // Date operations tests
  @Test
  public void testRemoveTimeFromDate() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 14, 30, 45);
    Date dateWithTime = cal.getTime();

    Date result = (Date) ValueDataUtil.removeTimeFromDate(new ValueMetaDate(), dateWithTime);

    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(result);
    assertEquals(0, resultCal.get(Calendar.HOUR_OF_DAY));
    assertEquals(0, resultCal.get(Calendar.MINUTE));
    assertEquals(0, resultCal.get(Calendar.SECOND));
    assertEquals(0, resultCal.get(Calendar.MILLISECOND));
  }

  @Test
  public void testAddDays() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 0, 0, 0);
    Date date = cal.getTime();

    Date result =
        (Date) ValueDataUtil.addDays(new ValueMetaDate(), date, new ValueMetaInteger(), 5L);

    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(result);
    assertEquals(20, resultCal.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  public void testAddHours() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 10, 0, 0);
    Date date = cal.getTime();

    Date result =
        (Date) ValueDataUtil.addHours(new ValueMetaDate(), date, new ValueMetaInteger(), 5L);

    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(result);
    assertEquals(15, resultCal.get(Calendar.HOUR_OF_DAY));
  }

  @Test
  public void testAddMinutes() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 10, 30, 0);
    Date date = cal.getTime();

    Date result =
        (Date) ValueDataUtil.addMinutes(new ValueMetaDate(), date, new ValueMetaInteger(), 45L);

    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(result);
    assertEquals(15, resultCal.get(Calendar.MINUTE));
  }

  @Test
  public void testYearOfDate() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15);
    Date date = cal.getTime();

    Long year = (Long) ValueDataUtil.yearOfDate(new ValueMetaDate(), date);
    assertEquals(Long.valueOf(2024), year);
  }

  @Test
  public void testMonthOfDate() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.MARCH, 15);
    Date date = cal.getTime();

    Long month = (Long) ValueDataUtil.monthOfDate(new ValueMetaDate(), date);
    assertEquals(Long.valueOf(3), month);
  }

  @Test
  public void testDayOfMonth() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15);
    Date date = cal.getTime();

    Long day = (Long) ValueDataUtil.dayOfMonth(new ValueMetaDate(), date);
    assertEquals(Long.valueOf(15), day);
  }

  @Test
  public void testHourOfDay() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 14, 30, 0);
    Date date = cal.getTime();

    Long hour = (Long) ValueDataUtil.hourOfDay(new ValueMetaDate(), date);
    assertEquals(Long.valueOf(14), hour);
  }

  // NVL function tests
  @Test
  public void testNvlWithNullFirstValue() throws HopValueException {
    String result =
        (String) ValueDataUtil.nvl(new ValueMetaString(), null, new ValueMetaString(), "default");
    assertEquals("default", result);
  }

  @Test
  public void testNvlWithNonNullFirstValue() throws HopValueException {
    String result =
        (String)
            ValueDataUtil.nvl(
                new ValueMetaString(), "value",
                new ValueMetaString(), "default");
    assertEquals("value", result);
  }

  @Test
  public void testNvlWithIntegers() throws HopValueException {
    Long result =
        (Long) ValueDataUtil.nvl(new ValueMetaInteger(), null, new ValueMetaInteger(), 100L);
    assertEquals(Long.valueOf(100), result);
  }

  // URL encoding/decoding tests
  @Test
  public void testUrlEncode() {
    String encoded = ValueDataUtil.urlEncode("hello world");
    assertEquals("hello+world", encoded);
    assertNull(ValueDataUtil.urlEncode(null));
  }

  @Test
  public void testUrlDecode() {
    String decoded = ValueDataUtil.urlDecode("hello+world");
    assertEquals("hello world", decoded);
    assertNull(ValueDataUtil.urlDecode(null));
  }

  // Hex encoding/decoding tests
  @Test
  public void testByteToHexEncode() throws HopValueException {
    String hex = ValueDataUtil.byteToHexEncode(new ValueMetaString(), "test");
    assertEquals("74657374", hex);
  }

  @Test
  public void testHexToByteDecode() throws HopValueException {
    String decoded = ValueDataUtil.hexToByteDecode(new ValueMetaString(), "74657374");
    assertEquals("test", decoded);
  }

  // XML/HTML escape tests
  @Test
  public void testEscapeXml() {
    String escaped = ValueDataUtil.escapeXml("<hello>");
    assertTrue(escaped.contains("&lt;") && escaped.contains("&gt;"));
    assertNull(ValueDataUtil.escapeXml(null));
  }

  @Test
  public void testUnEscapeXml() {
    String unescaped = ValueDataUtil.unEscapeXml("&lt;hello&gt;");
    assertEquals("<hello>", unescaped);
    assertNull(ValueDataUtil.unEscapeXml(null));
  }

  @Test
  public void testEscapeHtml() {
    String escaped = ValueDataUtil.escapeHtml("<hello>");
    assertTrue(escaped.contains("&lt;") && escaped.contains("&gt;"));
    assertNull(ValueDataUtil.escapeHtml(null));
  }

  @Test
  public void testUnEscapeHtml() {
    String unescaped = ValueDataUtil.unEscapeHtml("&lt;hello&gt;");
    assertEquals("<hello>", unescaped);
    assertNull(ValueDataUtil.unEscapeHtml(null));
  }

  // Test getZeroForValueMetaType
  @Test
  public void testGetZeroForValueMetaType() throws HopValueException {
    assertEquals(0L, ValueDataUtil.getZeroForValueMetaType(new ValueMetaInteger()));
    assertEquals((double) 0, ValueDataUtil.getZeroForValueMetaType(new ValueMetaNumber()));
    assertEquals(
        new BigDecimal(0), ValueDataUtil.getZeroForValueMetaType(new ValueMetaBigNumber()));
    assertEquals("", ValueDataUtil.getZeroForValueMetaType(new ValueMetaString()));
  }

  @Test(expected = HopValueException.class)
  public void testGetZeroForValueMetaTypeWithNull() throws HopValueException {
    ValueDataUtil.getZeroForValueMetaType(null);
  }

  @Test(expected = HopValueException.class)
  public void testGetZeroForValueMetaTypeWithUnsupportedType() throws HopValueException {
    ValueDataUtil.getZeroForValueMetaType(new ValueMetaBoolean());
  }

  // Test minus operation
  @Test
  public void testMinus() throws HopValueException {
    Object result =
        ValueDataUtil.minus(
            new ValueMetaInteger(), 10L,
            new ValueMetaInteger(), 3L);
    assertEquals(7L, result);

    result =
        ValueDataUtil.minus(
            new ValueMetaNumber(), 10.5,
            new ValueMetaNumber(), 3.2);
    assertEquals(7.3, (Double) result, 0.001);
  }

  // Test multiply operation
  @Test
  public void testMultiply() throws HopValueException {
    Object result =
        ValueDataUtil.multiply(
            new ValueMetaInteger(), 5L,
            new ValueMetaInteger(), 3L);
    assertEquals(15L, result);

    result =
        ValueDataUtil.multiply(
            new ValueMetaNumber(), 2.5,
            new ValueMetaNumber(), 4.0);
    assertEquals(10.0, (Double) result, 0.001);
  }

  // Test divide operation
  @Test
  public void testDivide() throws HopValueException {
    Object result =
        ValueDataUtil.divide(
            new ValueMetaInteger(), 10L,
            new ValueMetaInteger(), 2L);
    assertEquals(5L, result);

    result =
        ValueDataUtil.divide(
            new ValueMetaNumber(), 10.0,
            new ValueMetaNumber(), 2.0);
    assertEquals(5.0, (Double) result, 0.001);
  }

  // Test isXmlWellFormed (for string content, not file)
  @Test
  public void testIsXmlWellFormed() {
    assertTrue(
        ValueDataUtil.isXmlWellFormed(
            new ValueMetaString(), "<?xml version=\"1.0\"?><root>test</root>"));
    assertFalse(ValueDataUtil.isXmlWellFormed(new ValueMetaString(), "<root>test"));
    assertFalse(ValueDataUtil.isXmlWellFormed(new ValueMetaString(), null));
  }

  // SQL escape test
  @Test
  public void testEscapeSql() {
    String escaped = ValueDataUtil.escapeSql("O'Reilly");
    assertEquals("O''Reilly", escaped);
    assertNull(ValueDataUtil.escapeSql(null));
  }

  // CDATA test
  @Test
  public void testUseCDATA() {
    String cdata = ValueDataUtil.useCDATA("<test>data</test>");
    assertTrue(cdata.startsWith("<![CDATA[") && cdata.endsWith("]]>"));
    assertNull(ValueDataUtil.useCDATA(null));
  }

  // Percentage functions tests
  @Test
  public void testPercent1() throws HopValueException {
    // percent1: A / B * 100
    Object result =
        ValueDataUtil.percent1(
            new ValueMetaNumber(), 50.0,
            new ValueMetaNumber(), 200.0);
    assertEquals(25.0, (Double) result, 0.001);
  }

  @Test
  public void testPercent2() throws HopValueException {
    // percent2: A - (A * B / 100)
    Object result =
        ValueDataUtil.percent2(
            new ValueMetaNumber(), 100.0,
            new ValueMetaNumber(), 20.0);
    assertEquals(80.0, (Double) result, 0.001);
  }

  @Test
  public void testPercent3() throws HopValueException {
    // percent3: A + (A * B / 100)
    Object result =
        ValueDataUtil.percent3(
            new ValueMetaNumber(), 100.0,
            new ValueMetaNumber(), 20.0);
    assertEquals(120.0, (Double) result, 0.001);
  }

  // Combination functions tests
  @Test
  public void testCombination1() throws HopValueException {
    // combination1: A + B * C
    Object result =
        ValueDataUtil.combination1(
            new ValueMetaNumber(), 10.0,
            new ValueMetaNumber(), 5.0,
            new ValueMetaNumber(), 2.0);
    assertEquals(20.0, (Double) result, 0.001);
  }

  @Test
  public void testCombination2() throws HopValueException {
    // combination2: SQRT(A * A + B * B)
    Object result =
        ValueDataUtil.combination2(new ValueMetaNumber(), 3.0, new ValueMetaNumber(), 4.0);
    assertEquals(5.0, (Double) result, 0.001);
  }

  // Additional date operations tests
  @Test
  public void testAddSeconds() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 10, 30, 0);
    Date date = cal.getTime();

    Date result =
        (Date) ValueDataUtil.addSeconds(new ValueMetaDate(), date, new ValueMetaInteger(), 90L);

    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(result);
    assertEquals(31, resultCal.get(Calendar.MINUTE));
    assertEquals(30, resultCal.get(Calendar.SECOND));
  }

  @Test
  public void testAddMonths() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15);
    Date date = cal.getTime();

    Date result =
        (Date) ValueDataUtil.addMonths(new ValueMetaDate(), date, new ValueMetaInteger(), 3L);

    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(result);
    assertEquals(Calendar.APRIL, resultCal.get(Calendar.MONTH));
  }

  @Test
  public void testQuarterOfDate() throws HopValueException {
    Calendar cal = Calendar.getInstance();

    // Q1 - January
    cal.set(2024, Calendar.JANUARY, 15);
    Long q1 = (Long) ValueDataUtil.quarterOfDate(new ValueMetaDate(), cal.getTime());
    assertEquals(Long.valueOf(1), q1);

    // Q2 - May
    cal.set(2024, Calendar.MAY, 15);
    Long q2 = (Long) ValueDataUtil.quarterOfDate(new ValueMetaDate(), cal.getTime());
    assertEquals(Long.valueOf(2), q2);

    // Q3 - August
    cal.set(2024, Calendar.AUGUST, 15);
    Long q3 = (Long) ValueDataUtil.quarterOfDate(new ValueMetaDate(), cal.getTime());
    assertEquals(Long.valueOf(3), q3);

    // Q4 - November
    cal.set(2024, Calendar.NOVEMBER, 15);
    Long q4 = (Long) ValueDataUtil.quarterOfDate(new ValueMetaDate(), cal.getTime());
    assertEquals(Long.valueOf(4), q4);
  }

  @Test
  public void testWeekOfYear() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15);
    Date date = cal.getTime();

    Long week = (Long) ValueDataUtil.weekOfYear(new ValueMetaDate(), date);
    assertTrue(week >= 1 && week <= 53);
  }

  @Test
  public void testWeekOfYearISO8601() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15);
    Date date = cal.getTime();

    Long week = (Long) ValueDataUtil.weekOfYearISO8601(new ValueMetaDate(), date);
    assertTrue(week >= 1 && week <= 53);
  }

  @Test
  public void testYearOfDateISO8601() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 1);
    Date date = cal.getTime();

    Long year = (Long) ValueDataUtil.yearOfDateISO8601(new ValueMetaDate(), date);
    assertTrue(year >= 2023 && year <= 2024); // ISO week year can differ at year boundaries
  }

  @Test
  public void testDayOfYear() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 1);
    Date date = cal.getTime();

    Long day = (Long) ValueDataUtil.dayOfYear(new ValueMetaDate(), date);
    assertEquals(Long.valueOf(1), day);

    cal.set(2024, Calendar.DECEMBER, 31);
    date = cal.getTime();
    day = (Long) ValueDataUtil.dayOfYear(new ValueMetaDate(), date);
    assertTrue(day == 366); // 2024 is a leap year
  }

  @Test
  public void testMinuteOfHour() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 14, 35, 0);
    Date date = cal.getTime();

    Long minute = (Long) ValueDataUtil.minuteOfHour(new ValueMetaDate(), date);
    assertEquals(Long.valueOf(35), minute);
  }

  @Test
  public void testSecondOfMinute() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15, 14, 30, 45);
    Date date = cal.getTime();

    Long second = (Long) ValueDataUtil.secondOfMinute(new ValueMetaDate(), date);
    assertEquals(Long.valueOf(45), second);
  }

  @Test
  public void testDayOfWeek() throws HopValueException {
    Calendar cal = Calendar.getInstance();
    cal.set(2024, Calendar.JANUARY, 15); // Monday
    Date date = cal.getTime();

    Long dayOfWeek = (Long) ValueDataUtil.dayOfWeek(new ValueMetaDate(), date);
    assertTrue(dayOfWeek >= 1 && dayOfWeek <= 7);
  }

  // Hex encoding tests
  @Test
  public void testCharToHexEncode() throws HopValueException {
    String hex = ValueDataUtil.charToHexEncode(new ValueMetaString(), "A");
    assertEquals("0041", hex);
  }

  @Test
  public void testHexToCharDecode() throws HopValueException {
    String decoded = ValueDataUtil.hexToCharDecode(new ValueMetaString(), "0041");
    assertEquals("A", decoded);
  }

  // String utility functions tests
  @Test
  public void testRightPadString() {
    assertEquals("test  ", ValueDataUtil.rightPad("test", 6));
    assertEquals("test", ValueDataUtil.rightPad("test", 4));
    assertEquals("te", ValueDataUtil.rightPad("test", 2)); // Truncates if limit is shorter
  }

  @Test
  public void testRightPadStringBuffer() {
    StringBuffer sb = new StringBuffer("test");
    String result = ValueDataUtil.rightPad(sb, 6);
    assertEquals("test  ", result);
  }

  @Test
  public void testReplace() {
    String result = ValueDataUtil.replace("hello world", "world", "universe");
    assertEquals("hello universe", result);

    result = ValueDataUtil.replace("test test test", "test", "TEST");
    assertEquals("TEST TEST TEST", result);
  }

  @Test
  public void testReplaceBuffer() {
    StringBuffer buffer = new StringBuffer("hello world");
    ValueDataUtil.replaceBuffer(buffer, "world", "universe");
    assertEquals("hello universe", buffer.toString());
  }

  @Test
  public void testNrSpacesBefore() {
    assertEquals(0, ValueDataUtil.nrSpacesBefore("test"));
    assertEquals(3, ValueDataUtil.nrSpacesBefore("   test"));
    assertEquals(0, ValueDataUtil.nrSpacesBefore(""));
  }

  @Test
  public void testNrSpacesAfter() {
    assertEquals(0, ValueDataUtil.nrSpacesAfter("test"));
    assertEquals(3, ValueDataUtil.nrSpacesAfter("test   "));
    assertEquals(0, ValueDataUtil.nrSpacesAfter(""));
  }

  @Test
  public void testOnlySpaces() {
    assertTrue(ValueDataUtil.onlySpaces("   "));
    assertTrue(ValueDataUtil.onlySpaces(""));
    assertFalse(ValueDataUtil.onlySpaces("  a  "));
    assertFalse(ValueDataUtil.onlySpaces("test"));
  }

  // Date difference tests
  @Test
  public void testDateDiff() throws HopValueException {
    Calendar cal1 = Calendar.getInstance();
    cal1.set(2024, Calendar.JANUARY, 1, 0, 0, 0);
    Date date1 = cal1.getTime();

    Calendar cal2 = Calendar.getInstance();
    cal2.set(2024, Calendar.JANUARY, 6, 0, 0, 0);
    Date date2 = cal2.getTime();

    Long diff =
        (Long) ValueDataUtil.dateDiff(new ValueMetaDate(), date2, new ValueMetaDate(), date1, "d");
    assertEquals(Long.valueOf(5), diff);
  }

  @Test
  public void testDateDiffHours() throws HopValueException {
    Calendar cal1 = Calendar.getInstance();
    cal1.set(2024, Calendar.JANUARY, 1, 0, 0, 0);
    Date date1 = cal1.getTime();

    Calendar cal2 = Calendar.getInstance();
    cal2.set(2024, Calendar.JANUARY, 1, 5, 0, 0);
    Date date2 = cal2.getTime();

    Long diff =
        (Long) ValueDataUtil.dateDiff(new ValueMetaDate(), date2, new ValueMetaDate(), date1, "h");
    assertEquals(Long.valueOf(5), diff);
  }

  @Test
  public void testDateDiffMinutes() throws HopValueException {
    Calendar cal1 = Calendar.getInstance();
    cal1.set(2024, Calendar.JANUARY, 1, 0, 0, 0);
    Date date1 = cal1.getTime();

    Calendar cal2 = Calendar.getInstance();
    cal2.set(2024, Calendar.JANUARY, 1, 0, 30, 0);
    Date date2 = cal2.getTime();

    Long diff =
        (Long) ValueDataUtil.dateDiff(new ValueMetaDate(), date2, new ValueMetaDate(), date1, "mn");
    assertEquals(Long.valueOf(30), diff);
  }

  // Test addTimeToDate
  @Test
  public void testAddTimeToDate() throws HopValueException {
    Calendar dateCal = Calendar.getInstance();
    dateCal.set(2024, Calendar.JANUARY, 15, 0, 0, 0);
    Date date = dateCal.getTime();

    // addTimeToDate takes a date, a time string, and an optional time format
    Date result =
        (Date)
            ValueDataUtil.addTimeToDate(
                new ValueMetaDate(), date, new ValueMetaString(), "14:30:45", null, null);

    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(result);
    assertEquals(2024, resultCal.get(Calendar.YEAR));
    assertEquals(Calendar.JANUARY, resultCal.get(Calendar.MONTH));
    assertEquals(15, resultCal.get(Calendar.DAY_OF_MONTH));
    assertEquals(14, resultCal.get(Calendar.HOUR_OF_DAY));
    assertEquals(30, resultCal.get(Calendar.MINUTE));
    assertEquals(45, resultCal.get(Calendar.SECOND));
  }

  // Test plus3
  @Test
  public void testPlus3() throws HopValueException {
    Object result =
        ValueDataUtil.plus3(
            new ValueMetaInteger(), 10L,
            new ValueMetaInteger(), 20L,
            new ValueMetaInteger(), 30L);
    assertEquals(60L, result);

    result =
        ValueDataUtil.plus3(
            new ValueMetaNumber(), 10.5,
            new ValueMetaNumber(), 20.3,
            new ValueMetaNumber(), 5.2);
    assertEquals(36.0, (Double) result, 0.001);
  }

  // Test multiplyDoubles and multiplyLongs
  @Test
  public void testMultiplyDoubles() {
    assertEquals(Double.valueOf(6.0), ValueDataUtil.multiplyDoubles(2.0, 3.0));
  }

  @Test
  public void testMultiplyLongs() {
    assertEquals(Long.valueOf(6), ValueDataUtil.multiplyLongs(2L, 3L));
  }

  // Test divideDoubles and divideLongs
  @Test
  public void testDivideDoubles() {
    assertEquals(Double.valueOf(2.0), ValueDataUtil.divideDoubles(6.0, 3.0));
  }

  @Test
  public void testDivideLongs() {
    assertEquals(Long.valueOf(2), ValueDataUtil.divideLongs(6L, 3L));
  }
}
