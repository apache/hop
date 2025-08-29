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

package org.apache.hop.pipeline.transforms.calculator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.calculator.CalculatorMetaFunction.CalculationType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.stubbing.Answer;

class CalculatorValueDataUtilTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String YYYY_MM_DD = "yyyy-MM-dd";

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void testDateDiff_A_GT_B() {
    Object daysDiff =
        calculate("2010-05-12", "2010-01-01", IValueMeta.TYPE_DATE, CalculationType.DATE_DIFF);
    assertEquals(131L, daysDiff);
  }

  @Test
  void testDateDiff_A_LT_B() {
    Object daysDiff =
        calculate(
            "2010-12-31",
            "2011-02-10",
            IValueMeta.TYPE_DATE,
            CalculatorMetaFunction.CalculationType.DATE_DIFF);
    assertEquals((long) -41, daysDiff);
  }

  @Test
  void testWorkingDaysDays_A_GT_B() {
    Object daysDiff =
        calculate(
            "2010-05-12",
            "2010-01-01",
            IValueMeta.TYPE_DATE,
            CalculatorMetaFunction.CalculationType.DATE_WORKING_DIFF);
    assertEquals(94L, daysDiff);
  }

  @Test
  void testWorkingDaysDays_A_LT_B() {
    Object daysDiff =
        calculate(
            "2010-12-31",
            "2011-02-10",
            IValueMeta.TYPE_DATE,
            CalculatorMetaFunction.CalculationType.DATE_WORKING_DIFF);
    assertEquals((long) -30, daysDiff);
  }

  @Test
  void testPlus() throws HopValueException {

    long longValue = 1;

    assertEquals(
        longValue,
        ValueDataUtil.plus(
            new ValueMetaInteger(), longValue, new ValueMetaString(), StringUtils.EMPTY));
  }

  @Test
  void checksumTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String checksum = ValueDataUtil.createChecksum(new ValueMetaString(), path, "MD5", true);
    assertEquals("098f6bcd4621d373cade4e832627b4f6", checksum);
  }

  @Test
  void checksumMissingFileTest() throws Exception {
    String nonExistingFile = "nonExistingFile";
    String checksum =
        ValueDataUtil.createChecksum(new ValueMetaString(), nonExistingFile, "MD5", false);
    assertNull(checksum);
  }

  @Test
  void checksumNullPathTest() throws Exception {
    String nonExistingFile = "nonExistingFile";
    String checksum =
        ValueDataUtil.createChecksum(new ValueMetaString(), nonExistingFile, "MD5", false);
    assertNull(checksum);
  }

  @Test
  void checksumWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String checksum = ValueDataUtil.createChecksum(new ValueMetaString(), path, "MD5", true);
    assertEquals("098f6bcd4621d373cade4e832627b4f6", checksum);
  }

  @Test
  void checksumWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String checksum = ValueDataUtil.createChecksum(new ValueMetaString(), path, "MD5", false);
    assertEquals("098f6bcd4621d373cade4e832627b4f6", checksum);
  }

  @Test
  void checksumNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingFile = "nonExistingFile";
    String checksum =
        ValueDataUtil.createChecksum(new ValueMetaString(), nonExistingFile, "MD5", false);
    assertNull(checksum);
  }

  @Test
  void checksumFailIfNoFileTest() {
    String nonExistingPath = "nonExistingPath";
    assertThrows(
        HopFileNotFoundException.class,
        () -> {
          ValueDataUtil.createChecksum(new ValueMetaString(), nonExistingPath, "MD5", true);
        });
  }

  @Test
  void checksumNullPathNoFailTest() throws HopFileNotFoundException {
    assertNull(ValueDataUtil.createChecksum(new ValueMetaString(), null, "MD5", false));
  }

  @Test
  void checksumNullPathFailTest() throws HopFileNotFoundException {
    assertNull(ValueDataUtil.createChecksum(new ValueMetaString(), null, "MD5", true));
  }

  @Test
  void checksumCRC32Test() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    long checksum = ValueDataUtil.checksumCRC32(new ValueMetaString(), path, false);
    assertEquals(3632233996L, checksum);
  }

  @Test
  void checksumCRC32MissingFileTest() throws Exception {
    String nonExistingFile = "nonExistingFile";
    long checksum = ValueDataUtil.checksumCRC32(new ValueMetaString(), nonExistingFile, false);
    assertEquals(0, checksum);
  }

  @Test
  void checksumCRC32NullPathTest() throws Exception {
    String nonExistingFile = "nonExistingFile";
    long checksum = ValueDataUtil.checksumCRC32(new ValueMetaString(), nonExistingFile, false);
    assertEquals(0, checksum);
  }

  @Test
  void checksumCRC32WithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    long checksum = ValueDataUtil.checksumCRC32(new ValueMetaString(), path, false);
    assertEquals(3632233996L, checksum);
  }

  @Test
  void checksumCRC32NoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    long checksum = ValueDataUtil.checksumCRC32(new ValueMetaString(), nonExistingPath, false);
    assertEquals(0, checksum);
  }

  @Test
  void checksumCRC32FailIfNoFileTest() {
    String nonExistingPath = "nonExistingPath";
    assertThrows(
        HopFileNotFoundException.class,
        () -> ValueDataUtil.checksumCRC32(new ValueMetaString(), nonExistingPath, true));
  }

  @Test
  void checksumCRC32NullPathNoFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumCRC32(new ValueMetaString(), null, false);
    assertEquals(0, checksum);
  }

  @Test
  void checksumCRC32NullPathFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumCRC32(new ValueMetaString(), null, true);
    assertEquals(0, checksum);
  }

  @Test
  void checksumAdlerWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    long checksum = ValueDataUtil.checksumAdler32(new ValueMetaString(), path, true);
    assertEquals(73204161L, checksum);
  }

  @Test
  void checksumAdlerWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    long checksum = ValueDataUtil.checksumAdler32(new ValueMetaString(), path, false);
    assertEquals(73204161L, checksum);
  }

  @Test
  void checksumAdlerNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    long checksum = ValueDataUtil.checksumAdler32(new ValueMetaString(), nonExistingPath, false);
    assertEquals(0, checksum);
  }

  @Test
  void checksumAdlerFailIfNoFileTest() {
    String nonExistingPath = "nonExistingPath";
    assertThrows(
        HopFileNotFoundException.class,
        () -> ValueDataUtil.checksumAdler32(new ValueMetaString(), nonExistingPath, true));
  }

  @Test
  void checksumAdlerNullPathNoFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumAdler32(new ValueMetaString(), null, false);
    assertEquals(0, checksum);
  }

  @Test
  void checksumAdlerNullPathFailTest() throws HopFileNotFoundException {
    long checksum = ValueDataUtil.checksumAdler32(new ValueMetaString(), null, true);
    assertEquals(0, checksum);
  }

  @Test
  void xmlFileWellFormedTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource("xml-sample.xml").getPath();
    boolean wellFormed =
        ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), xmlFilePath, true);
    assertTrue(wellFormed);
  }

  @Test
  void xmlFileBadlyFormedTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource("invalid-xml-sample.xml").getPath();
    boolean wellFormed =
        ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), invalidXmlFilePath, true);
    assertFalse(wellFormed);
  }

  @Test
  void xmlFileWellFormedWithFailIfNoFileTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource("xml-sample.xml").getPath();
    boolean wellFormed =
        ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), xmlFilePath, true);
    assertTrue(wellFormed);
  }

  @Test
  void xmlFileWellFormedWithoutFailIfNoFileTest() throws HopFileNotFoundException {
    String xmlFilePath = getClass().getResource("xml-sample.xml").getPath();
    boolean wellFormed =
        ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), xmlFilePath, false);
    assertTrue(wellFormed);
  }

  @Test
  void xmlFileBadlyFormedWithFailIfNoFileTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource("invalid-xml-sample.xml").getPath();
    boolean wellFormed =
        ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), invalidXmlFilePath, true);
    assertFalse(wellFormed);
  }

  @Test
  void xmlFileBadlyFormedWithNoFailIfNoFileTest() throws HopFileNotFoundException {
    String invalidXmlFilePath = getClass().getResource("invalid-xml-sample.xml").getPath();
    boolean wellFormed =
        ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), invalidXmlFilePath, false);
    assertFalse(wellFormed);
  }

  @Test
  void xmlFileWellFormedNoFailIfNoFileTest() throws HopFileNotFoundException {
    String nonExistingPath = "nonExistingPath";
    boolean wellFormed =
        ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), nonExistingPath, false);
    assertFalse(wellFormed);
  }

  @Test
  void xmlFileWellFormedFailIfNoFileTest() {
    String nonExistingPath = "nonExistingPath";
    assertThrows(
        HopFileNotFoundException.class,
        () -> ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), nonExistingPath, true));
  }

  @Test
  void xmlFileWellFormedNullPathNoFailTest() throws HopFileNotFoundException {
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), null, false);
    assertFalse(wellFormed);
  }

  @Test
  void xmlFileWellFormedNullPathFailTest() throws HopFileNotFoundException {
    boolean wellFormed = ValueDataUtil.isXmlFileWellFormed(new ValueMetaString(), null, true);
    assertFalse(wellFormed);
  }

  @Test
  void loadFileContentInBinary() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    byte[] content = ValueDataUtil.loadFileContentInBinary(new ValueMetaString(), path, true);
    assertArrayEquals("test".getBytes(), content);
  }

  @Test
  void loadFileContentInBinaryNoFailIfNoFileTest() throws Exception {
    String nonExistingPath = "nonExistingPath";
    assertNull(
        ValueDataUtil.loadFileContentInBinary(new ValueMetaString(), nonExistingPath, false));
  }

  @Test
  void loadFileContentInBinaryFailIfNoFileTest() {
    String nonExistingPath = "nonExistingPath";
    assertThrows(
        HopFileNotFoundException.class,
        () -> ValueDataUtil.loadFileContentInBinary(new ValueMetaString(), nonExistingPath, true));
  }

  @Test
  void loadFileContentInBinaryNullPathNoFailTest() throws Exception {
    assertNull(ValueDataUtil.loadFileContentInBinary(new ValueMetaString(), null, false));
  }

  @Test
  void loadFileContentInBinaryNullPathFailTest()
      throws HopFileNotFoundException, HopValueException {
    assertNull(ValueDataUtil.loadFileContentInBinary(new ValueMetaString(), null, true));
  }

  @Test
  void getFileEncodingWithFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), path, true);
    assertEquals("US-ASCII", encoding);
  }

  @Test
  void getFileEncodingWithoutFailIfNoFileTest() throws Exception {
    String path = getClass().getResource("txt-sample.txt").getPath();
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), path, false);
    assertEquals("US-ASCII", encoding);
  }

  @Test
  void getFileEncodingNoFailIfNoFileTest() throws Exception {
    String nonExistingPath = "nonExistingPath";
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), nonExistingPath, false);
    assertNull(encoding);
  }

  @Test
  void getFileEncodingFailIfNoFileTest() {
    String nonExistingPath = "nonExistingPath";
    assertThrows(
        HopFileNotFoundException.class,
        () -> ValueDataUtil.getFileEncoding(new ValueMetaString(), nonExistingPath, true));
  }

  @Test
  void getFileEncodingNullPathNoFailTest() throws Exception {
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), null, false);
    assertNull(encoding);
  }

  @Test
  void getFileEncodingNullPathFailTest() throws HopFileNotFoundException, HopValueException {
    String encoding = ValueDataUtil.getFileEncoding(new ValueMetaString(), null, true);
    assertNull(encoding);
  }

  @Test
  void testAdd() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("3.0"),
        calculate("1", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ADD));
    assertEquals(
        Double.valueOf("0.0"),
        calculate("2", "-2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ADD));
    assertEquals(
        Double.valueOf("30.0"),
        calculate("10", "20", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ADD));
    assertEquals(
        Double.valueOf("-50.0"),
        calculate(
            "-100", "50", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ADD));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("3"),
        calculate("1", "2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ADD));
    assertEquals(
        Long.valueOf("0"),
        calculate("2", "-2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ADD));
    assertEquals(
        Long.valueOf("30"),
        calculate("10", "20", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ADD));
    assertEquals(
        Long.valueOf("-50"),
        calculate(
            "-100", "50", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ADD));

    // Test Hop big Number types
    assertEquals(
        0,
        new BigDecimal("2.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "1",
                        "1",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD)));
    assertEquals(
        0,
        new BigDecimal("0.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "2",
                        "-2",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD)));
    assertEquals(
        0,
        new BigDecimal("30.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "10",
                        "20",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD)));
    assertEquals(
        0,
        new BigDecimal("-50.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "-100",
                        "50",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD)));
  }

  @Test
  void testAdd3() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("6.0"),
        calculate(
            "1", "2", "3", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ADD3));
    assertEquals(
        Double.valueOf("10.0"),
        calculate(
            "2", "-2", "10", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ADD3));
    assertEquals(
        Double.valueOf("27.0"),
        calculate(
            "10", "20", "-3", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ADD3));
    assertEquals(
        Double.valueOf("-55.0"),
        calculate(
            "-100",
            "50",
            "-5",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.ADD3));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("3"),
        calculate(
            "1", "1", "1", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ADD3));
    assertEquals(
        Long.valueOf("10"),
        calculate(
            "2", "-2", "10", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ADD3));
    assertEquals(
        Long.valueOf("27"),
        calculate(
            "10",
            "20",
            "-3",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.ADD3));
    assertEquals(
        Long.valueOf("-55"),
        calculate(
            "-100",
            "50",
            "-5",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.ADD3));

    // Test Hop big Number types
    assertEquals(
        0,
        new BigDecimal("6.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "1",
                        "2",
                        "3",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD3)));
    assertEquals(
        0,
        new BigDecimal("10.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "2",
                        "-2",
                        "10",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD3)));
    assertEquals(
        0,
        new BigDecimal("27.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "10",
                        "20",
                        "-3",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD3)));
    assertEquals(
        0,
        new BigDecimal("-55.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "-100",
                        "50",
                        "-5",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.ADD3)));
  }

  @Test
  void testSubtract() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("10.0"),
        calculate(
            "20", "10", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.SUBTRACT));
    assertEquals(
        Double.valueOf("-10.0"),
        calculate(
            "10", "20", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.SUBTRACT));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("10"),
        calculate(
            "20", "10", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.SUBTRACT));
    assertEquals(
        Long.valueOf("-10"),
        calculate(
            "10", "20", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.SUBTRACT));

    // Test Hop big Number types
    assertEquals(
        0,
        new BigDecimal("10")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "20",
                        "10",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.SUBTRACT)));
    assertEquals(
        0,
        new BigDecimal("-10")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "10",
                        "20",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.SUBTRACT)));
  }

  @Test
  void testDivide() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("2.0"),
        calculate("2", "1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        Double.valueOf("2.0"),
        calculate("4", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        Double.valueOf("0.5"),
        calculate(
            "10", "20", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        Double.valueOf("2.0"),
        calculate(
            "100", "50", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "2", "1", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "4", "2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        Long.valueOf("0"),
        calculate(
            "10", "20", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "100", "50", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.DIVIDE));

    // Test Hop big Number types
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("2")),
        calculate(
            "2", "1", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("2")),
        calculate(
            "4", "2", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("0.5")),
        calculate(
            "10", "20", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("2")),
        calculate(
            "100", "50", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.DIVIDE));
  }

  @Test
  void testMulitplyBigNumbers() {
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
  void testDivisionBigNumbers() {
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
  void testRemainderBigNumbers() throws Exception {
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
  void testPercent1() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("10.0"),
        calculate(
            "10", "100", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        Double.valueOf("100.0"),
        calculate(
            "2", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        Double.valueOf("50.0"),
        calculate(
            "10", "20", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        Double.valueOf("200.0"),
        calculate(
            "100", "50", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_1));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("10"),
        calculate(
            "10",
            "100",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        Long.valueOf("100"),
        calculate(
            "2", "2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        Long.valueOf("50"),
        calculate(
            "10", "20", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        Long.valueOf("200"),
        calculate(
            "100",
            "50",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.PERCENT_1));

    // Test Hop big Number types
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("10")),
        calculate(
            "10",
            "100",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("100")),
        calculate(
            "2", "2", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("50")),
        calculate(
            "10",
            "20",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.PERCENT_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("200")),
        calculate(
            "100",
            "50",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.PERCENT_1));
  }

  @Test
  void testPercent2() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("0.99"),
        calculate(
            "1", "1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        Double.valueOf("1.96"),
        calculate(
            "2", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        Double.valueOf("8.0"),
        calculate(
            "10", "20", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        Double.valueOf("50.0"),
        calculate(
            "100", "50", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_2));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("1"),
        calculate(
            "1", "1", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "2", "2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        Long.valueOf("8"),
        calculate(
            "10", "20", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        Long.valueOf("50"),
        calculate(
            "100",
            "50",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.PERCENT_2));

    // Test Hop big Number types
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("0.99")),
        calculate(
            "1", "1", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("1.96")),
        calculate(
            "2", "2", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        new BigDecimal("8.0"),
        calculate(
            "10",
            "20",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.PERCENT_2));
    assertEquals(
        new BigDecimal("50.0"),
        calculate(
            "100",
            "50",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.PERCENT_2));
  }

  @Test
  void testPercent3() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("1.01"),
        calculate(
            "1", "1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_3));
    assertEquals(
        Double.valueOf("2.04"),
        calculate(
            "2", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_3));
    assertEquals(
        Double.valueOf("12.0"),
        calculate(
            "10", "20", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_3));
    assertEquals(
        Double.valueOf("150.0"),
        calculate(
            "100", "50", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.PERCENT_3));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("1"),
        calculate(
            "1", "1", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_3));
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "2", "2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_3));
    assertEquals(
        Long.valueOf("12"),
        calculate(
            "10", "20", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.PERCENT_3));
    assertEquals(
        Long.valueOf("150"),
        calculate(
            "100",
            "50",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.PERCENT_3));

    // Test Hop big Number types
    assertEquals(
        0,
        new BigDecimal("1.01")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "1",
                        "1",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.PERCENT_3)));
    assertEquals(
        0,
        new BigDecimal("2.04")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "2",
                        "2",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.PERCENT_3)));
    assertEquals(
        0,
        new BigDecimal("12")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "10",
                        "20",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.PERCENT_3)));
    assertEquals(
        0,
        new BigDecimal("150")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "100",
                        "50",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.PERCENT_3)));
  }

  @Test
  void testCombination1() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("2.0"),
        calculate(
            "1",
            "1",
            "1",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));
    assertEquals(
        Double.valueOf("22.0"),
        calculate(
            "2",
            "2",
            "10",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));
    assertEquals(
        Double.valueOf("70.0"),
        calculate(
            "10",
            "20",
            "3",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));
    assertEquals(
        Double.valueOf("350"),
        calculate(
            "100",
            "50",
            "5",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "1",
            "1",
            "1",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));
    assertEquals(
        Long.valueOf("22"),
        calculate(
            "2",
            "2",
            "10",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));
    assertEquals(
        Long.valueOf("70"),
        calculate(
            "10",
            "20",
            "3",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));
    assertEquals(
        Long.valueOf("350"),
        calculate(
            "100",
            "50",
            "5",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_1));

    // Test Hop big Number types
    assertEquals(
        0,
        new BigDecimal("2.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "1",
                        "1",
                        "1",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_1)));
    assertEquals(
        0,
        new BigDecimal("22.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "2",
                        "2",
                        "10",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_1)));
    assertEquals(
        0,
        new BigDecimal("70.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "10",
                        "20",
                        "3",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_1)));
    assertEquals(
        0,
        new BigDecimal("350.0")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "100",
                        "50",
                        "5",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_1)));
  }

  @Test
  void testCombination2() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("1.4142135623730951"),
        calculate(
            "1",
            "1",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));
    assertEquals(
        Double.valueOf("2.8284271247461903"),
        calculate(
            "2",
            "2",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));
    assertEquals(
        Double.valueOf("22.360679774997898"),
        calculate(
            "10",
            "20",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));
    assertEquals(
        Double.valueOf("111.80339887498948"),
        calculate(
            "100",
            "50",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("1"),
        calculate(
            "1",
            "1",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "2",
            "2",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));
    assertEquals(
        Long.valueOf("10"),
        calculate(
            "10",
            "20",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));
    assertEquals(
        Long.valueOf("100"),
        calculate(
            "100",
            "50",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.COMBINATION_2));

    // Test Hop big Number types
    assertEquals(
        0,
        new BigDecimal("1.4142135623730951")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "1",
                        "1",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_2)));
    assertEquals(
        0,
        new BigDecimal("2.8284271247461903")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "2",
                        "2",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_2)));
    assertEquals(
        0,
        new BigDecimal("22.360679774997898")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "10",
                        "20",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_2)));
    assertEquals(
        0,
        new BigDecimal("111.80339887498948")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "100",
                        "50",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.COMBINATION_2)));
  }

  @Test
  void testRound() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("1.0"),
        calculate("1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        Double.valueOf("103.0"),
        calculate(
            "103.01", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        Double.valueOf("1235.0"),
        calculate(
            "1234.6", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    // half
    assertEquals(
        Double.valueOf("1235.0"),
        calculate(
            "1234.5", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        Double.valueOf("1236.0"),
        calculate(
            "1235.5", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        Double.valueOf("-1234.0"),
        calculate(
            "-1234.5", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        Double.valueOf("-1235.0"),
        calculate(
            "-1235.5", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("1"),
        calculate("1", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        Long.valueOf("2"),
        calculate("2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        Long.valueOf("-103"),
        calculate("-103", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_1));

    // Test Hop big Number types
    assertEquals(
        BigDecimal.ONE,
        calculate("1", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("103")),
        calculate(
            "103.01", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("1235")),
        calculate(
            "1234.6", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    // half
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("1235")),
        calculate(
            "1234.5", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("1236")),
        calculate(
            "1235.5", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("-1234")),
        calculate(
            "-1234.5", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
    assertEquals(
        BigDecimal.valueOf(Long.parseLong("-1235")),
        calculate(
            "-1235.5", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_1));
  }

  @Test
  void testRound2() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("1.0"),
        calculate(
            "1", "1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Double.valueOf("2.1"),
        calculate(
            "2.06", "1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Double.valueOf("103.0"),
        calculate(
            "103.01", "1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Double.valueOf("12.35"),
        calculate(
            "12.346", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    // scale < 0
    assertEquals(
        Double.valueOf("10.0"),
        calculate(
            "12.0", "-1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    // half
    assertEquals(
        Double.valueOf("12.35"),
        calculate(
            "12.345", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Double.valueOf("12.36"),
        calculate(
            "12.355", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Double.valueOf("-12.34"),
        calculate(
            "-12.345",
            "2",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Double.valueOf("-12.35"),
        calculate(
            "-12.355",
            "2",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("1"),
        calculate(
            "1", "1", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Long.valueOf("2"),
        calculate(
            "2", "2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Long.valueOf("103"),
        calculate(
            "103", "3", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Long.valueOf("12"),
        calculate(
            "12", "4", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_2));
    // scale < 0
    assertEquals(
        Long.valueOf("100"),
        calculate(
            "120", "-2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.ROUND_2));
    // half
    assertEquals(
        Long.valueOf("12350"),
        calculate(
            "12345",
            "-1",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Long.valueOf("12360"),
        calculate(
            "12355",
            "-1",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Long.valueOf("-12340"),
        calculate(
            "-12345",
            "-1",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        Long.valueOf("-12350"),
        calculate(
            "-12355",
            "-1",
            IValueMeta.TYPE_INTEGER,
            CalculatorMetaFunction.CalculationType.ROUND_2));

    // Test Hop big Number types
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("1.0")),
        calculate(
            "1", "1", IValueMeta.TYPE_BIGNUMBER, CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("2.1")),
        calculate(
            "2.06",
            "1",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("103.0")),
        calculate(
            "103.01",
            "1",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("12.35")),
        calculate(
            "12.346",
            "2",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    // scale < 0
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("10.0")).setScale(-1),
        calculate(
            "12.0",
            "-1",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    // half
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("12.35")),
        calculate(
            "12.345",
            "2",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("12.36")),
        calculate(
            "12.355",
            "2",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("-12.34")),
        calculate(
            "-12.345",
            "2",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
    assertEquals(
        BigDecimal.valueOf(Double.parseDouble("-12.35")),
        calculate(
            "-12.355",
            "2",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.ROUND_2));
  }

  @Test
  void testNVL() {

    // Test Hop number types
    assertEquals(
        Double.valueOf("1.0"),
        calculate("1", "", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        Double.valueOf("2.0"),
        calculate("", "2", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        Double.valueOf("10.0"),
        calculate("10", "20", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.NVL));
    assertNull(calculate("", "", IValueMeta.TYPE_NUMBER, CalculationType.NVL));

    // Test Hop string types
    assertEquals(
        "1",
        calculate("1", "", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        "2",
        calculate("", "2", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        "10",
        calculate("10", "20", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CalculationType.NVL));
    assertNull(calculate("", "", IValueMeta.TYPE_STRING, CalculationType.NVL));

    // Test Hop Integer (Java Long) types
    assertEquals(
        Long.valueOf("1"),
        calculate("1", "", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        Long.valueOf("2"),
        calculate("", "2", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        Long.valueOf("10"),
        calculate("10", "20", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.NVL));
    assertNull(calculate("", "", IValueMeta.TYPE_INTEGER, CalculationType.NVL));

    // Test Hop big Number types
    assertEquals(
        0,
        new BigDecimal("1")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "1",
                        "",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.NVL)));
    assertEquals(
        0,
        new BigDecimal("2")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "",
                        "2",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.NVL)));
    assertEquals(
        0,
        new BigDecimal("10")
            .compareTo(
                (BigDecimal)
                    calculate(
                        "10",
                        "20",
                        IValueMeta.TYPE_BIGNUMBER,
                        CalculatorMetaFunction.CalculationType.NVL)));
    assertNull(calculate("", "", IValueMeta.TYPE_BIGNUMBER, CalculationType.NVL));

    // boolean
    assertEquals(
        true,
        calculate("true", "", IValueMeta.TYPE_BOOLEAN, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        false,
        calculate(
            "", "false", IValueMeta.TYPE_BOOLEAN, CalculatorMetaFunction.CalculationType.NVL));
    assertEquals(
        false,
        calculate(
            "false", "true", IValueMeta.TYPE_BOOLEAN, CalculatorMetaFunction.CalculationType.NVL));
    assertNull(calculate("", "", IValueMeta.TYPE_BOOLEAN, CalculationType.NVL));

    // Test Hop date
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYY_MM_DD);

    try {
      assertEquals(
          simpleDateFormat.parse("2012-04-11"),
          calculate(
              "2012-04-11", "", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CalculationType.NVL));
      assertEquals(
          simpleDateFormat.parse("2012-11-04"),
          calculate(
              "", "2012-11-04", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CalculationType.NVL));
      assertEquals(
          simpleDateFormat.parse("1965-07-01"),
          calculate(
              "1965-07-01",
              "1967-04-11",
              IValueMeta.TYPE_DATE,
              CalculatorMetaFunction.CalculationType.NVL));
      assertNull(
          calculate("", "", IValueMeta.TYPE_DATE, CalculatorMetaFunction.CalculationType.NVL));

    } catch (ParseException pe) {
      fail(pe.getMessage());
    }

    // binary
    IValueMeta stringValueMeta = new ValueMetaString("string");
    try {
      byte[] data = stringValueMeta.getBinary("101");
      byte[] calculated =
          (byte[])
              calculate(
                  "101", "", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CalculationType.NVL);
      assertArrayEquals(data, calculated);

      data = stringValueMeta.getBinary("011");
      calculated =
          (byte[])
              calculate(
                  "", "011", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CalculationType.NVL);
      assertArrayEquals(data, calculated);

      data = stringValueMeta.getBinary("110");
      calculated =
          (byte[])
              calculate(
                  "110", "011", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CalculationType.NVL);
      assertArrayEquals(data, calculated);

      calculated =
          (byte[])
              calculate("", "", IValueMeta.TYPE_BINARY, CalculatorMetaFunction.CalculationType.NVL);
      assertNull(calculated);

    } catch (HopValueException kve) {
      fail(kve.getMessage());
    }
  }

  @Test
  void testRemainder() {
    assertNull(
        calculate(
            null, null, IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.REMAINDER));
    assertNull(
        calculate(
            null, "3", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.REMAINDER));
    assertNull(
        calculate(
            "10", null, IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.REMAINDER));
    assertEquals(
        Long.valueOf("1"),
        calculate(
            "10", "3", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.REMAINDER));
    assertEquals(
        Long.valueOf("-1"),
        calculate(
            "-10", "3", IValueMeta.TYPE_INTEGER, CalculatorMetaFunction.CalculationType.REMAINDER));

    Double comparisonDelta = Double.valueOf("0.0000000000001");
    assertNull(
        calculate(
            null, null, IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.REMAINDER));
    assertNull(
        calculate(
            null, "4.1", IValueMeta.TYPE_NUMBER, CalculatorMetaFunction.CalculationType.REMAINDER));
    assertNull(
        calculate(
            "17.8",
            null,
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));
    assertEquals(
        Double.parseDouble("1.4"),
        (Double) calculate("17.8", "4.1", IValueMeta.TYPE_NUMBER, CalculationType.REMAINDER),
        comparisonDelta);
    assertEquals(
        Double.parseDouble("1.4"),
        (Double) calculate("17.8", "-4.1", IValueMeta.TYPE_NUMBER, CalculationType.REMAINDER),
        comparisonDelta);

    assertEquals(
        Double.parseDouble("-1.4"),
        (Double) calculate("-17.8", "-4.1", IValueMeta.TYPE_NUMBER, CalculationType.REMAINDER),
        comparisonDelta);

    assertNull(
        calculate(
            null,
            null,
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));
    assertNull(
        calculate(
            null,
            "16.12",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));
    assertNull(
        calculate(
            "-144.144",
            null,
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));

    assertEquals(
        new BigDecimal("-15.184"),
        calculate(
            "-144.144",
            "16.12",
            IValueMeta.TYPE_BIGNUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));
    assertEquals(
        Double.valueOf("2.6000000000000005"),
        calculate(
            "12.5",
            "3.3",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));
    assertEquals(
        Double.valueOf("4.0"),
        calculate(
            "12.5",
            "4.25",
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));
    assertEquals(
        Long.valueOf("1"),
        calculate(
            "10",
            "3.3",
            null,
            IValueMeta.TYPE_INTEGER,
            IValueMeta.TYPE_NUMBER,
            IValueMeta.TYPE_NUMBER,
            CalculatorMetaFunction.CalculationType.REMAINDER));
  }

  @Test
  void testSumWithNullValues() throws Exception {
    IValueMeta metaA = new ValueMetaInteger();
    metaA.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    IValueMeta metaB = new ValueMetaInteger();
    metaA.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

    assertNull(ValueDataUtil.sum(metaA, null, metaB, null));

    Long valueB = 2L;
    ValueDataUtil.sum(metaA, null, metaB, valueB);
  }

  @Test
  void testSumConvertingStorageTypeToNormal() throws Exception {
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

  @Test
  void testJaro() {
    assertEquals(
        Double.valueOf("0.0"),
        calculate(
            "abcd", "defg", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CalculationType.JARO));
    assertEquals(
        Double.valueOf("0.44166666666666665"),
        calculate(
            "elephant",
            "hippo",
            IValueMeta.TYPE_STRING,
            CalculatorMetaFunction.CalculationType.JARO));
    assertEquals(
        Double.valueOf("0.8666666666666667"),
        calculate(
            "hello", "hallo", IValueMeta.TYPE_STRING, CalculatorMetaFunction.CalculationType.JARO));
  }

  @Test
  void testJaroWinkler() {
    assertEquals(
        Double.valueOf("0.0"),
        calculate(
            "abcd",
            "defg",
            IValueMeta.TYPE_STRING,
            CalculatorMetaFunction.CalculationType.JARO_WINKLER));
  }

  private Object calculate(
      String stringDataA, int valueMetaInterfaceType, CalculationType calculatorMetaFunction) {
    return calculate(stringDataA, null, null, valueMetaInterfaceType, calculatorMetaFunction);
  }

  private Object calculate(
      String stringDataA,
      String stringDataB,
      int valueMetaInterfaceType,
      CalculationType calculatorMetaFunction) {
    return calculate(
        stringDataA, stringDataB, null, valueMetaInterfaceType, calculatorMetaFunction);
  }

  private Object createObject(
      String stringValue, int valueMetaInterfaceType, IValueMeta parameterValueMeta)
      throws HopValueException {
    if (valueMetaInterfaceType == IValueMeta.TYPE_NUMBER) {
      return (!Utils.isEmpty(stringValue) ? Double.valueOf(stringValue) : null);
    } else if (valueMetaInterfaceType == IValueMeta.TYPE_INTEGER) {
      return (!Utils.isEmpty(stringValue) ? Long.valueOf(stringValue) : null);
    } else if (valueMetaInterfaceType == IValueMeta.TYPE_DATE) {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYY_MM_DD);
      try {
        return (!Utils.isEmpty(stringValue) ? simpleDateFormat.parse(stringValue) : null);
      } catch (ParseException pe) {
        fail(pe.getMessage());
        return null;
      }
    } else if (valueMetaInterfaceType == IValueMeta.TYPE_BIGNUMBER) {
      return (!Utils.isEmpty(stringValue)
          ? BigDecimal.valueOf(Double.parseDouble(stringValue))
          : null);
    } else if (valueMetaInterfaceType == IValueMeta.TYPE_STRING) {
      return (!Utils.isEmpty(stringValue) ? stringValue : null);
    } else if (valueMetaInterfaceType == IValueMeta.TYPE_BINARY) {
      IValueMeta binaryValueMeta = new ValueMetaBinary("binary_data");
      return (!Utils.isEmpty(stringValue)
          ? binaryValueMeta.convertData(parameterValueMeta, stringValue)
          : null);
    } else if (valueMetaInterfaceType == IValueMeta.TYPE_BOOLEAN) {
      if (!Utils.isEmpty(stringValue)) {
        return (stringValue.equalsIgnoreCase("true"));
      } else {
        return null;
      }
    } else {
      fail("Invalid IValueMeta type.");
      return null;
    }
  }

  private Object calculate(
      String stringDataA,
      String stringDataB,
      String stringDataC,
      int valueMetaInterfaceTypeABC,
      CalculationType calculatorMetaFunction) {
    return calculate(
        stringDataA,
        stringDataB,
        stringDataC,
        valueMetaInterfaceTypeABC,
        valueMetaInterfaceTypeABC,
        valueMetaInterfaceTypeABC,
        calculatorMetaFunction);
  }

  private Object calculate(
      String stringDataA,
      String stringDataB,
      String stringDataC,
      int valueMetaInterfaceTypeA,
      int valueMetaInterfaceTypeB,
      int valueMetaInterfaceTypeC,
      CalculationType calculatorMetaFunction) {

    try {

      //
      IValueMeta parameterValueMeta = new ValueMetaString("parameter");

      // We create the meta information for
      IValueMeta valueMetaA = createValueMeta("data_A", valueMetaInterfaceTypeA);
      IValueMeta valueMetaB = createValueMeta("data_B", valueMetaInterfaceTypeB);
      IValueMeta valueMetaC = createValueMeta("data_C", valueMetaInterfaceTypeC);

      Object dataA = createObject(stringDataA, valueMetaInterfaceTypeA, parameterValueMeta);
      Object dataB = createObject(stringDataB, valueMetaInterfaceTypeB, parameterValueMeta);
      Object dataC = createObject(stringDataC, valueMetaInterfaceTypeC, parameterValueMeta);

      if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.ADD) {
        return ValueDataUtil.plus(valueMetaA, dataA, valueMetaB, dataB);
      }
      if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.ADD3) {
        return ValueDataUtil.plus3(valueMetaA, dataA, valueMetaB, dataB, valueMetaC, dataC);
      }
      if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.SUBTRACT) {
        return ValueDataUtil.minus(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.DIVIDE) {
        return ValueDataUtil.divide(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.PERCENT_1) {
        return ValueDataUtil.percent1(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.PERCENT_2) {
        return ValueDataUtil.percent2(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.PERCENT_3) {
        return ValueDataUtil.percent3(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.COMBINATION_1) {
        return ValueDataUtil.combination1(valueMetaA, dataA, valueMetaB, dataB, valueMetaC, dataC);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.COMBINATION_2) {
        return ValueDataUtil.combination2(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.ROUND_1) {
        return ValueDataUtil.round(valueMetaA, dataA);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.ROUND_2) {
        return ValueDataUtil.round(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.NVL) {
        return ValueDataUtil.nvl(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.DATE_DIFF) {
        return ValueDataUtil.DateDiff(valueMetaA, dataA, valueMetaB, dataB, "");
      } else if (calculatorMetaFunction
          == CalculatorMetaFunction.CalculationType.DATE_WORKING_DIFF) {
        return ValueDataUtil.DateWorkingDiff(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.REMAINDER) {
        return ValueDataUtil.remainder(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.JARO) {
        return ValueDataUtil.getJaro_Similitude(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.JARO_WINKLER) {
        return ValueDataUtil.getJaroWinkler_Similitude(valueMetaA, dataA, valueMetaB, dataB);
      } else if (calculatorMetaFunction == CalculatorMetaFunction.CalculationType.MULTIPLY) {
        return ValueDataUtil.multiply(valueMetaA, dataA, valueMetaB, dataB);
      } else {
        fail("Invalid CalculatorMetaFunction specified.");
        return null;
      }
    } catch (HopValueException kve) {
      fail(kve.getMessage());
      return null;
    }
  }

  private IValueMeta createValueMeta(String name, int valueType) {
    try {
      return ValueMetaFactory.createValueMeta(name, valueType);
    } catch (HopPluginException e) {
      throw new RuntimeException(e);
    }
  }
}
