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

package org.apache.hop.testing;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataSetCsvWriterTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void writeAndReadRoundTrip() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("roundtrip");
    dataSet.setBaseFilename("roundtrip.csv");
    dataSet.setFields(
        List.of(
            new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0"),
            new DataSetField("name", IValueMeta.TYPE_STRING, -1, -1, "", "")));

    IRowMeta rowMeta = dataSet.getSetRowMeta();
    try (DataSetCsvWriter writer = new DataSetCsvWriter(variables, dataSet, rowMeta)) {
      writer.writeRow(new Object[] {1L, "Alice"});
      writer.writeRow(new Object[] {2L, "Bob"});
    }

    List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);
    assertEquals(2, rows.size());
    assertEquals(1L, rows.get(0)[0]);
    assertEquals("Alice", rows.get(0)[1]);
    assertEquals(2L, rows.get(1)[0]);
    assertEquals("Bob", rows.get(1)[1]);
  }

  @Test
  void emptyStreamWritesHeaderOnly() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("empty");
    dataSet.setBaseFilename("empty.csv");
    dataSet.setFields(List.of(new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0")));

    IRowMeta rowMeta = dataSet.getSetRowMeta();
    try (DataSetCsvWriter writer = new DataSetCsvWriter(variables, dataSet, rowMeta)) {
      // no rows
    }

    Path csv = tempDir.resolve("empty.csv");
    assertTrue(Files.exists(csv));
    String content = Files.readString(csv);
    assertTrue(content.contains("id"));
    assertEquals(0, DataSetCsvUtil.getAllRows(variables, dataSet).size());
  }

  @Test
  void writeDataSetDataUsesStreamingWriter() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("batch");
    dataSet.setBaseFilename("batch.csv");
    dataSet.setFields(List.of(new DataSetField("name", IValueMeta.TYPE_STRING, -1, -1, "", "")));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    DataSetCsvUtil.writeDataSetData(
        variables, dataSet, rowMeta, List.of(new Object[] {"one"}, new Object[] {"two"}));

    List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);
    assertEquals(2, rows.size());
    assertEquals("one", rows.get(0)[0]);
    assertEquals("two", rows.get(1)[0]);
  }

  @Test
  void createsMissingParentFolder() throws Exception {
    Variables variables = new Variables();
    Path nested = tempDir.resolve("nested").resolve("sets");
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, nested.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("nested");
    dataSet.setBaseFilename("nested.csv");
    dataSet.setFields(List.of(new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0")));

    try (DataSetCsvWriter writer =
        new DataSetCsvWriter(variables, dataSet, dataSet.getSetRowMeta())) {
      writer.writeRow(new Object[] {7L});
    }

    assertTrue(Files.exists(nested.resolve("nested.csv")));
  }

  @Test
  void numberWithoutFormatKeepsAllDigits() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("numbers");
    dataSet.setBaseFilename("numbers.csv");
    dataSet.setFields(
        List.of(
            new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0"),
            new DataSetField("pi", IValueMeta.TYPE_NUMBER, -1, -1, "", "")));

    double[] values = {3.14, -1234.5678, 0.1 + 0.2, 1e-20, 12345678901.25, 7.0};
    IRowMeta rowMeta = dataSet.getSetRowMeta();
    try (DataSetCsvWriter writer = new DataSetCsvWriter(variables, dataSet, rowMeta)) {
      for (int i = 0; i < values.length; i++) {
        writer.writeRow(new Object[] {(long) i, values[i]});
      }
      writer.writeRow(new Object[] {(long) values.length, null});
    }

    // The caller's row metadata must not be touched
    assertTrue(rowMeta.getValueMeta(1).getConversionMask().isEmpty());

    String csv = Files.readString(tempDir.resolve("numbers.csv"));
    assertTrue(csv.contains("0,3.14"), csv);
    // The previous "0.#" mask wrote -1234.6
    assertTrue(csv.contains("1,-1234.5678"), csv);

    List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);
    assertEquals(values.length + 1, rows.size());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], (Double) rows.get(i)[1]);
    }
    assertNull(rows.get(values.length)[1]);
  }

  @Test
  void bigNumberWithoutFormatKeepsAllDigits() throws Exception {
    Locale defaultLocale = Locale.getDefault();
    // A locale with a comma decimal separator and a dot grouping separator
    Locale.setDefault(Locale.GERMANY);
    try {
      Variables variables = new Variables();
      variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

      DataSet dataSet = new DataSet();
      dataSet.setName("big-numbers");
      dataSet.setBaseFilename("big-numbers.csv");
      dataSet.setFields(
          List.of(
              new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0"),
              new DataSetField("unsized", IValueMeta.TYPE_BIGNUMBER, -1, -1, "", ""),
              new DataSetField("sized", IValueMeta.TYPE_BIGNUMBER, 10, 2, "", "")));

      BigDecimal[] values = {
        new BigDecimal("1234.56789"),
        new BigDecimal("-1234567.5"),
        new BigDecimal("0.1234567890123456789012345"),
        new BigDecimal("12345678901234567890.5")
      };
      try (DataSetCsvWriter writer =
          new DataSetCsvWriter(variables, dataSet, dataSet.getSetRowMeta())) {
        for (int i = 0; i < values.length; i++) {
          writer.writeRow(new Object[] {(long) i, values[i], values[i]});
        }
        writer.writeRow(new Object[] {(long) values.length, null, null});
      }

      String csv = Files.readString(tempDir.resolve("big-numbers.csv"));
      // No grouping, a dot decimal separator and no zero-padding to the declared length
      assertTrue(csv.contains("0,1234.56789,1234.56789"), csv);
      assertTrue(csv.contains("1,-1234567.5,-1234567.5"), csv);

      List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);
      assertEquals(values.length + 1, rows.size());
      for (int i = 0; i < values.length; i++) {
        assertEquals(0, values[i].compareTo((BigDecimal) rows.get(i)[1]), csv);
        assertEquals(0, values[i].compareTo((BigDecimal) rows.get(i)[2]), csv);
      }
      assertNull(rows.get(values.length)[1]);
      assertNull(rows.get(values.length)[2]);
    } finally {
      Locale.setDefault(defaultLocale);
    }
  }

  @Test
  void bigNumberWrittenZeroPaddedIsStillRead() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("padded");
    dataSet.setBaseFilename("padded.csv");
    dataSet.setFields(List.of(new DataSetField("sized", IValueMeta.TYPE_BIGNUMBER, 10, 2, "", "")));

    // What earlier versions wrote for a BigNumber(10,2) field without a format
    Files.writeString(tempDir.resolve("padded.csv"), "sized\n\" 00001234.57\"\n-01234567.50\n");

    List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);
    assertEquals(2, rows.size());
    assertEquals(0, new BigDecimal("1234.57").compareTo((BigDecimal) rows.get(0)[0]));
    assertEquals(0, new BigDecimal("-1234567.5").compareTo((BigDecimal) rows.get(1)[0]));
  }

  @Test
  void numberWithFormatUsesThatFormat() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("formatted");
    dataSet.setBaseFilename("formatted.csv");
    dataSet.setFields(List.of(new DataSetField("pi", IValueMeta.TYPE_NUMBER, -1, -1, "", "0.00")));

    try (DataSetCsvWriter writer =
        new DataSetCsvWriter(variables, dataSet, dataSet.getSetRowMeta())) {
      writer.writeRow(new Object[] {3.14159});
    }

    List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);
    assertEquals(3.14, (Double) rows.get(0)[0]);
  }

  @Test
  void binaryValuesRoundTripAsLowercaseHex() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());

    DataSet dataSet = new DataSet();
    dataSet.setName("binary");
    dataSet.setBaseFilename("binary.csv");
    dataSet.setFields(
        List.of(
            new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0"),
            new DataSetField("payload", IValueMeta.TYPE_BINARY, -1, -1, "", "")));

    IRowMeta rowMeta = dataSet.getSetRowMeta();
    byte[] hash = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
    byte[] nulHigh = {0x00, (byte) 0xff};
    try (DataSetCsvWriter writer = new DataSetCsvWriter(variables, dataSet, rowMeta)) {
      writer.writeRow(new Object[] {1L, hash});
      writer.writeRow(new Object[] {2L, nulHigh});
      writer.writeRow(new Object[] {3L, null});
    }

    String csv = Files.readString(tempDir.resolve("binary.csv"));
    assertTrue(csv.contains("deadbeef"), csv);
    assertTrue(csv.contains("00ff"), csv);

    List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);
    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0)[0]);
    assertArrayEquals(hash, (byte[]) rows.get(0)[1]);
    assertArrayEquals(nulHigh, (byte[]) rows.get(1)[1]);
    assertNull(rows.get(2)[1]);
  }
}
