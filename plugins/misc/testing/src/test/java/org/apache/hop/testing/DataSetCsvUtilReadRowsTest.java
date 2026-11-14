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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Reading a data set one row at a time gives the rows that reading it whole gives. */
class DataSetCsvUtilReadRowsTest {

  @TempDir Path tempDir;

  private Variables variables;

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, tempDir.toString());
  }

  @Test
  void everyFieldTypeIsReadBackOverManyRows() throws Exception {
    DataSet dataSet = dataSet("types");
    Date first = new Date(1_700_000_000_000L);
    Date second = new Date(1_700_000_123_456L);
    Object[][] written = {
      {1L, "Alice", 3.14, new BigDecimal("12345.678"), first, true, new byte[] {1, 2, (byte) 255}},
      {2L, "Bob", -0.5, new BigDecimal("-1"), second, false, new byte[] {0}},
      {3L, null, null, null, null, null, null},
      {4L, "Carol, \"quoted\"", 1e-20, new BigDecimal("0.000001"), first, true, new byte[] {9}}
    };
    try (DataSetCsvWriter writer =
        new DataSetCsvWriter(variables, dataSet, dataSet.getSetRowMeta())) {
      for (Object[] row : written) {
        writer.writeRow(row);
      }
    }

    List<Object[]> streamed = new ArrayList<>();
    DataSetCsvUtil.readRows(variables, dataSet, streamed::add);
    List<Object[]> loaded = DataSetCsvUtil.getAllRows(variables, dataSet);

    assertEquals(written.length, streamed.size());
    assertEquals(written.length, loaded.size());
    for (int r = 0; r < written.length; r++) {
      for (int f = 0; f < written[r].length; f++) {
        Object expected = written[r][f];
        Object actual = streamed.get(r)[f];
        if (expected instanceof byte[] bytes) {
          assertArrayEquals(bytes, (byte[]) actual);
          assertArrayEquals(bytes, (byte[]) loaded.get(r)[f]);
        } else if (expected instanceof BigDecimal number) {
          assertEquals(0, number.compareTo((BigDecimal) actual), "row " + r + " field " + f);
          assertEquals(actual, loaded.get(r)[f]);
        } else {
          if (expected == null && f == 1) {
            // CSV has no null for text: an empty String field reads back as an empty string
            expected = "";
          }
          assertEquals(expected, actual, "row " + r + " field " + f);
          assertEquals(actual, loaded.get(r)[f]);
        }
      }
    }
  }

  @Test
  void readingStopsWhenTheHandlerSaysSo() throws Exception {
    DataSet dataSet = dataSet("stop");
    try (DataSetCsvWriter writer =
        new DataSetCsvWriter(variables, dataSet, dataSet.getSetRowMeta())) {
      for (long i = 1; i <= 10; i++) {
        writer.writeRow(new Object[] {i, "row " + i, null, null, null, null, null});
      }
    }

    List<Object[]> rows = new ArrayList<>();
    DataSetCsvUtil.readRows(
        variables,
        dataSet,
        row -> {
          rows.add(row);
          return rows.size() < 3;
        });

    assertEquals(3, rows.size());
    assertEquals(3L, rows.get(2)[0]);
  }

  @Test
  void aDataSetWithoutAFileHasNoRows() throws Exception {
    DataSet dataSet = dataSet("missing");

    List<Object[]> rows = new ArrayList<>();
    DataSetCsvUtil.readRows(variables, dataSet, rows::add);

    assertTrue(rows.isEmpty());
    assertTrue(DataSetCsvUtil.getAllRows(variables, dataSet).isEmpty());
  }

  @Test
  void aShortRecordLeavesTheMissingFieldsEmpty() throws Exception {
    DataSet dataSet = dataSet("short");
    Files.writeString(
        tempDir.resolve("short.csv"),
        "id,name,amount,total,created,active,payload\n7,Dave\n",
        StandardCharsets.UTF_8);

    List<Object[]> rows = DataSetCsvUtil.getAllRows(variables, dataSet);

    assertEquals(1, rows.size());
    assertEquals(7L, rows.get(0)[0]);
    assertEquals("Dave", rows.get(0)[1]);
    for (int f = 2; f < 7; f++) {
      assertNull(rows.get(0)[f]);
    }
  }

  private static DataSet dataSet(String name) {
    DataSet dataSet = new DataSet();
    dataSet.setName(name);
    dataSet.setBaseFilename(name + ".csv");
    dataSet.setFields(
        List.of(
            new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0"),
            new DataSetField("name", IValueMeta.TYPE_STRING, -1, -1, "", ""),
            new DataSetField("amount", IValueMeta.TYPE_NUMBER, -1, -1, "", ""),
            new DataSetField("total", IValueMeta.TYPE_BIGNUMBER, -1, -1, "", ""),
            new DataSetField("created", IValueMeta.TYPE_DATE, -1, -1, "", ""),
            new DataSetField("active", IValueMeta.TYPE_BOOLEAN, -1, -1, "", ""),
            new DataSetField("payload", IValueMeta.TYPE_BINARY, -1, -1, "", "")));
    return dataSet;
  }
}
