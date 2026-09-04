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

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnowflakeBulkLoaderTest {

  private SnowflakeBulkLoader bulkLoaderSpy;
  private SnowflakeBulkLoaderMeta meta;
  private SnowflakeBulkLoaderData data;
  private Database db;
  private TransformMeta transformMeta;
  private PipelineMeta pipelineMeta;

  @BeforeEach
  void setUp() {
    meta = mock(SnowflakeBulkLoaderMeta.class);
    doReturn("TestConnection").when(meta).getConnection();
    doReturn("myschema").when(meta).getTargetSchema();
    doReturn("mytable").when(meta).getTargetTable();

    transformMeta = mock(TransformMeta.class);
    doReturn("transform").when(transformMeta).getName();
    doReturn(mock(TransformPartitioningMeta.class))
        .when(transformMeta)
        .getTargetTransformPartitioningMeta();
    doReturn(meta).when(transformMeta).getTransform();

    db = mock(Database.class);
    doReturn(mock(Connection.class)).when(db).getConnection();

    data = new SnowflakeBulkLoaderData();
    data.db = db;
    data.databaseMeta = mock(DatabaseMeta.class);

    pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(anyString());

    SnowflakeBulkLoader bulkLoader =
        new SnowflakeBulkLoader(
            transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine()));
    bulkLoaderSpy = spy(bulkLoader);
    doReturn(transformMeta).when(bulkLoaderSpy).getTransformMeta();
    doReturn(false).when(bulkLoaderSpy).isRowLevel();
    doReturn(false).when(bulkLoaderSpy).isDebug();
    doReturn("myschema").when(bulkLoaderSpy).resolve("myschema");
    doReturn("mytable").when(bulkLoaderSpy).resolve("mytable");
  }

  @Test
  void testTruncateTableOff() throws Exception {
    when(meta.isTruncateTable()).thenReturn(false);

    bulkLoaderSpy.truncateTable();

    verify(db, never()).truncateTable(anyString(), anyString());
  }

  @Test
  void testTruncateTableOnCopyZero() throws Exception {
    when(meta.isTruncateTable()).thenReturn(true);
    when(bulkLoaderSpy.getCopy()).thenReturn(0);

    bulkLoaderSpy.truncateTable();

    verify(db).truncateTable(nullable(String.class), nullable(String.class));
  }

  @Test
  void testTruncateTableOnWithPartitionId() throws Exception {
    when(meta.isTruncateTable()).thenReturn(true);
    when(bulkLoaderSpy.getCopy()).thenReturn(1);
    when(bulkLoaderSpy.getPartitionId()).thenReturn("partition id");

    bulkLoaderSpy.truncateTable();

    verify(db).truncateTable(nullable(String.class), nullable(String.class));
  }

  @Test
  void testTruncateTableSkippedOnNonZeroCopyWithoutPartition() throws Exception {
    when(meta.isTruncateTable()).thenReturn(true);
    when(bulkLoaderSpy.getCopy()).thenReturn(1);
    when(bulkLoaderSpy.getPartitionId()).thenReturn(null);

    bulkLoaderSpy.truncateTable();

    verify(db, never()).truncateTable(anyString(), anyString());
  }

  @Test
  void testProcessRowTruncatesIfNoRowsAvailable() throws Exception {
    when(meta.isTruncateTable()).thenReturn(true);
    when(meta.isOnlyWhenHaveRows()).thenReturn(false);
    doReturn(null).when(bulkLoaderSpy).getRow();

    boolean result = bulkLoaderSpy.processRow();

    assertFalse(result);
    verify(bulkLoaderSpy).truncateTable();
  }

  @Test
  void testProcessRowDoesNotTruncateIfNoRowsAndOnlyWhenHaveRows() throws Exception {
    when(meta.isTruncateTable()).thenReturn(true);
    when(meta.isOnlyWhenHaveRows()).thenReturn(true);
    doReturn(null).when(bulkLoaderSpy).getRow();

    boolean result = bulkLoaderSpy.processRow();

    assertFalse(result);
    verify(bulkLoaderSpy, never()).truncateTable();
  }

  @Test
  void testProcessRowDoesNotTruncateIfNoRowsAndTruncateOff() throws Exception {
    when(meta.isTruncateTable()).thenReturn(false);
    doReturn(null).when(bulkLoaderSpy).getRow();

    boolean result = bulkLoaderSpy.processRow();

    assertFalse(result);
    verify(bulkLoaderSpy, never()).truncateTable();
  }

  @Test
  void testProcessRowTruncatesOnFirstRow() throws Exception {
    when(meta.isTruncateTable()).thenReturn(true);
    Object[] row = new Object[] {};
    doReturn(row).when(bulkLoaderSpy).getRow();

    try {
      bulkLoaderSpy.processRow();
    } catch (Exception e) {
      // Not fully wired for a complete row write; truncate should still have been attempted.
    }

    verify(bulkLoaderSpy, times(1)).truncateTable();
  }

  @Test
  void testProcessRowDoesNotTruncateOnSubsequentRows() throws Exception {
    when(meta.isTruncateTable()).thenReturn(true);
    Object[] row = new Object[] {};
    doReturn(row).when(bulkLoaderSpy).getRow();
    bulkLoaderSpy.first = false;

    try {
      bulkLoaderSpy.processRow();
    } catch (Exception e) {
      // Not fully wired for a complete row write.
    }

    verify(bulkLoaderSpy, never()).truncateTable();
  }

  @Test
  void testDateIsWrittenInTheSnowflakeTimestampFormatWhenTheTableIsUnknown() {
    data.dbFields = null;
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("created"));

    IValueMeta[] writeValueMetas = bulkLoaderSpy.getWriteValueMetas(rowMeta);

    assertEquals(SnowflakeBulkLoaderMeta.TIMESTAMP_MASK, writeValueMetas[0].getConversionMask());
  }

  @Test
  void testDateIsWrittenInTheFormatOfTheTargetColumn() {
    data.dbFields = new ArrayList<>();
    data.dbFields.add(new String[] {"BIRTHDAY", "DATE"});
    data.dbFields.add(new String[] {"CREATED", "TIMESTAMP_NTZ(9)"});
    data.dbFields.add(new String[] {"STARTED", "TIME(9)"});

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("birthday"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("created"));
    rowMeta.addValueMeta(new ValueMetaDate("started"));

    IValueMeta[] writeValueMetas = bulkLoaderSpy.getWriteValueMetas(rowMeta);

    assertEquals(SnowflakeBulkLoaderMeta.DATE_MASK, writeValueMetas[0].getConversionMask());
    assertEquals(SnowflakeBulkLoaderMeta.TIMESTAMP_MASK, writeValueMetas[1].getConversionMask());
    assertEquals(SnowflakeBulkLoaderMeta.TIME_MASK, writeValueMetas[2].getConversionMask());
  }

  @Test
  void testFormatOfTheStreamIsKeptForNonDateColumns() {
    data.dbFields = new ArrayList<>();
    data.dbFields.add(new String[] {"CREATED", "VARCHAR(16777216)"});
    data.dbFields.add(new String[] {"NAME", "VARCHAR(16777216)"});

    IRowMeta rowMeta = new RowMeta();
    IValueMeta date = new ValueMetaDate("created");
    date.setConversionMask("dd/MM/yyyy");
    rowMeta.addValueMeta(date);
    IValueMeta name = new ValueMetaString("name");
    rowMeta.addValueMeta(name);

    IValueMeta[] writeValueMetas = bulkLoaderSpy.getWriteValueMetas(rowMeta);

    assertSame(date, writeValueMetas[0]);
    assertSame(name, writeValueMetas[1]);
  }

  @Test
  void testTheFormatOfTheStreamIsNotModified() {
    data.dbFields = null;
    IRowMeta rowMeta = new RowMeta();
    IValueMeta date = new ValueMetaDate("created");
    date.setConversionMask("dd/MM/yyyy");
    rowMeta.addValueMeta(date);

    bulkLoaderSpy.getWriteValueMetas(rowMeta);

    assertEquals("dd/MM/yyyy", date.getConversionMask());
  }

  @Test
  void testDatesAreWrittenToTheTempFileInTheSnowflakeFormat() throws Exception {
    data.dbFields = new ArrayList<>();
    data.dbFields.add(new String[] {"BIRTHDAY", "DATE"});
    data.dbFields.add(new String[] {"CREATED", "TIMESTAMP_NTZ(9)"});

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("birthday"));
    rowMeta.addValueMeta(new ValueMetaDate("created"));

    data.outputRowMeta = rowMeta;
    data.writeValueMetas = bulkLoaderSpy.getWriteValueMetas(rowMeta);
    data.binarySeparator = SnowflakeBulkLoaderMeta.CSV_DELIMITER.getBytes(StandardCharsets.UTF_8);
    data.binaryEnclosure = SnowflakeBulkLoaderMeta.ENCLOSURE.getBytes(StandardCharsets.UTF_8);
    data.escapeCharacters =
        SnowflakeBulkLoaderMeta.CSV_ESCAPE_CHAR.getBytes(StandardCharsets.UTF_8);
    data.binaryNewline =
        SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    data.writer = output;

    Date date = new GregorianCalendar(2023, Calendar.FEBRUARY, 9, 10, 11, 12).getTime();
    bulkLoaderSpy.writeRowToFile(rowMeta, new Object[] {date, date});

    assertEquals(
        "2023-02-09,2023-02-09 10:11:12.000" + SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER,
        output.toString(StandardCharsets.UTF_8));
  }

  @Test
  void writesBinaryValuesAsHex() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBinary("hash"));

    data.outputRowMeta = rowMeta;
    data.writeValueMetas = bulkLoaderSpy.getWriteValueMetas(rowMeta);
    data.binarySeparator = SnowflakeBulkLoaderMeta.CSV_DELIMITER.getBytes(StandardCharsets.UTF_8);
    data.binaryEnclosure = SnowflakeBulkLoaderMeta.ENCLOSURE.getBytes(StandardCharsets.UTF_8);
    data.escapeCharacters =
        SnowflakeBulkLoaderMeta.CSV_ESCAPE_CHAR.getBytes(StandardCharsets.UTF_8);
    data.binaryNewline =
        SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    data.writer = output;

    bulkLoaderSpy.writeRowToFile(
        rowMeta, new Object[] {new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}});

    assertEquals(
        "deadbeef" + SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER,
        output.toString(StandardCharsets.UTF_8));
  }
}
