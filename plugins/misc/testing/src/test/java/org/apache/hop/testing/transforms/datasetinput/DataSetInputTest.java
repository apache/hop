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
package org.apache.hop.testing.transforms.datasetinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetCsvWriter;
import org.apache.hop.testing.DataSetField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataSetInputTest {

  @TempDir Path tempDir;

  private TransformMockHelper<DataSetInputMeta, DataSetInputData> mockHelper;
  private MemoryMetadataProvider provider;

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("Data set input", DataSetInputMeta.class, DataSetInputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getCopies(any())).thenReturn(1);
    when(mockHelper.transformMeta.isPartitioned()).thenReturn(false);
    provider = new MemoryMetadataProvider();
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void everyRowIsWrittenInFileOrder() throws Exception {
    DataSet dataSet = save("orders");
    write(dataSet, 5);

    DataSetInput transform = createTransform("orders", false);
    BlockingRowSet output = output(transform);
    assertTrue(transform.init());
    assertFalse(transform.processRow());

    for (long i = 1; i <= 5; i++) {
      Object[] row = output.getRowWait(1, TimeUnit.SECONDS);
      assertEquals(i, row[0]);
      assertEquals("row " + i, row[1]);
    }
    assertNull(output.getRowWait(100, TimeUnit.MILLISECONDS));
    assertTrue(output.isDone());
  }

  @Test
  void aDataSetWithoutAFileGivesNoRows() throws Exception {
    save("empty");

    DataSetInput transform = createTransform("empty", false);
    BlockingRowSet output = output(transform);
    assertTrue(transform.init());
    assertFalse(transform.processRow());

    assertNull(output.getRowWait(100, TimeUnit.MILLISECONDS));
    assertTrue(output.isDone());
  }

  /** The rows are written while the file is read, so a stop ends the reading too. */
  @Test
  void readingStopsWhenThePipelineIsStopped() throws Exception {
    DataSet dataSet = save("stopped");
    write(dataSet, 50);

    DataSetInput transform = createTransform("stopped", true);
    BlockingRowSet output = output(transform);
    assertTrue(transform.init());
    assertFalse(transform.processRow());

    int rows = 0;
    while (output.getRowWait(100, TimeUnit.MILLISECONDS) != null) {
      rows++;
    }
    assertEquals(3, rows);
  }

  private DataSet save(String name) throws HopException {
    DataSet dataSet = new DataSet();
    dataSet.setName(name);
    dataSet.setFolderName(tempDir.toString());
    dataSet.setBaseFilename(name + ".csv");
    dataSet.setFields(
        List.of(
            new DataSetField("id", IValueMeta.TYPE_INTEGER, -1, 0, "", "0"),
            new DataSetField("name", IValueMeta.TYPE_STRING, -1, -1, "", "")));
    provider.getSerializer(DataSet.class).save(dataSet);
    return dataSet;
  }

  private void write(DataSet dataSet, int rows) throws Exception {
    try (DataSetCsvWriter writer =
        new DataSetCsvWriter(mockHelper.pipeline, dataSet, dataSet.getSetRowMeta())) {
      for (long i = 1; i <= rows; i++) {
        writer.writeRow(new Object[] {i, "row " + i});
      }
    }
  }

  private static BlockingRowSet output(DataSetInput transform) {
    BlockingRowSet rowSet = new BlockingRowSet(100);
    rowSet.setThreadNameFromToCopy("Data set input", 0, "next", 0);
    transform.addRowSetToOutputRowSets(rowSet);
    return rowSet;
  }

  /**
   * @param stopAfterThreeRows stop the transform once it has written its third row
   */
  private DataSetInput createTransform(String dataSetName, boolean stopAfterThreeRows) {
    DataSetInputMeta meta = new DataSetInputMeta();
    meta.setDataSetName(dataSetName);
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    DataSetInput transform =
        new DataSetInput(
            mockHelper.transformMeta,
            meta,
            new DataSetInputData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline) {
          private int written;

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            super.putRow(rowMeta, row);
            if (stopAfterThreeRows && ++written == 3) {
              setStopped(true);
            }
          }
        };
    transform.setMetadataProvider(provider);
    return transform;
  }
}
