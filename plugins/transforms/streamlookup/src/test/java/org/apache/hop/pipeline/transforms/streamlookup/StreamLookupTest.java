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

package org.apache.hop.pipeline.transforms.streamlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.when;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test for StreamLookup transform
 *
 * @see StreamLookup
 */
class StreamLookupTest {
  private TransformMockHelper<StreamLookupMeta, StreamLookupData> smh;

  @BeforeEach
  void setUp() {
    smh = new TransformMockHelper<>("StreamLookup", StreamLookupMeta.class, StreamLookupData.class);
    when(smh.logChannelFactory.create(any(), nullable(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);
    when(smh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    smh.cleanUp();
  }

  private void convertDataToBinary(Object[][] data) {
    for (int i = 0; i < data.length; i++) {
      for (int j = 0; j < data[i].length; j++) {
        data[i][j] = ((String) data[i][j]).getBytes();
      }
    }
  }

  private IRowSet mockLookupRowSet(boolean binary) {
    final int storageType =
        binary ? IValueMeta.STORAGE_TYPE_BINARY_STRING : IValueMeta.STORAGE_TYPE_NORMAL;
    Object[][] data = {{"Value1", "1"}, {"Value2", "2"}};

    if (binary) {
      convertDataToBinary(data);
    }

    IRowSet lookupRowSet = smh.getMockInputRowSet(data);
    doReturn("Lookup").when(lookupRowSet).getOriginTransformName();
    doReturn("StreamLookup").when(lookupRowSet).getDestinationTransformName();

    RowMeta lookupRowMeta = new RowMeta();
    ValueMetaString valueMeta = new ValueMetaString("Value");
    valueMeta.setStorageType(storageType);
    valueMeta.setStorageMetadata(new ValueMetaString());
    lookupRowMeta.addValueMeta(valueMeta);
    ValueMetaString idMeta = new ValueMetaString("Id");
    idMeta.setStorageType(storageType);
    idMeta.setStorageMetadata(new ValueMetaString());
    lookupRowMeta.addValueMeta(idMeta);

    doReturn(lookupRowMeta).when(lookupRowSet).getRowMeta();

    return lookupRowSet;
  }

  private IRowSet mockDataRowSet(boolean binary) {
    final int storageType =
        binary ? IValueMeta.STORAGE_TYPE_BINARY_STRING : IValueMeta.STORAGE_TYPE_NORMAL;
    Object[][] data = {{"Name1", "1"}, {"Name2", "2"}};

    if (binary) {
      convertDataToBinary(data);
    }

    IRowSet dataRowSet = smh.getMockInputRowSet(data);

    RowMeta dataRowMeta = new RowMeta();
    ValueMetaString valueMeta = new ValueMetaString("Name");
    valueMeta.setStorageType(storageType);
    valueMeta.setStorageMetadata(new ValueMetaString());
    dataRowMeta.addValueMeta(valueMeta);
    ValueMetaString idMeta = new ValueMetaString("Id");
    idMeta.setStorageType(storageType);
    idMeta.setStorageMetadata(new ValueMetaString());
    dataRowMeta.addValueMeta(idMeta);

    doReturn(dataRowMeta).when(dataRowSet).getRowMeta();

    return dataRowSet;
  }

  private StreamLookupMeta mockProcessRowMeta(boolean memoryPreservationActive)
      throws HopTransformException {
    StreamLookupMeta meta = smh.iTransformMeta;

    TransformMeta lookupTransformMeta =
        when(mock(TransformMeta.class).getName()).thenReturn("Lookup").getMock();
    doReturn(lookupTransformMeta).when(smh.pipelineMeta).findTransform("Lookup");

    TransformIOMeta transformIOMeta = new TransformIOMeta(true, true, false, false, false, false);
    transformIOMeta.addStream(
        new Stream(IStream.StreamType.INFO, lookupTransformMeta, null, StreamIcon.INFO, null));

    doReturn(transformIOMeta).when(meta).getTransformIOMeta();
    doReturn(new String[] {"Id"}).when(meta).getKeylookup();
    doReturn(new String[] {"Id"}).when(meta).getKeystream();
    doReturn(new String[] {"Value"}).when(meta).getValue();
    doReturn(memoryPreservationActive).when(meta).isMemoryPreservationActive();
    doReturn(false).when(meta).isUsingSortedList();
    doReturn(false).when(meta).isUsingIntegerPair();
    doReturn(new int[] {-1}).when(meta).getValueDefaultType();
    doReturn(new String[] {""}).when(meta).getValueDefault();
    doReturn(new String[] {"Value"}).when(meta).getValueName();
    doReturn(new String[] {"Value"}).when(meta).getValue();
    doCallRealMethod()
        .when(meta)
        .getFields(
            nullable(IRowMeta.class),
            nullable(String.class),
            nullable(IRowMeta[].class),
            nullable(TransformMeta.class),
            nullable(IVariables.class),
            nullable(IHopMetadataProvider.class));

    return meta;
  }

  private void doTest(
      boolean memoryPreservationActive, boolean binaryLookupStream, boolean binaryDataStream)
      throws HopException {
    StreamLookup transform =
        new StreamLookup(
            smh.transformMeta,
            smh.iTransformMeta,
            smh.iTransformData,
            0,
            smh.pipelineMeta,
            smh.pipeline);
    transform.init();
    transform.addRowSetToInputRowSets(mockLookupRowSet(binaryLookupStream));
    transform.addRowSetToInputRowSets(mockDataRowSet(binaryDataStream));
    transform.addRowSetToOutputRowSets(new QueueRowSet());

    StreamLookupMeta meta = mockProcessRowMeta(memoryPreservationActive);
    StreamLookupData data = new StreamLookupData();
    data.readLookupValues = true;

    IRowSet outputRowSet = transform.getOutputRowSets().get(0);

    // Process rows and collect output
    int rowNumber = 0;
    String[] expectedOutput = {"Name", "", "Value"};
    while (transform.processRow()) {
      Object[] rowData = outputRowSet.getRow();
      if (rowData != null) {
        IRowMeta rowMeta = outputRowSet.getRowMeta();
        assertEquals(3, rowMeta.size(), "Output row is of wrong size");
        rowNumber++;
        // Verify output
        for (int valueIndex = 0; valueIndex < rowMeta.size(); valueIndex++) {
          String expectedValue = expectedOutput[valueIndex] + rowNumber;
          Object actualValue =
              rowMeta.getValueMeta(valueIndex).convertToNormalStorageType(rowData[valueIndex]);
          assertEquals(
              expectedValue,
              actualValue,
              "Unexpected value at row " + rowNumber + " position " + valueIndex);
        }
      }
    }

    assertEquals(2, rowNumber, "Incorrect output row number");
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testWithNormalStreams() throws HopException {
    doTest(false, false, false);
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testWithBinaryLookupStream() throws HopException {
    doTest(false, true, false);
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testWithBinaryDateStream() throws HopException {
    doTest(false, false, true);
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testWithBinaryStreams() throws HopException {
    doTest(false, false, true);
  }

  @Test
  void testMemoryPreservationWithNormalStreams() throws HopException {
    doTest(true, false, false);
  }

  @Test
  void testMemoryPreservationWithBinaryLookupStream() throws HopException {
    doTest(true, true, false);
  }

  @Test
  void testMemoryPreservationWithBinaryDateStream() throws HopException {
    doTest(true, false, true);
  }

  @Test
  void testMemoryPreservationWithBinaryStreams() throws HopException {
    doTest(true, false, true);
  }
}
