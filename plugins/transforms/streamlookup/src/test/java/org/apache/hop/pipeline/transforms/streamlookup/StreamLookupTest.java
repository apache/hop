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

import junit.framework.Assert;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for StreamLookup transform
 *
 * @author Pavel Sakun
 * @see StreamLookup
 */
public class StreamLookupTest {
  private TransformMockHelper<StreamLookupMeta, StreamLookupData> smh;

  @Before
  public void setUp() {
    smh =
      new TransformMockHelper<>( "StreamLookup", StreamLookupMeta.class,
        StreamLookupData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      smh.iLogChannel );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  private void convertDataToBinary( Object[][] data ) {
    for ( int i = 0; i < data.length; i++ ) {
      for ( int j = 0; j < data[ i ].length; j++ ) {
        data[ i ][ j ] = ( (String) data[ i ][ j ] ).getBytes();
      }
    }
  }

  private IRowSet mockLookupRowSet(boolean binary ) {
    final int storageType = binary ? IValueMeta.STORAGE_TYPE_BINARY_STRING : IValueMeta.STORAGE_TYPE_NORMAL;
    Object[][] data = { { "Value1", "1" }, { "Value2", "2" } };

    if ( binary ) {
      convertDataToBinary( data );
    }

    IRowSet lookupRowSet =
      smh.getMockInputRowSet( data );
    doReturn( "Lookup" ).when( lookupRowSet ).getOriginTransformName();
    doReturn( "StreamLookup" ).when( lookupRowSet ).getDestinationTransformName();

    RowMeta lookupRowMeta = new RowMeta();
    ValueMetaString valueMeta = new ValueMetaString( "Value" );
    valueMeta.setStorageType( storageType );
    valueMeta.setStorageMetadata( new ValueMetaString() );
    lookupRowMeta.addValueMeta( valueMeta );
    ValueMetaString idMeta = new ValueMetaString( "Id" );
    idMeta.setStorageType( storageType );
    idMeta.setStorageMetadata( new ValueMetaString() );
    lookupRowMeta.addValueMeta( idMeta );

    doReturn( lookupRowMeta ).when( lookupRowSet ).getRowMeta();

    return lookupRowSet;
  }

  private IRowSet mockDataRowSet( boolean binary ) {
    final int storageType = binary ? IValueMeta.STORAGE_TYPE_BINARY_STRING : IValueMeta.STORAGE_TYPE_NORMAL;
    Object[][] data = { { "Name1", "1" }, { "Name2", "2" } };

    if ( binary ) {
      convertDataToBinary( data );
    }

    IRowSet dataRowSet = smh.getMockInputRowSet( data );

    RowMeta dataRowMeta = new RowMeta();
    ValueMetaString valueMeta = new ValueMetaString( "Name" );
    valueMeta.setStorageType( storageType );
    valueMeta.setStorageMetadata( new ValueMetaString() );
    dataRowMeta.addValueMeta( valueMeta );
    ValueMetaString idMeta = new ValueMetaString( "Id" );
    idMeta.setStorageType( storageType );
    idMeta.setStorageMetadata( new ValueMetaString() );
    dataRowMeta.addValueMeta( idMeta );

    doReturn( dataRowMeta ).when( dataRowSet ).getRowMeta();

    return dataRowSet;
  }

  private StreamLookupMeta mockProcessRowMeta( boolean memoryPreservationActive ) throws HopTransformException {
    StreamLookupMeta meta = smh.iTransformMeta;

    TransformMeta lookupTransformMeta = when( mock( TransformMeta.class ).getName() ).thenReturn( "Lookup" ).getMock();
    doReturn( lookupTransformMeta ).when( smh.pipelineMeta ).findTransform( "Lookup" );

    TransformIOMeta transformIOMeta = new TransformIOMeta( true, true, false, false, false, false );
    transformIOMeta.addStream( new Stream( IStream.StreamType.INFO, lookupTransformMeta, null, StreamIcon.INFO, null ) );

    doReturn( transformIOMeta ).when( meta ).getTransformIOMeta();
    doReturn( new String[] { "Id" } ).when( meta ).getKeylookup();
    doReturn( new String[] { "Id" } ).when( meta ).getKeystream();
    doReturn( new String[] { "Value" } ).when( meta ).getValue();
    doReturn( memoryPreservationActive ).when( meta ).isMemoryPreservationActive();
    doReturn( false ).when( meta ).isUsingSortedList();
    doReturn( false ).when( meta ).isUsingIntegerPair();
    doReturn( new int[] { -1 } ).when( meta ).getValueDefaultType();
    doReturn( new String[] { "" } ).when( meta ).getValueDefault();
    doReturn( new String[] { "Value" } ).when( meta ).getValueName();
    doReturn( new String[] { "Value" } ).when( meta ).getValue();
    doCallRealMethod().when( meta ).getFields( any( IRowMeta.class ), anyString(), any( IRowMeta[].class ), any( TransformMeta.class ),
      any( IVariables.class ), any( IHopMetadataProvider.class ) );

    return meta;
  }

  private void doTest( boolean memoryPreservationActive, boolean binaryLookupStream, boolean binaryDataStream ) throws HopException {
    StreamLookup transform = new StreamLookup( smh.transformMeta, smh.iTransformMeta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline );
    transform.init();
    transform.addRowSetToInputRowSets( mockLookupRowSet( binaryLookupStream ) );
    transform.addRowSetToInputRowSets( mockDataRowSet( binaryDataStream ) );
    transform.addRowSetToOutputRowSets( new QueueRowSet() );

    StreamLookupMeta meta = mockProcessRowMeta( memoryPreservationActive );
    StreamLookupData data = new StreamLookupData();
    data.readLookupValues = true;

    IRowSet outputRowSet = transform.getOutputRowSets().get( 0 );

    // Process rows and collect output
    int rowNumber = 0;
    String[] expectedOutput = { "Name", "", "Value" };
    while ( transform.processRow() ) {
      Object[] rowData = outputRowSet.getRow();
      if ( rowData != null ) {
        IRowMeta rowMeta = outputRowSet.getRowMeta();
        Assert.assertEquals( "Output row is of wrong size", 3, rowMeta.size() );
        rowNumber++;
        // Verify output
        for ( int valueIndex = 0; valueIndex < rowMeta.size(); valueIndex++ ) {
          String expectedValue = expectedOutput[ valueIndex ] + rowNumber;
          Object actualValue = rowMeta.getValueMeta( valueIndex ).convertToNormalStorageType( rowData[ valueIndex ] );
          Assert.assertEquals( "Unexpected value at row " + rowNumber + " position " + valueIndex, expectedValue,
            actualValue );
        }
      }
    }

    Assert.assertEquals( "Incorrect output row number", 2, rowNumber );
  }

  @Test
  @Ignore
  public void testWithNormalStreams() throws HopException {
    doTest( false, false, false );
  }

  @Test
  @Ignore
  public void testWithBinaryLookupStream() throws HopException {
    doTest( false, true, false );
  }

  @Test
  @Ignore
  public void testWithBinaryDateStream() throws HopException {
    doTest( false, false, true );
  }

  @Test
  @Ignore
  public void testWithBinaryStreams() throws HopException {
    doTest( false, false, true );
  }

  @Test
  public void testMemoryPreservationWithNormalStreams() throws HopException {
    doTest( true, false, false );
  }

  @Test
  public void testMemoryPreservationWithBinaryLookupStream() throws HopException {
    doTest( true, true, false );
  }

  @Test
  public void testMemoryPreservationWithBinaryDateStream() throws HopException {
    doTest( true, false, true );
  }

  @Test
  public void testMemoryPreservationWithBinaryStreams() throws HopException {
    doTest( true, false, true );
  }
}
