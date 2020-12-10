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

package org.apache.hop.pipeline.transforms.fuzzymatch;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 10/16/13 Time: 6:23 PM
 */
public class FuzzyMatchTest {
  @InjectMocks
  private FuzzyMatchHandler fuzzyMatch;
  private TransformMockHelper<FuzzyMatchMeta, FuzzyMatchData> mockHelper;

  private Object[] row = new Object[] { "Catrine" };
  private Object[] rowB = new Object[] { "Catrine".getBytes() };
  private Object[] row2 = new Object[] { "John" };
  private Object[] row2B = new Object[] { "John".getBytes() };
  private Object[] row3 = new Object[] { "Catriny" };
  private Object[] row3B = new Object[] { "Catriny".getBytes() };
  private List<Object[]> rows = new ArrayList<>();
  private List<Object[]> binaryRows = new ArrayList<>();
  private List<Object[]> lookupRows = new ArrayList<>();
  private List<Object[]> binaryLookupRows = new ArrayList<>();

  {
    rows.add( row );
    binaryRows.add( rowB );
    lookupRows.add( row2 );
    lookupRows.add( row3 );
    binaryLookupRows.add( row2B );
    binaryLookupRows.add( row3B );
  }

  private class FuzzyMatchHandler extends FuzzyMatch {
    private Object[] resultRow = null;
    private IRowSet rowset = null;

    public FuzzyMatchHandler( TransformMeta transformMeta, FuzzyMatchMeta meta, FuzzyMatchData data, int copyNr, PipelineMeta pipelineMeta,
                              Pipeline pipeline ) {
      super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    }

    @Override
    public void putRow( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      resultRow = row;
    }

    /**
     * Find input row set.
     *
     * @param sourceTransformName the source transform
     * @return the row set
     * @throws HopTransformException the hop transform exception
     */
    @Override
    public IRowSet findInputRowSet( String sourceTransformName ) throws HopTransformException {
      return rowset;
    }
  }

  @Before
  public void setUp() throws Exception {
    mockHelper =
      new TransformMockHelper<>( "Fuzzy Match", FuzzyMatchMeta.class, FuzzyMatchData.class );
    when( mockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      mockHelper.iLogChannel );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() throws Exception {
    mockHelper.cleanUp();
  }

  @SuppressWarnings( "unchecked" )
  @Test
  public void testProcessRow() throws Exception {
    fuzzyMatch =
      new FuzzyMatchHandler( mockHelper.transformMeta, mockHelper.iTransformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta,
        mockHelper.pipeline );
    fuzzyMatch.init();
    fuzzyMatch.addRowSetToInputRowSets( mockHelper.getMockInputRowSet( rows ) );
    fuzzyMatch.addRowSetToInputRowSets( mockHelper.getMockInputRowSet( lookupRows ) );

    when( mockHelper.iTransformMeta.getAlgorithmType() ).thenReturn( 8 );
    mockHelper.iTransformData.look = mock( HashSet.class );
    when( mockHelper.iTransformData.look.iterator() ).thenReturn( lookupRows.iterator() );

    fuzzyMatch.processRow();
    Assert.assertEquals( fuzzyMatch.resultRow[ 0 ], row3[ 0 ] );
  }

  @Test
  public void testReadLookupValues() throws Exception {
    FuzzyMatchData data = spy( new FuzzyMatchData() );
    data.indexOfCachedFields = new int[ 2 ];
    data.minimalDistance = 0;
    data.maximalDistance = 5;
    FuzzyMatchMeta meta = spy( new FuzzyMatchMeta() );
    meta.setOutputMatchField( "I don't want NPE here!" );
    data.readLookupValues = true;
    fuzzyMatch = new FuzzyMatchHandler( mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline );

    fuzzyMatch.init();
    IRowSet lookupRowSet = mockHelper.getMockInputRowSet( binaryLookupRows );
    fuzzyMatch.addRowSetToInputRowSets( mockHelper.getMockInputRowSet( binaryRows ) );
    fuzzyMatch.addRowSetToInputRowSets( lookupRowSet );
    fuzzyMatch.rowset = lookupRowSet;

    IRowMeta iRowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaString( "field1" );
    valueMeta.setStorageMetadata( new ValueMetaString( "field1" ) );
    valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    iRowMeta.addValueMeta( valueMeta );
    when( lookupRowSet.getRowMeta() ).thenReturn( iRowMeta );
    when( meta.getLookupField() ).thenReturn( "field1" );
    when( meta.getMainStreamField() ).thenReturn( "field1" );
    fuzzyMatch.setInputRowMeta( iRowMeta.clone() );

    when( meta.getAlgorithmType() ).thenReturn( 1 );
    ITransformIOMeta transformIOMetaInterface = mock( ITransformIOMeta.class );
    when( meta.getTransformIOMeta() ).thenReturn( transformIOMetaInterface );
    IStream streamInterface = mock( IStream.class );
    List<IStream> streamInterfaceList = new ArrayList<>();
    streamInterfaceList.add( streamInterface );
    when( streamInterface.getTransformMeta() ).thenReturn( mockHelper.transformMeta );

    when( transformIOMetaInterface.getInfoStreams() ).thenReturn( streamInterfaceList );

    fuzzyMatch.processRow();
    Assert.assertEquals( iRowMeta.getString( row3B, 0 ),
      data.outputRowMeta.getString( fuzzyMatch.resultRow, 1 ) );
  }

  @Test
  public void testLookupValuesWhenMainFieldIsNull() throws Exception {
    FuzzyMatchData data = spy( new FuzzyMatchData() );
    FuzzyMatchMeta meta = spy( new FuzzyMatchMeta() );
    data.readLookupValues = false;
    fuzzyMatch = new FuzzyMatchHandler( mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    fuzzyMatch.init();
    fuzzyMatch.first = false;
    data.indexOfMainField = 1;
    Object[] inputRow = { "test input", null };
    IRowSet lookupRowSet = mockHelper.getMockInputRowSet( new Object[] { "test lookup" } );
    fuzzyMatch.addRowSetToInputRowSets( mockHelper.getMockInputRowSet( inputRow ) );
    fuzzyMatch.addRowSetToInputRowSets( lookupRowSet );
    fuzzyMatch.rowset = lookupRowSet;

    IRowMeta iRowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaString( "field1" );
    valueMeta.setStorageMetadata( new ValueMetaString( "field1" ) );
    valueMeta.setStorageType( IValueMeta.TYPE_STRING );
    iRowMeta.addValueMeta( valueMeta );
    when( lookupRowSet.getRowMeta() ).thenReturn( iRowMeta );
    fuzzyMatch.setInputRowMeta( iRowMeta.clone() );
    data.outputRowMeta = iRowMeta.clone();

    fuzzyMatch.processRow();
    Assert.assertEquals( inputRow[ 0 ], fuzzyMatch.resultRow[ 0 ] );
    Assert.assertNull( fuzzyMatch.resultRow[ 1 ] );
    Assert.assertTrue( Arrays.stream( fuzzyMatch.resultRow, 3, fuzzyMatch.resultRow.length ).allMatch( val -> val == null ) );
  }
}
