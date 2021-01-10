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

package org.apache.hop.pipeline.transforms.joinrows;

import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.*;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Denis Mashukov
 */
public class JoinRowsTest {

  private JoinRowsMeta meta;
  private JoinRowsData data;

  @Before
  public void setUp() throws Exception {
    meta = new JoinRowsMeta();
    data = new JoinRowsData();

    ILogChannelFactory logChannelFactory = mock( ILogChannelFactory.class );
    ILogChannel logChannelInterface = mock( ILogChannel.class );
    HopLogStore.setLogChannelFactory( logChannelFactory );
    when( logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
            logChannelInterface );
  }

  @After
  public void tearDown() {
    meta = null;
    data = null;
  }

  /**
   * BACKLOG-8520 Check that method call does't throw an error NullPointerException.
   */
  @Test
  @Ignore
  public void checkThatMethodPerformedWithoutError() throws Exception {
    getJoinRows().dispose();
  }

  @Test
  @Ignore
  public void disposeDataFiles() throws Exception {
    File mockFile1 = mock( File.class );
    File mockFile2 = mock( File.class );
    data.file = new File[] { null, mockFile1, mockFile2 };
    getJoinRows().dispose();
    verify( mockFile1, times( 1 ) ).delete();
    verify( mockFile2, times( 1 ) ).delete();
  }

  private JoinRows getJoinRows() throws Exception {
    TransformMeta transformMeta = new TransformMeta();
    PipelineMeta pipelineMeta = new PipelineMeta();
    Pipeline pipeline = new LocalPipelineEngine( pipelineMeta );

    pipelineMeta.clear();
    pipelineMeta.addTransform( transformMeta );
    pipelineMeta.setTransform( 0, transformMeta );
    transformMeta.setName( "test" );
    pipeline.setLogChannel( new LogChannel("junit"));
    pipeline.prepareExecution();
    pipeline.startThreads();

    return new JoinRows( transformMeta, null, null, 0, pipelineMeta, pipeline );
  }

  @Test
  @Ignore
  public void testJoinRowsTransform() throws Exception {
    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();
    joinRowsMeta.setMainTransformName( "main transform name" );
    joinRowsMeta.setPrefix( "out" );
    joinRowsMeta.setCacheSize( 3 );

    JoinRowsData joinRowsData = new JoinRowsData();

    JoinRows joinRows = getJoinRows();
//    joinRows.getPipeline().setRunning( true );

    joinRows.init();


    List<IRowSet> rowSets = new ArrayList<>();
    rowSets.add( getRowSetWithData( 3, "main --", true ) );
    rowSets.add( getRowSetWithData( 3, "secondary --", false ) );

    joinRows.setInputRowSets( rowSets );

    TransformRowsCollector rowTransformCollector = new TransformRowsCollector();

    joinRows.addRowListener( rowTransformCollector );
    joinRows.getLogChannel().setLogLevel( LogLevel.ROWLEVEL );
    HopLogStore.init();


    while ( true ) {
      if ( !joinRows.processRow()) {
        break;
      }
    }

    rowTransformCollector.getRowsWritten();

    //since we have data join of two row sets with size 3 then we must have 9 written rows
    assertEquals( 9, rowTransformCollector.getRowsWritten().size() );
    assertEquals( 6, rowTransformCollector.getRowsRead().size() );

    Object[][] expectedResult = createExpectedResult();

    List<Object[]> rowWritten = rowTransformCollector.getRowsWritten().stream().map( RowMetaAndData::getData ).collect( Collectors.toList() );

    for ( int i = 0; i < 9; i++ ) {
      assertTrue( Arrays.equals( expectedResult[ i ], rowWritten.get( i ) ) );
    }
  }


  BlockingRowSet getRowSetWithData( int size, String dataPrefix, boolean isMainTransform ) {
    BlockingRowSet blockingRowSet = new BlockingRowSet( size );
    RowMeta rowMeta = new RowMeta();

    IValueMeta valueMetaString = new ValueMetaString( dataPrefix + " first value name" );
    IValueMeta valueMetaInteger = new ValueMetaString( dataPrefix + " second value name" );
    IValueMeta valueMetaBoolean = new ValueMetaString( dataPrefix + " third value name" );

    rowMeta.addValueMeta( valueMetaString );
    rowMeta.addValueMeta( valueMetaInteger );
    rowMeta.addValueMeta( valueMetaBoolean );

    blockingRowSet.setRowMeta( rowMeta );

    for ( int i = 0; i < size; i++ ) {
      Object[] rowData = new Object[ 3 ];
      rowData[ 0 ] = dataPrefix + " row[" + i + "]-first value";
      rowData[ 1 ] = dataPrefix + " row[" + i + "]-second value";
      rowData[ 2 ] = dataPrefix + " row[" + i + "]-third value";
      blockingRowSet.putRow( rowMeta, rowData );
    }

    if ( isMainTransform ) {
      blockingRowSet.setThreadNameFromToCopy( "main transform name", 0, null, 0 );
    } else {
      blockingRowSet.setThreadNameFromToCopy( "secondary transform name", 0, null, 0 );
    }

    blockingRowSet.setDone();

    return blockingRowSet;
  }

  private Object[][] createExpectedResult() {
    Object[][] objects = { { "main -- row[0]-first value", "main -- row[0]-second value", "main -- row[0]-third value", "secondary -- row[0]-first value", "secondary -- row[0]-second value",
      "secondary -- row[0]-third value" },
      { "main -- row[0]-first value", "main -- row[0]-second value", "main -- row[0]-third value", "secondary -- row[1]-first value", "secondary -- row[1]-second value",
        "secondary -- row[1]-third value" },
      { "main -- row[0]-first value", "main -- row[0]-second value", "main -- row[0]-third value", "secondary -- row[2]-first value", "secondary -- row[2]-second value",
        "secondary -- row[2]-third value" },
      { "main -- row[1]-first value", "main -- row[1]-second value", "main -- row[1]-third value", "secondary -- row[0]-first value", "secondary -- row[0]-second value",
        "secondary -- row[0]-third value" },
      { "main -- row[1]-first value", "main -- row[1]-second value", "main -- row[1]-third value", "secondary -- row[1]-first value", "secondary -- row[1]-second value",
        "secondary -- row[1]-third value" },
      { "main -- row[1]-first value", "main -- row[1]-second value", "main -- row[1]-third value", "secondary -- row[2]-first value", "secondary -- row[2]-second value",
        "secondary -- row[2]-third value" },
      { "main -- row[2]-first value", "main -- row[2]-second value", "main -- row[2]-third value", "secondary -- row[0]-first value", "secondary -- row[0]-second value",
        "secondary -- row[0]-third value" },
      { "main -- row[2]-first value", "main -- row[2]-second value", "main -- row[2]-third value", "secondary -- row[1]-first value", "secondary -- row[1]-second value",
        "secondary -- row[1]-third value" },
      { "main -- row[2]-first value", "main -- row[2]-second value", "main -- row[2]-third value", "secondary -- row[2]-first value", "secondary -- row[2]-second value",
        "secondary -- row[2]-third value" } };
    return objects;
  }
}
