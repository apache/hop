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

package org.apache.hop.pipeline.transforms.tableoutput;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableOutputTest {
  private DatabaseMeta databaseMeta;

  private TransformMeta transformMeta;

  private TableOutput tableOutput, tableOutputSpy;
  private TableOutputMeta tableOutputMeta;
  private TableOutputData tableOutputData;
  private Database db;
  private PipelineMeta pipelineMeta;

  @Before
  public void setUp() throws Exception {
    databaseMeta = mock( DatabaseMeta.class );
    doReturn( "" ).when( databaseMeta ).quoteField( anyString() );

    tableOutputMeta = mock( TableOutputMeta.class );
    doReturn( databaseMeta ).when( tableOutputMeta ).getDatabaseMeta();

    transformMeta = mock( TransformMeta.class );
    doReturn( "transform" ).when( transformMeta ).getName();
    doReturn( mock( TransformPartitioningMeta.class ) ).when( transformMeta ).getTargetTransformPartitioningMeta();
    doReturn( tableOutputMeta ).when( transformMeta ).getTransform();

    db = mock( Database.class );
    doReturn( mock( Connection.class ) ).when( db ).getConnection();

    tableOutputData = mock( TableOutputData.class );
    tableOutputData.db = db;
    tableOutputData.tableName = "sas";
    tableOutputData.preparedStatements = mock( Map.class );
    tableOutputData.commitCounterMap = mock( Map.class );

    pipelineMeta = mock( PipelineMeta.class );
    doReturn( transformMeta ).when( pipelineMeta ).findTransform( anyString() );

    setupTableOutputSpy();
  }

  private void setupTableOutputSpy() throws Exception {

    tableOutput = new TableOutput( transformMeta, tableOutputMeta, tableOutputData, 1, pipelineMeta, spy( new LocalPipelineEngine() ) );
    tableOutputSpy = spy( tableOutput );
    doReturn( transformMeta ).when( tableOutputSpy ).getTransformMeta();
    doReturn( false ).when( tableOutputSpy ).isRowLevel();
    doReturn( false ).when( tableOutputSpy ).isDebug();
    doNothing().when( tableOutputSpy ).logDetailed( anyString() );

  }


  @Test
  public void testWriteToTable() throws Exception {
    tableOutputSpy.writeToTable( mock( IRowMeta.class ), new Object[] {} );
  }

  @Test
  public void testTruncateTableOff() throws Exception {
    tableOutputSpy.truncateTable();
    verify( db, never() ).truncateTable( anyString(), anyString() );
  }

  @Test
  public void testTruncateTable_on() throws Exception {
    when( tableOutputMeta.truncateTable() ).thenReturn( true );
    when( tableOutputSpy.getCopy() ).thenReturn( 0 );

    tableOutputSpy.truncateTable();
    verify( db ).truncateTable( anyString(), anyString() );
  }

  @Test
  public void testTruncateTable_on_PartitionId() throws Exception {
    when( tableOutputMeta.truncateTable() ).thenReturn( true );
    when( tableOutputSpy.getCopy() ).thenReturn( 1 );
    when( tableOutputSpy.getPartitionId() ).thenReturn( "partition id" );

    tableOutputSpy.truncateTable();
    verify( db ).truncateTable( anyString(), anyString() );
  }

  @Test
  public void testProcessRow_truncatesIfNoRowsAvailable() throws Exception {
    when( tableOutputMeta.truncateTable() ).thenReturn( true );

    doReturn( null ).when( tableOutputSpy ).getRow();

    boolean result = tableOutputSpy.processRow();

    assertFalse( result );
    verify( tableOutputSpy ).truncateTable();
  }

  @Test
  public void testProcessRow_doesNotTruncateIfNoRowsAvailableAndTruncateIsOff() throws Exception {
    when( tableOutputMeta.truncateTable() ).thenReturn( false );

    doReturn( null ).when( tableOutputSpy ).getRow();

    boolean result = tableOutputSpy.processRow();

    assertFalse( result );
    verify( tableOutputSpy, never() ).truncateTable();
  }

  @Test
  public void testProcessRow_truncatesOnFirstRow() throws Exception {
    when( tableOutputMeta.truncateTable() ).thenReturn( true );
    Object[] row = new Object[] {};
    doReturn( row ).when( tableOutputSpy ).getRow();

    try {
      boolean result = tableOutputSpy.processRow();
    } catch ( NullPointerException npe ) {
      // not everything is set up to process an entire row, but we don't need that for this test
    }
    verify( tableOutputSpy, times( 1 ) ).truncateTable();
  }

  @Test
  public void testProcessRow_doesNotTruncateOnOtherRows() throws Exception {
    when( tableOutputMeta.truncateTable() ).thenReturn( true );
    Object[] row = new Object[] {};
    doReturn( row ).when( tableOutputSpy ).getRow();
    tableOutputSpy.first = false;
    doReturn( null ).when( tableOutputSpy ).writeToTable( any( IRowMeta.class ), any( row.getClass() ) );

    boolean result = tableOutputSpy.processRow();

    assertTrue( result );
    verify( tableOutputSpy, never() ).truncateTable();
  }

  @Test
  public void testInit_unsupportedConnection() {

    IDatabase dbInterface = mock( IDatabase.class );

    doNothing().when( tableOutputSpy ).logError( anyString() );

    when( tableOutputMeta.getCommitSize() ).thenReturn( "1" );
    when( tableOutputMeta.getDatabaseMeta() ).thenReturn( databaseMeta );
    when( databaseMeta.getIDatabase() ).thenReturn( dbInterface );

    String unsupportedTableOutputMessage = "unsupported exception";
    when( dbInterface.getUnsupportedTableOutputMessage() ).thenReturn( unsupportedTableOutputMessage );

    //Will cause the Hop Exception
    when( dbInterface.supportsStandardTableOutput() ).thenReturn( false );

    tableOutputSpy.init();

    HopException ke = new HopException( unsupportedTableOutputMessage );
    verify( tableOutputSpy, times( 1 ) ).logError( "An error occurred intialising this transform: " + ke.getMessage() );
  }
}
