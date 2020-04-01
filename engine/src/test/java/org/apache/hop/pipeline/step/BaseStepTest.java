/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.step;

import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.SingleRowRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.fileinput.NonAccessibleFileObject;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.BasePartitioner;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.steps.mock.StepMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class BaseStepTest {
  private StepMockHelper<StepMetaInterface, StepDataInterface> mockHelper;

  @Mock RowHandler rowHandler;

  @Before
  public void setup() {
    mockHelper =
      new StepMockHelper<>( "BASE STEP", StepMetaInterface.class,
        StepDataInterface.class );
    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      mockHelper.logChannelInterface );
  }

  @After
  public void tearDown() {
    mockHelper.cleanUp();
  }

  /**
   * This test checks that data from one non-partitioned step copies to 2 partitioned steps right.
   *
   * @throws HopException
   * @see {@link <a href="http://jira.pentaho.com/browse/PDI-12211">http://jira.pentaho.com/browse/PDI-12211<a>}
   */
  @Test
  public void testBaseStepPutRowLocalSpecialPartitioning() throws HopException {
    List<StepMeta> stepMetas = new ArrayList<StepMeta>();
    stepMetas.add( mockHelper.stepMeta );
    stepMetas.add( mockHelper.stepMeta );
    StepPartitioningMeta stepPartitioningMeta = spy( new StepPartitioningMeta() );
    BasePartitioner partitioner = mock( BasePartitioner.class );

    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenAnswer(
      new Answer<LogChannelInterface>() {

        @Override
        public LogChannelInterface answer( InvocationOnMock invocation ) throws Throwable {
          ( (BaseStep) invocation.getArguments()[ 0 ] ).getLogLevel();
          return mockHelper.logChannelInterface;
        }
      } );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
    when( mockHelper.pipelineMeta.findNextSteps( any( StepMeta.class ) ) ).thenReturn( stepMetas );
    when( mockHelper.stepMeta.getStepPartitioningMeta() ).thenReturn( stepPartitioningMeta );
    when( stepPartitioningMeta.getPartitioner() ).thenReturn( partitioner );
    when( partitioner.getNrPartitions() ).thenReturn( 2 );

    Object object0 = "name0";
    ValueMetaInterface meta0 = new ValueMetaString( object0.toString() );

    Object object1 = "name1";
    ValueMetaInterface meta2 = new ValueMetaString( object1.toString() );

    RowMetaInterface rowMeta0 = new RowMeta();
    rowMeta0.addValueMeta( meta0 );

    Object[] objects0 = { object0 };

    RowMetaInterface rowMeta1 = new RowMeta();
    rowMeta1.addValueMeta( meta2 );

    Object[] objects1 = { object1 };

    when( stepPartitioningMeta.getPartition( rowMeta0, objects0 ) ).thenReturn( 0 );
    when( stepPartitioningMeta.getPartition( rowMeta1, objects1 ) ).thenReturn( 1 );

    BlockingRowSet[] rowSet =
      { new BlockingRowSet( 2 ), new BlockingRowSet( 2 ), new BlockingRowSet( 2 ), new BlockingRowSet( 2 ) };
    List<RowSet> outputRowSets = new ArrayList<RowSet>();
    outputRowSets.addAll( Arrays.asList( rowSet ) );

    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setStopped( false );
    baseStep.setRepartitioning( StepPartitioningMeta.PARTITIONING_METHOD_SPECIAL );
    baseStep.setOutputRowSets( outputRowSets );
    baseStep.putRow( rowMeta0, objects0 );
    baseStep.putRow( rowMeta1, objects1 );

    assertEquals( object0, baseStep.getOutputRowSets().get( 0 ).getRow()[ 0 ] );
    assertEquals( object1, baseStep.getOutputRowSets().get( 1 ).getRow()[ 0 ] );
    assertEquals( object0, baseStep.getOutputRowSets().get( 2 ).getRow()[ 0 ] );
    assertEquals( object1, baseStep.getOutputRowSets().get( 3 ).getRow()[ 0 ] );
  }

  @Test
  public void testBaseStepGetLogLevelWontThrowNPEWithNullLog() {
    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenAnswer(
      new Answer<LogChannelInterface>() {

        @Override
        public LogChannelInterface answer( InvocationOnMock invocation ) throws Throwable {
          ( (BaseStep) invocation.getArguments()[ 0 ] ).getLogLevel();
          return mockHelper.logChannelInterface;
        }
      } );
    new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline )
      .getLogLevel();
  }

  @Test
  public void testStepListenersConcurrentModification() throws InterruptedException {
    // Create a base step
    final BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );

    // Create thread to dynamically add listeners
    final AtomicBoolean done = new AtomicBoolean( false );
    Thread addListeners = new Thread() {
      @Override
      public void run() {
        while ( !done.get() ) {
          baseStep.addStepListener( mock( StepListener.class ) );
          synchronized ( done ) {
            done.notify();
          }
        }
      }
    };

    // Mark start and stop while listeners are being added
    try {
      addListeners.start();

      // Allow a few listeners to be added
      synchronized ( done ) {
        while ( baseStep.getStepListeners().size() < 20 ) {
          done.wait();
        }
      }

      baseStep.markStart();

      // Allow more listeners to be added
      synchronized ( done ) {
        while ( baseStep.getStepListeners().size() < 100 ) {
          done.wait();
        }
      }

      baseStep.markStop();

    } finally {
      // Close addListeners thread
      done.set( true );
      addListeners.join();
    }
  }

  @Test
  public void resultFilesMapIsSafeForConcurrentModification() throws Exception {
    final BaseStep step =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );

    final AtomicBoolean complete = new AtomicBoolean( false );

    final int FILES_AMOUNT = 10 * 1000;
    Thread filesProducer = new Thread( new Runnable() {
      @Override public void run() {
        try {
          for ( int i = 0; i < FILES_AMOUNT; i++ ) {
            step.addResultFile( new ResultFile( 0, new NonAccessibleFileObject( Integer.toString( i ) ), null, null ) );
            try {
              Thread.sleep( 1 );
            } catch ( Exception e ) {
              fail( e.getMessage() );
            }
          }
        } finally {
          complete.set( true );
        }
      }
    } );

    filesProducer.start();
    try {
      while ( !complete.get() ) {
        for ( Map.Entry<String, ResultFile> entry : step.getResultFiles().entrySet() ) {
          entry.getKey();
        }
      }
    } finally {
      filesProducer.join();
    }
  }

  @Test
  public void outputRowMetasAreNotSharedAmongSeveralStreams() throws Exception {
    RowSet rs1 = new SingleRowRowSet();
    RowSet rs2 = new SingleRowRowSet();

    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setStopped( false );
    baseStep.setRepartitioning( StepPartitioningMeta.PARTITIONING_METHOD_NONE );
    baseStep.setOutputRowSets( Arrays.asList( rs1, rs2 ) );

    for ( RowSet rowSet : baseStep.getOutputRowSets() ) {
      assertNull( "RowMeta should be null, since no calls were done", rowSet.getRowMeta() );
    }

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "integer" ) );

    baseStep.putRow( rowMeta, new Object[] { "a", 1 } );

    RowMetaInterface meta1 = rs1.getRowMeta();
    RowMetaInterface meta2 = rs2.getRowMeta();
    assertNotNull( meta1 );
    assertNotNull( meta2 );
    // content is same
    for ( ValueMetaInterface meta : meta1.getValueMetaList() ) {
      assertTrue( meta.getName(), meta2.exists( meta ) );
    }
    // whereas instances differ
    assertFalse( meta1 == meta2 );
  }

  @Test
  public void testBuildLog() throws HopValueException {
    BaseStep testObject = new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta,
      mockHelper.pipeline );
    Date startDate = new Date( (long) 123 );
    Date endDate = new Date( (long) 125 );
    RowMetaAndData result = testObject.buildLog( "myStepName", 13, 123, 234, 345, 456, 567, startDate, endDate );

    assertNotNull( result );
    assertEquals( 9, result.size() );
    assertEquals( ValueMetaInterface.TYPE_STRING, result.getValueMeta( 0 ).getType() );
    assertEquals( "myStepName", result.getString( 0, "default" ) );
    assertEquals( ValueMetaInterface.TYPE_NUMBER, result.getValueMeta( 1 ).getType() );
    assertEquals( new Double( 13.0 ), Double.valueOf( result.getNumber( 1, 0.1 ) ) );
    assertEquals( ValueMetaInterface.TYPE_NUMBER, result.getValueMeta( 2 ).getType() );
    assertEquals( new Double( 123 ), Double.valueOf( result.getNumber( 2, 0.1 ) ) );
    assertEquals( ValueMetaInterface.TYPE_NUMBER, result.getValueMeta( 3 ).getType() );
    assertEquals( new Double( 234 ), Double.valueOf( result.getNumber( 3, 0.1 ) ) );
    assertEquals( ValueMetaInterface.TYPE_NUMBER, result.getValueMeta( 4 ).getType() );
    assertEquals( new Double( 345 ), Double.valueOf( result.getNumber( 4, 0.1 ) ) );
    assertEquals( ValueMetaInterface.TYPE_NUMBER, result.getValueMeta( 5 ).getType() );
    assertEquals( new Double( 456 ), Double.valueOf( result.getNumber( 5, 0.1 ) ) );
    assertEquals( ValueMetaInterface.TYPE_NUMBER, result.getValueMeta( 6 ).getType() );
    assertEquals( new Double( 567 ), Double.valueOf( result.getNumber( 6, 0.1 ) ) );
    assertEquals( ValueMetaInterface.TYPE_DATE, result.getValueMeta( 7 ).getType() );
    assertEquals( startDate, result.getDate( 7, Calendar.getInstance().getTime() ) );
    assertEquals( ValueMetaInterface.TYPE_DATE, result.getValueMeta( 8 ).getType() );
    assertEquals( endDate, result.getDate( 8, Calendar.getInstance().getTime() ) );
  }

  @Test
  public void getRowWithRowHandler() throws HopException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface,
        0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setRowHandler( rowHandler );
    baseStep.getRow();
    verify( rowHandler, times( 1 ) ).getRow();
  }

  @Test
  public void putRowWithRowHandler() throws HopException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface,
        0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setRowHandler( rowHandler );

    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    Object[] objects = new Object[] { "foo", "bar" };
    baseStep.putRow( rowMetaInterface, objects );
    verify( rowHandler, times( 1 ) ).putRow( rowMetaInterface, objects );
  }

  @Test
  public void putErrorWithRowHandler() throws HopException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface,
        0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setRowHandler( rowHandler );
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    Object[] objects = new Object[] { "foo", "bar" };
    baseStep.putError( rowMetaInterface, objects, 3l, "desc",
      "field1,field2", "errorCode" );
    verify( rowHandler, times( 1 ) ).putError(
      rowMetaInterface, objects, 3l, "desc",
      "field1,field2", "errorCode" );
  }

  @Test
  public void putGetFromPutToDefaultRowHandlerMethods() throws HopException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface,
        0, mockHelper.pipelineMeta, mockHelper.pipeline );

    baseStep.setRowHandler( rowHandlerWithDefaultMethods() );

    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    Object[] objects = new Object[] { "foo", "bar" };

    try {
      baseStep.putRowTo( rowMetaInterface, objects, new QueueRowSet() );
      fail( "Expected default exception for putRowTo" );
    } catch ( UnsupportedOperationException uoe ) {
      assertTrue( uoe.getMessage().contains( this.getClass().getName() ) );
    }
    try {
      baseStep.getRowFrom( new QueueRowSet() );
      fail( "Expected default exception for getRowFrom" );
    } catch ( UnsupportedOperationException uoe ) {
      assertTrue( uoe.getMessage().contains( this.getClass().getName() ) );
    }
  }


  private RowHandler rowHandlerWithDefaultMethods() {
    return new RowHandler() {
      @Override public Object[] getRow() throws HopException {
        return new Object[ 0 ];
      }

      @Override public void putRow( RowMetaInterface rowMeta, Object[] row ) throws HopStepException {

      }

      @Override public void putError( RowMetaInterface rowMeta, Object[] row, long nrErrors, String errorDescriptions,
                                      String fieldNames, String errorCodes ) throws HopStepException {

      }
    };
  }

  @Test
  public void notEmptyFieldName() throws HopStepException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setRowHandler( rowHandler );

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaBase( "name", ValueMetaInterface.TYPE_INTEGER ) );

    baseStep.putRow( rowMeta, new Object[] {
      0 } );
  }

  @Test( expected = HopStepException.class )
  public void nullFieldName() throws HopStepException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setRowHandler( rowHandler );

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaBase( null, ValueMetaInterface.TYPE_INTEGER ) );

    baseStep.putRow( rowMeta, new Object[] {
      0 } );
  }

  @Test( expected = HopStepException.class )
  public void emptyFieldName() throws HopStepException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setRowHandler( rowHandler );

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaBase( "", ValueMetaInterface.TYPE_INTEGER ) );

    baseStep.putRow( rowMeta, new Object[] {
      0 } );
  }

  @Test( expected = HopStepException.class )
  public void blankFieldName() throws HopStepException {
    BaseStep baseStep =
      new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    baseStep.setRowHandler( rowHandler );

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaBase( "  ", ValueMetaInterface.TYPE_INTEGER ) );

    baseStep.putRow( rowMeta, new Object[] {
      0 } );
  }

  @Test
  public void testGetRowSafeModeEnabled() throws HopException {
    Pipeline pipelineMock = mock( Pipeline.class );
    when( pipelineMock.isSafeModeEnabled() ).thenReturn( true );
    BaseStep baseStepSpy =
      spy( new BaseStep( mockHelper.stepMeta, mockHelper.stepDataInterface,
        0, mockHelper.pipelineMeta, pipelineMock ) );
    doNothing().when( baseStepSpy ).waitUntilPipelineIsStarted();

    BlockingRowSet rowSet = new BlockingRowSet( 1 );
    List<ValueMetaInterface> valueMetaList = Arrays.asList( new ValueMetaInteger( "x" ), new ValueMetaString( "a" ) );
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList( valueMetaList );
    final Object[] row = new Object[] {};
    rowSet.putRow( rowMeta, row );

    baseStepSpy.setInputRowSets( Arrays.asList( rowSet ) );
    doReturn( rowSet ).when( baseStepSpy ).currentInputStream();

    baseStepSpy.getRow();
    verify( mockHelper.pipelineMeta, times( 1 ) ).checkRowMixingStatically( any( StepMeta.class ), anyObject() );
  }
}
