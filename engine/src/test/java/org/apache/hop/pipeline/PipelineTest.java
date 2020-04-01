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

package org.apache.hop.pipeline;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.StepLogTable;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaDataCombi;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.ImmutableList.of;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PipelineTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  @Mock private StepInterface stepMock, stepMock2;
  @Mock private StepDataInterface data, data2;
  @Mock private StepMeta stepMeta, stepMeta2;
  @Mock private PipelineMeta pipelineMeta;


  int count = 10000;
  Pipeline pipeline;
  PipelineMeta meta;


  @BeforeClass
  public static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void beforeTest() throws HopException {
    meta = new PipelineMeta();
    pipeline = new Pipeline( meta );
    pipeline.setLog( Mockito.mock( LogChannelInterface.class ) );
    pipeline.prepareExecution();
    pipeline.startThreads();
  }

  /**
   * PDI-14948 - Execution of pipeline with no steps never ends
   */
  @Test( timeout = 1000 )
  public void pipelineWithNoStepsIsNotEndless() throws Exception {
    Pipeline pipelineWithNoSteps = new Pipeline( new PipelineMeta() );
    pipelineWithNoSteps = spy( pipelineWithNoSteps );

    pipelineWithNoSteps.prepareExecution();

    pipelineWithNoSteps.startThreads();

    // check pipeline lifecycle is not corrupted
    verify( pipelineWithNoSteps ).firePipelineStartedListeners();
    verify( pipelineWithNoSteps ).firePipelineFinishedListeners();
  }

  /**
   * PDI-10762 - Pipeline and PipelineMeta leak
   */
  @Test
  public void testLoggingObjectIsNotLeakInMeta() {
    String expected = meta.log.getLogChannelId();
    meta.clear();
    String actual = meta.log.getLogChannelId();
    assertEquals( "Use same logChannel for empty constructors, or assign General level for clear() calls",
      expected, actual );
  }

  /**
   * PDI-5229 - ConcurrentModificationException when restarting pipeline Test that listeners can be accessed
   * concurrently during pipeline finish
   *
   * @throws HopException
   * @throws InterruptedException
   */
  @Test
  public void testPipelineFinishListenersConcurrentModification() throws HopException, InterruptedException {
    CountDownLatch start = new CountDownLatch( 1 );
    PipelineFinishListenerAdder add = new PipelineFinishListenerAdder( pipeline, start );
    PipelineFinishListenerFirer firer = new PipelineFinishListenerFirer( pipeline, start );
    startThreads( add, firer, start );
    assertEquals( "All listeners are added: no ConcurrentModificationException", count, add.c );
    assertEquals( "All Finish listeners are iterated over: no ConcurrentModificationException", count, firer.c );
  }

  /**
   * Test that listeners can be accessed concurrently during pipeline start
   *
   * @throws InterruptedException
   */
  @Test
  public void testPipelineStartListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch( 1 );
    PipelineFinishListenerAdder add = new PipelineFinishListenerAdder( pipeline, start );
    PipelineStartListenerFirer starter = new PipelineStartListenerFirer( pipeline, start );
    startThreads( add, starter, start );
    assertEquals( "All listeners are added: no ConcurrentModificationException", count, add.c );
    assertEquals( "All Start listeners are iterated over: no ConcurrentModificationException", count, starter.c );
  }

  /**
   * Test that pipeline stop listeners can be accessed concurrently
   *
   * @throws InterruptedException
   */
  @Test
  public void testPipelineStoppedListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch( 1 );
    PipelineStoppedCaller stopper = new PipelineStoppedCaller( pipeline, start );
    PipelineStopListenerAdder adder = new PipelineStopListenerAdder( pipeline, start );
    startThreads( stopper, adder, start );
    assertEquals( "All pipeline stop listeners is added", count, adder.c );
    assertEquals( "All stop call success", count, stopper.c );
  }

  @Test
  public void testPDI12424ParametersFromMetaAreCopiedToPipeline() throws HopException, URISyntaxException, IOException {
    String testParam = "testParam";
    String testParamValue = "testParamValue";
    PipelineMeta mockPipelineMeta = mock( PipelineMeta.class );
    when( mockPipelineMeta.listVariables() ).thenReturn( new String[] {} );
    when( mockPipelineMeta.listParameters() ).thenReturn( new String[] { testParam } );
    when( mockPipelineMeta.getParameterValue( testParam ) ).thenReturn( testParamValue );
    FileObject ktr = HopVFS.createTempFile( "parameters", ".hpl", "ram://" );
    try ( OutputStream outputStream = ktr.getContent().getOutputStream( true ) ) {
      InputStream inputStream = new ByteArrayInputStream( "<pipeline></pipeline>".getBytes() );
      IOUtils.copy( inputStream, outputStream );
    }
    Pipeline pipeline = new Pipeline( mockPipelineMeta, null, ktr.getURL().toURI().toString(), new MemoryMetaStore() );
    assertEquals( testParamValue, pipeline.getParameterValue( testParam ) );
  }

  @Test
  public void testRecordsCleanUpMethodIsCalled() throws Exception {
    Database mockedDataBase = mock( Database.class );
    Pipeline pipeline = mock( Pipeline.class );

    StepLogTable stepLogTable =
      StepLogTable.getDefault( mock( VariableSpace.class ), mock( IMetaStore.class ) );
    stepLogTable.setConnectionName( "connection" );

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setStepLogTable( stepLogTable );

    when( pipeline.getPipelineMeta() ).thenReturn( pipelineMeta );
    when( pipeline.createDataBase( any( DatabaseMeta.class ) ) ).thenReturn( mockedDataBase );
    when( pipeline.getSteps() ).thenReturn( new ArrayList<>() );

    doCallRealMethod().when( pipeline ).writeStepLogInformation();
    pipeline.writeStepLogInformation();

    verify( mockedDataBase ).cleanupLogRecords( stepLogTable );
  }

  @Test
  public void testFirePipelineFinishedListeners() throws Exception {
    Pipeline pipeline = new Pipeline();
    ExecutionListener mockListener = mock( ExecutionListener.class );
    pipeline.setExecutionListeners( Collections.singletonList( mockListener ) );

    pipeline.firePipelineFinishedListeners();

    verify( mockListener ).finished( pipeline );
  }

  @Test( expected = HopException.class )
  public void testFirePipelineFinishedListenersExceptionOnPipelineFinished() throws Exception {
    Pipeline pipeline = new Pipeline();
    ExecutionListener mockListener = mock( ExecutionListener.class );
    doThrow( HopException.class ).when( mockListener ).finished( pipeline );
    pipeline.setExecutionListeners( Collections.singletonList( mockListener ) );

    pipeline.firePipelineFinishedListeners();
  }

  @Test
  public void testFinishStatus() throws Exception {
    while ( pipeline.isRunning() ) {
      Thread.sleep( 1 );
    }
    assertEquals( Pipeline.STRING_FINISHED, pipeline.getStatus() );
  }

  @Test
  public void testSafeStop() {
    when( stepMock.isSafeStopped() ).thenReturn( false );
    when( stepMock.getStepname() ).thenReturn( "stepName" );

    pipeline.setSteps( of( combi( stepMock, data, stepMeta ) ) );
    Result result = pipeline.getResult();
    assertFalse( result.isSafeStop() );

    when( stepMock.isSafeStopped() ).thenReturn( true );
    result = pipeline.getResult();
    assertTrue( result.isSafeStop() );
  }

  @Test
  public void safeStopStopsInputStepsRightAway() throws HopException {
    pipeline.setSteps( of( combi( stepMock, data, stepMeta ) ) );
    when( pipelineMeta.findPreviousSteps( stepMeta, true ) ).thenReturn( emptyList() );
    pipeline.pipelineMeta = pipelineMeta;
    pipeline.safeStop();
    verifyStopped( stepMock, 1 );
  }

  @Test
  public void safeLetsNonInputStepsKeepRunning() throws HopException {
    pipeline.setSteps( of(
      combi( stepMock, data, stepMeta ),
      combi( stepMock2, data2, stepMeta2 ) ) );

    when( pipelineMeta.findPreviousSteps( stepMeta, true ) ).thenReturn( emptyList() );
    // stepMeta2 will have stepMeta as previous, so is not an input step
    when( pipelineMeta.findPreviousSteps( stepMeta2, true ) ).thenReturn( of( stepMeta ) );
    pipeline.pipelineMeta = pipelineMeta;

    pipeline.safeStop();
    verifyStopped( stepMock, 1 );
    // non input step shouldn't have stop called
    verifyStopped( stepMock2, 0 );
  }

  private void verifyStopped( StepInterface step, int numberTimesCalled ) throws HopException {
    verify( step, times( numberTimesCalled ) ).setStopped( true );
    verify( step, times( numberTimesCalled ) ).setSafeStopped( true );
    verify( step, times( numberTimesCalled ) ).resumeRunning();
    verify( step, times( numberTimesCalled ) ).stopRunning( any(), any() );
  }

  private StepMetaDataCombi combi( StepInterface step, StepDataInterface data, StepMeta stepMeta ) {
    StepMetaDataCombi stepMetaDataCombi = new StepMetaDataCombi();
    stepMetaDataCombi.step = step;
    stepMetaDataCombi.data = data;
    stepMetaDataCombi.stepMeta = stepMeta;
    return stepMetaDataCombi;
  }

  private void startThreads( Runnable one, Runnable two, CountDownLatch start ) throws InterruptedException {
    Thread th = new Thread( one );
    Thread tt = new Thread( two );
    th.start();
    tt.start();
    start.countDown();
    th.join();
    tt.join();
  }

  private abstract class PipelineKicker implements Runnable {
    protected Pipeline tr;
    protected int c = 0;
    protected CountDownLatch start;
    protected int max = count;

    PipelineKicker( Pipeline tr, CountDownLatch start ) {
      this.tr = tr;
      this.start = start;
    }

    protected boolean isStopped() {
      c++;
      return c >= max;
    }
  }

  private class PipelineStoppedCaller extends PipelineKicker {
    PipelineStoppedCaller( Pipeline tr, CountDownLatch start ) {
      super( tr, start );
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch ( InterruptedException e ) {
        throw new RuntimeException();
      }
      while ( !isStopped() ) {
        pipeline.stopAll();
      }
    }
  }

  private class PipelineStopListenerAdder extends PipelineKicker {
    PipelineStopListenerAdder( Pipeline tr, CountDownLatch start ) {
      super( tr, start );
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch ( InterruptedException e ) {
        throw new RuntimeException();
      }
      while ( !isStopped() ) {
        pipeline.addPipelineStoppedListener( pipelineStoppedListener );
      }
    }
  }

  private class PipelineFinishListenerAdder extends PipelineKicker {
    PipelineFinishListenerAdder( Pipeline tr, CountDownLatch start ) {
      super( tr, start );
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch ( InterruptedException e ) {
        throw new RuntimeException();
      }
      // run
      while ( !isStopped() ) {
        tr.addPipelineListener( listener );
      }
    }
  }

  private class PipelineFinishListenerFirer extends PipelineKicker {
    PipelineFinishListenerFirer( Pipeline tr, CountDownLatch start ) {
      super( tr, start );
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch ( InterruptedException e ) {
        throw new RuntimeException();
      }
      // run
      while ( !isStopped() ) {
        try {
          tr.firePipelineFinishedListeners();
          // clean array blocking queue
          tr.waitUntilFinished();
        } catch ( HopException e ) {
          throw new RuntimeException();
        }
      }
    }
  }

  private class PipelineStartListenerFirer extends PipelineKicker {
    PipelineStartListenerFirer( Pipeline tr, CountDownLatch start ) {
      super( tr, start );
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch ( InterruptedException e ) {
        throw new RuntimeException();
      }
      // run
      while ( !isStopped() ) {
        try {
          tr.firePipelineStartedListeners();
        } catch ( HopException e ) {
          throw new RuntimeException();
        }
      }
    }
  }

  private final ExecutionListener<PipelineMeta> listener = new ExecutionListener<PipelineMeta>() {
    @Override
    public void started( IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
    }

    @Override
    public void becameActive( IPipelineEngine<PipelineMeta> pipeline ) {
    }

    @Override
    public void finished( IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
    }
  };

  private final PipelineStoppedListener pipelineStoppedListener = new PipelineStoppedListener() {
    @Override
    public void pipelineStopped( Pipeline pipeline ) {
    }

  };

  @Test
  public void testNewPipelineWithContainerObjectId() throws Exception {
    PipelineMeta meta = mock( PipelineMeta.class );
    doReturn( new String[] { "X", "Y", "Z" } ).when( meta ).listVariables();
    doReturn( new String[] { "A", "B", "C" } ).when( meta ).listParameters();
    doReturn( "XYZ" ).when( meta ).getVariable( anyString() );
    doReturn( "" ).when( meta ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta ).getParameterDefault( anyString() );
    doReturn( "ABC" ).when( meta ).getParameterValue( anyString() );

    String carteId = UUID.randomUUID().toString();
    doReturn( carteId ).when( meta ).getContainerObjectId();

    Pipeline pipeline = new Pipeline( meta );

    assertEquals( carteId, pipeline.getContainerObjectId() );
  }

  /**
   * This test demonstrates the issue fixed in PDI-17436.
   * When a job is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  public void testTwoPipelineGetSameLogChannelId() throws Exception {
    PipelineMeta meta = mock( PipelineMeta.class );
    doReturn( new String[] { "X", "Y", "Z" } ).when( meta ).listVariables();
    doReturn( new String[] { "A", "B", "C" } ).when( meta ).listParameters();
    doReturn( "XYZ" ).when( meta ).getVariable( anyString() );
    doReturn( "" ).when( meta ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta ).getParameterDefault( anyString() );
    doReturn( "ABC" ).when( meta ).getParameterValue( anyString() );

    Pipeline pipeline1 = new Pipeline( meta );
    Pipeline pipeline2 = new Pipeline( meta );

    assertEquals( pipeline1.getLogChannelId(), pipeline2.getLogChannelId() );
  }

  /**
   * This test demonstrates the fix for PDI-17436.
   * Two schedules -> two HopServer object Ids -> two log channel Ids
   */
  @Test
  public void testTwoPipelineGetDifferentLogChannelIdWithDifferentCarteId() throws Exception {
    PipelineMeta meta1 = mock( PipelineMeta.class );
    doReturn( new String[] { "X", "Y", "Z" } ).when( meta1 ).listVariables();
    doReturn( new String[] { "A", "B", "C" } ).when( meta1 ).listParameters();
    doReturn( "XYZ" ).when( meta1 ).getVariable( anyString() );
    doReturn( "" ).when( meta1 ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta1 ).getParameterDefault( anyString() );
    doReturn( "ABC" ).when( meta1 ).getParameterValue( anyString() );

    PipelineMeta meta2 = mock( PipelineMeta.class );
    doReturn( new String[] { "X", "Y", "Z" } ).when( meta2 ).listVariables();
    doReturn( new String[] { "A", "B", "C" } ).when( meta2 ).listParameters();
    doReturn( "XYZ" ).when( meta2 ).getVariable( anyString() );
    doReturn( "" ).when( meta2 ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta2 ).getParameterDefault( anyString() );
    doReturn( "ABC" ).when( meta2 ).getParameterValue( anyString() );


    String carteId1 = UUID.randomUUID().toString();
    String carteId2 = UUID.randomUUID().toString();

    doReturn( carteId1 ).when( meta1 ).getContainerObjectId();
    doReturn( carteId2 ).when( meta2 ).getContainerObjectId();

    Pipeline pipeline1 = new Pipeline( meta1 );
    Pipeline pipeline2 = new Pipeline( meta2 );

    assertNotEquals( pipeline1.getContainerObjectId(), pipeline2.getContainerObjectId() );
    assertNotEquals( pipeline1.getLogChannelId(), pipeline2.getLogChannelId() );
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    Pipeline pipelineTest = new Pipeline();
    boolean hasFilename = true;
    boolean hasRepoDir = false;
    pipelineTest.copyVariablesFrom( null );
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    pipelineTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "file:///C:/SomeFilenameDirectory", pipelineTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );

  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    Pipeline pipelineTest = new Pipeline();
    pipelineTest.copyVariablesFrom( null );
    boolean hasFilename = false;
    boolean hasRepoDir = false;
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    pipelineTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "Original value defined at run execution", pipelineTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }

}
