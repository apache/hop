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

package org.apache.hop.trans;

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
import org.apache.hop.trans.engine.IEngine;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaDataCombi;
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
public class TransTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  @Mock private StepInterface stepMock, stepMock2;
  @Mock private StepDataInterface data, data2;
  @Mock private StepMeta stepMeta, stepMeta2;
  @Mock private TransMeta transMeta;


  int count = 10000;
  Trans trans;
  TransMeta meta;


  @BeforeClass
  public static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void beforeTest() throws HopException {
    meta = new TransMeta();
    trans = new Trans( meta );
    trans.setLog( Mockito.mock( LogChannelInterface.class ) );
    trans.prepareExecution();
    trans.startThreads();
  }

  /**
   * PDI-14948 - Execution of trans with no steps never ends
   */
  @Test( timeout = 1000 )
  public void transWithNoStepsIsNotEndless() throws Exception {
    Trans transWithNoSteps = new Trans( new TransMeta() );
    transWithNoSteps = spy( transWithNoSteps );

    transWithNoSteps.prepareExecution();

    transWithNoSteps.startThreads();

    // check trans lifecycle is not corrupted
    verify( transWithNoSteps ).fireTransStartedListeners();
    verify( transWithNoSteps ).fireTransFinishedListeners();
  }

  /**
   * PDI-10762 - Trans and TransMeta leak
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
   * PDI-5229 - ConcurrentModificationException when restarting transformation Test that listeners can be accessed
   * concurrently during transformation finish
   *
   * @throws HopException
   * @throws InterruptedException
   */
  @Test
  public void testTransFinishListenersConcurrentModification() throws HopException, InterruptedException {
    CountDownLatch start = new CountDownLatch( 1 );
    TransFinishListenerAdder add = new TransFinishListenerAdder( trans, start );
    TransFinishListenerFirer firer = new TransFinishListenerFirer( trans, start );
    startThreads( add, firer, start );
    assertEquals( "All listeners are added: no ConcurrentModificationException", count, add.c );
    assertEquals( "All Finish listeners are iterated over: no ConcurrentModificationException", count, firer.c );
  }

  /**
   * Test that listeners can be accessed concurrently during transformation start
   *
   * @throws InterruptedException
   */
  @Test
  public void testTransStartListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch( 1 );
    TransFinishListenerAdder add = new TransFinishListenerAdder( trans, start );
    TransStartListenerFirer starter = new TransStartListenerFirer( trans, start );
    startThreads( add, starter, start );
    assertEquals( "All listeners are added: no ConcurrentModificationException", count, add.c );
    assertEquals( "All Start listeners are iterated over: no ConcurrentModificationException", count, starter.c );
  }

  /**
   * Test that transformation stop listeners can be accessed concurrently
   *
   * @throws InterruptedException
   */
  @Test
  public void testTransStoppedListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch( 1 );
    TransStoppedCaller stopper = new TransStoppedCaller( trans, start );
    TransStopListenerAdder adder = new TransStopListenerAdder( trans, start );
    startThreads( stopper, adder, start );
    assertEquals( "All transformation stop listeners is added", count, adder.c );
    assertEquals( "All stop call success", count, stopper.c );
  }

  @Test
  public void testPDI12424ParametersFromMetaAreCopiedToTrans() throws HopException, URISyntaxException, IOException {
    String testParam = "testParam";
    String testParamValue = "testParamValue";
    TransMeta mockTransMeta = mock( TransMeta.class );
    when( mockTransMeta.listVariables() ).thenReturn( new String[] {} );
    when( mockTransMeta.listParameters() ).thenReturn( new String[] { testParam } );
    when( mockTransMeta.getParameterValue( testParam ) ).thenReturn( testParamValue );
    FileObject ktr = HopVFS.createTempFile( "parameters", ".ktr", "ram://" );
    try ( OutputStream outputStream = ktr.getContent().getOutputStream( true ) ) {
      InputStream inputStream = new ByteArrayInputStream( "<transformation></transformation>".getBytes() );
      IOUtils.copy( inputStream, outputStream );
    }
    Trans trans = new Trans( mockTransMeta, null, ktr.getURL().toURI().toString(), null );
    assertEquals( testParamValue, trans.getParameterValue( testParam ) );
  }

  @Test
  public void testRecordsCleanUpMethodIsCalled() throws Exception {
    Database mockedDataBase = mock( Database.class );
    Trans trans = mock( Trans.class );

    StepLogTable stepLogTable =
      StepLogTable.getDefault( mock( VariableSpace.class ), mock( IMetaStore.class ) );
    stepLogTable.setConnectionName( "connection" );

    TransMeta transMeta = new TransMeta();
    transMeta.setStepLogTable( stepLogTable );

    when( trans.getTransMeta() ).thenReturn( transMeta );
    when( trans.createDataBase( any( DatabaseMeta.class ) ) ).thenReturn( mockedDataBase );
    when( trans.getSteps() ).thenReturn( new ArrayList<>() );

    doCallRealMethod().when( trans ).writeStepLogInformation();
    trans.writeStepLogInformation();

    verify( mockedDataBase ).cleanupLogRecords( stepLogTable );
  }

  @Test
  public void testFireTransFinishedListeners() throws Exception {
    Trans trans = new Trans();
    ExecutionListener mockListener = mock( ExecutionListener.class );
    trans.setExecutionListeners( Collections.singletonList( mockListener ) );

    trans.fireTransFinishedListeners();

    verify( mockListener ).finished( trans );
  }

  @Test( expected = HopException.class )
  public void testFireTransFinishedListenersExceptionOnTransFinished() throws Exception {
    Trans trans = new Trans();
    ExecutionListener mockListener = mock( ExecutionListener.class );
    doThrow( HopException.class ).when( mockListener ).finished( trans );
    trans.setExecutionListeners( Collections.singletonList( mockListener ) );

    trans.fireTransFinishedListeners();
  }

  @Test
  public void testFinishStatus() throws Exception {
    while ( trans.isRunning() ) {
      Thread.sleep( 1 );
    }
    assertEquals( Trans.STRING_FINISHED, trans.getStatus() );
  }

  @Test
  public void testSafeStop() {
    when( stepMock.isSafeStopped() ).thenReturn( false );
    when( stepMock.getStepname() ).thenReturn( "stepName" );

    trans.setSteps( of( combi( stepMock, data, stepMeta ) ) );
    Result result = trans.getResult();
    assertFalse( result.isSafeStop() );

    when( stepMock.isSafeStopped() ).thenReturn( true );
    result = trans.getResult();
    assertTrue( result.isSafeStop() );
  }

  @Test
  public void safeStopStopsInputStepsRightAway() throws HopException {
    trans.setSteps( of( combi( stepMock, data, stepMeta ) ) );
    when( transMeta.findPreviousSteps( stepMeta, true ) ).thenReturn( emptyList() );
    trans.transMeta = transMeta;
    trans.safeStop();
    verifyStopped( stepMock, 1 );
  }

  @Test
  public void safeLetsNonInputStepsKeepRunning() throws HopException {
    trans.setSteps( of(
      combi( stepMock, data, stepMeta ),
      combi( stepMock2, data2, stepMeta2 ) ) );

    when( transMeta.findPreviousSteps( stepMeta, true ) ).thenReturn( emptyList() );
    // stepMeta2 will have stepMeta as previous, so is not an input step
    when( transMeta.findPreviousSteps( stepMeta2, true ) ).thenReturn( of( stepMeta ) );
    trans.transMeta = transMeta;

    trans.safeStop();
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

  private abstract class TransKicker implements Runnable {
    protected Trans tr;
    protected int c = 0;
    protected CountDownLatch start;
    protected int max = count;

    TransKicker( Trans tr, CountDownLatch start ) {
      this.tr = tr;
      this.start = start;
    }

    protected boolean isStopped() {
      c++;
      return c >= max;
    }
  }

  private class TransStoppedCaller extends TransKicker {
    TransStoppedCaller( Trans tr, CountDownLatch start ) {
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
        trans.stopAll();
      }
    }
  }

  private class TransStopListenerAdder extends TransKicker {
    TransStopListenerAdder( Trans tr, CountDownLatch start ) {
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
        trans.addTransStoppedListener( transStoppedListener );
      }
    }
  }

  private class TransFinishListenerAdder extends TransKicker {
    TransFinishListenerAdder( Trans tr, CountDownLatch start ) {
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
        tr.addTransListener( listener );
      }
    }
  }

  private class TransFinishListenerFirer extends TransKicker {
    TransFinishListenerFirer( Trans tr, CountDownLatch start ) {
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
          tr.fireTransFinishedListeners();
          // clean array blocking queue
          tr.waitUntilFinished();
        } catch ( HopException e ) {
          throw new RuntimeException();
        }
      }
    }
  }

  private class TransStartListenerFirer extends TransKicker {
    TransStartListenerFirer( Trans tr, CountDownLatch start ) {
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
          tr.fireTransStartedListeners();
        } catch ( HopException e ) {
          throw new RuntimeException();
        }
      }
    }
  }

  private final ExecutionListener<TransMeta> listener = new ExecutionListener<TransMeta>() {
    @Override
    public void started( IEngine<TransMeta> trans ) throws HopException {
    }

    @Override
    public void becameActive( IEngine<TransMeta> trans ) {
    }

    @Override
    public void finished( IEngine<TransMeta> trans ) throws HopException {
    }
  };

  private final TransStoppedListener transStoppedListener = new TransStoppedListener() {
    @Override
    public void transStopped( Trans trans ) {
    }

  };

  @Test
  public void testNewTransformationsWithContainerObjectId() throws Exception {
    TransMeta meta = mock( TransMeta.class );
    doReturn( new String[] { "X", "Y", "Z" } ).when( meta ).listVariables();
    doReturn( new String[] { "A", "B", "C" } ).when( meta ).listParameters();
    doReturn( "XYZ" ).when( meta ).getVariable( anyString() );
    doReturn( "" ).when( meta ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta ).getParameterDefault( anyString() );
    doReturn( "ABC" ).when( meta ).getParameterValue( anyString() );

    String carteId = UUID.randomUUID().toString();
    doReturn( carteId ).when( meta ).getContainerObjectId();

    Trans trans = new Trans( meta );

    assertEquals( carteId, trans.getContainerObjectId() );
  }

  /**
   * This test demonstrates the issue fixed in PDI-17436.
   * When a job is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  public void testTwoTransformationsGetSameLogChannelId() throws Exception {
    TransMeta meta = mock( TransMeta.class );
    doReturn( new String[] { "X", "Y", "Z" } ).when( meta ).listVariables();
    doReturn( new String[] { "A", "B", "C" } ).when( meta ).listParameters();
    doReturn( "XYZ" ).when( meta ).getVariable( anyString() );
    doReturn( "" ).when( meta ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta ).getParameterDefault( anyString() );
    doReturn( "ABC" ).when( meta ).getParameterValue( anyString() );

    Trans trans1 = new Trans( meta );
    Trans trans2 = new Trans( meta );

    assertEquals( trans1.getLogChannelId(), trans2.getLogChannelId() );
  }

  /**
   * This test demonstrates the fix for PDI-17436.
   * Two schedules -> two HopServer object Ids -> two log channel Ids
   */
  @Test
  public void testTwoTransformationsGetDifferentLogChannelIdWithDifferentCarteId() throws Exception {
    TransMeta meta1 = mock( TransMeta.class );
    doReturn( new String[] { "X", "Y", "Z" } ).when( meta1 ).listVariables();
    doReturn( new String[] { "A", "B", "C" } ).when( meta1 ).listParameters();
    doReturn( "XYZ" ).when( meta1 ).getVariable( anyString() );
    doReturn( "" ).when( meta1 ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta1 ).getParameterDefault( anyString() );
    doReturn( "ABC" ).when( meta1 ).getParameterValue( anyString() );

    TransMeta meta2 = mock( TransMeta.class );
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

    Trans trans1 = new Trans( meta1 );
    Trans trans2 = new Trans( meta2 );

    assertNotEquals( trans1.getContainerObjectId(), trans2.getContainerObjectId() );
    assertNotEquals( trans1.getLogChannelId(), trans2.getLogChannelId() );
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    Trans transTest = new Trans();
    boolean hasFilename = true;
    boolean hasRepoDir = false;
    transTest.copyVariablesFrom( null );
    transTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    transTest.setVariable( Const.INTERNAL_VARIABLE_TRANSFORMATION_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    transTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "file:///C:/SomeFilenameDirectory", transTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );

  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    Trans transTest = new Trans();
    transTest.copyVariablesFrom( null );
    boolean hasFilename = false;
    boolean hasRepoDir = false;
    transTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    transTest.setVariable( Const.INTERNAL_VARIABLE_TRANSFORMATION_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    transTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "Original value defined at run execution", transTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }

}
