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

package org.apache.hop.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class PipelineTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  @Mock private ITransform transformMock, transformMock2;
  @Mock private ITransformData data, data2;
  @Mock private TransformMeta transformMeta, transformMeta2;
  @Mock private PipelineMeta pipelineMeta;


  int count = 10000;
  IPipelineEngine<PipelineMeta> pipeline;
  PipelineMeta meta;


  @BeforeClass
  public static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void beforeTest() throws HopException {
    meta = new PipelineMeta();
    pipeline = new LocalPipelineEngine( meta );
    pipeline.setLogChannel( Mockito.mock( ILogChannel.class ) );
    pipeline.prepareExecution();
    pipeline.startThreads();
  }

  /**
   * PDI-14948 - Execution of pipeline with no transforms never ends
   */
  @Test( timeout = 1000 )
  public void pipelineWithNoTransformsIsNotEndless() throws Exception {
    Pipeline pipelineWithNoTransforms = new LocalPipelineEngine( new PipelineMeta() );
    pipelineWithNoTransforms = spy( pipelineWithNoTransforms );

    pipelineWithNoTransforms.prepareExecution();

    pipelineWithNoTransforms.startThreads();

    // check pipeline lifecycle is not corrupted
    verify( pipelineWithNoTransforms ).firePipelineExecutionStartedListeners();
    verify( pipelineWithNoTransforms ).firePipelineExecutionFinishedListeners();
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
  public void testFirePipelineFinishedListeners() throws Exception {
    Pipeline pipeline = new LocalPipelineEngine();
    IExecutionFinishedListener mockListener = mock( IExecutionFinishedListener.class );
    pipeline.setExecutionFinishedListeners( Collections.singletonList( mockListener ) );

    pipeline.firePipelineExecutionFinishedListeners();

    verify( mockListener ).finished( pipeline );
  }

  @Test( expected = HopException.class )
  public void testFirePipelineFinishedListenersExceptionOnPipelineFinished() throws Exception {
    Pipeline pipeline = new LocalPipelineEngine();
    IExecutionFinishedListener mockListener = mock( IExecutionFinishedListener.class );
    doThrow( HopException.class ).when( mockListener ).finished( pipeline );
    pipeline.setExecutionFinishedListeners( Collections.singletonList( mockListener ) );

    pipeline.firePipelineExecutionFinishedListeners();
  }

  @Test
  public void testFinishStatus() throws Exception {
    while ( pipeline.isRunning() ) {
      Thread.sleep( 1 );
    }
    assertEquals( Pipeline.STRING_FINISHED, pipeline.getStatusDescription() );
  }

  private void verifyStopped( ITransform transform, int numberTimesCalled ) throws HopException {
    verify( transform, times( numberTimesCalled ) ).setStopped( true );
    verify( transform, times( numberTimesCalled ) ).setSafeStopped( true );
    verify( transform, times( numberTimesCalled ) ).resumeRunning();
    verify( transform, times( numberTimesCalled ) ).stopRunning();
  }

  private TransformMetaDataCombi combi( ITransform transform, ITransformData data, TransformMeta transformMeta ) {
    TransformMetaDataCombi transformMetaDataCombi = new TransformMetaDataCombi();
    transformMetaDataCombi.transform = transform;
    transformMetaDataCombi.data = data;
    transformMetaDataCombi.transformMeta = transformMeta;
    return transformMetaDataCombi;
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
    protected IPipelineEngine<PipelineMeta> pipeline;
    protected int c = 0;
    protected CountDownLatch start;
    protected int max = count;

    PipelineKicker( IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start ) {
      this.pipeline = pipeline;
      this.start = start;
    }

    protected boolean isStopped() {
      c++;
      return c >= max;
    }
  }

  private class PipelineStoppedCaller extends PipelineKicker {
    PipelineStoppedCaller( IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start ) {
      super( pipeline, start );
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
    PipelineStopListenerAdder( IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start ) {
      super( pipeline, start );
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch ( InterruptedException e ) {
        throw new RuntimeException();
      }
      while ( !isStopped() ) {
        try {
          pipeline.addExecutionStoppedListener( pipelineStoppedListener );
        } catch ( HopException e ) {
          throw new RuntimeException( e );
        }
      }
    }
  }

  private class PipelineFinishListenerAdder extends PipelineKicker {
    PipelineFinishListenerAdder( IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start ) {
      super( pipeline, start );
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
          pipeline.addExecutionFinishedListener( listener );
        } catch ( HopException e ) {
          throw new RuntimeException( e );
        }
      }
    }
  }

  private class PipelineFinishListenerFirer extends PipelineKicker {
    PipelineFinishListenerFirer( IPipelineEngine<PipelineMeta> tr, CountDownLatch start ) {
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
          pipeline.firePipelineExecutionFinishedListeners();
          // clean array blocking queue
          pipeline.waitUntilFinished();
        } catch ( HopException e ) {
          throw new RuntimeException();
        }
      }
    }
  }

  private class PipelineStartListenerFirer extends PipelineKicker {
    PipelineStartListenerFirer( IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start ) {
      super( pipeline, start );
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
          pipeline.firePipelineExecutionStartedListeners();
        } catch ( HopException e ) {
          throw new RuntimeException();
        }
      }
    }
  }

  private final IExecutionFinishedListener<IPipelineEngine<PipelineMeta>> listener = pipeline -> { };

  private final IExecutionStoppedListener<IPipelineEngine<PipelineMeta>> pipelineStoppedListener = pipeline -> { };

  @Test
  public void testNewPipelineWithContainerObjectId() throws Exception {
    PipelineMeta meta = mock( PipelineMeta.class );

    doReturn( new String[] { "A", "B", "C" } ).when( meta ).listParameters();
    doReturn( "" ).when( meta ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta ).getParameterDefault( anyString() );

    String carteId = UUID.randomUUID().toString();
    doReturn( carteId ).when( meta ).getContainerId();

    Pipeline pipeline = new LocalPipelineEngine( meta );

    assertEquals( carteId, pipeline.getContainerId() );
  }

  /**
   * This test demonstrates the issue fixed in PDI-17436.
   * When a workflow is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  public void testTwoPipelineGetSameLogChannelId() throws Exception {
    PipelineMeta meta = mock( PipelineMeta.class );
    doReturn( new String[] { "A", "B", "C" } ).when( meta ).listParameters();
    doReturn( "" ).when( meta ).getParameterDescription( anyString() );
    doReturn( "" ).when( meta ).getParameterDefault( anyString() );

    IPipelineEngine<PipelineMeta> pipeline1 = new LocalPipelineEngine( meta );
    IPipelineEngine<PipelineMeta> pipeline2 = new LocalPipelineEngine( meta );

    assertEquals( pipeline1.getLogChannelId(), pipeline2.getLogChannelId() );
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    Pipeline pipelineTest = new LocalPipelineEngine();
    boolean hasFilename = true;
    boolean hasRepoDir = false;
    pipelineTest.copyFrom( null );
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution" );
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    pipelineTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "file:///C:/SomeFilenameDirectory", pipelineTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER ) );

  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    Pipeline pipelineTest = new LocalPipelineEngine();
    pipelineTest.copyFrom( null );
    boolean hasFilename = false;
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution" );
    pipelineTest.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    pipelineTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "Original value defined at run execution", pipelineTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER ) );
  }

}
