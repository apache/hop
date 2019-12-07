/*
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 * **************************************************************************
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
 */

package org.apache.hop.core.extension;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ExtensionPointIntegrationTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();
  public static final String EXECUTED_FIELD_NAME = "executed";
  private static final int TOTAL_THREADS_TO_RUN = 2000;
  private static final int MAX_TIMEOUT_SECONDS = 60;
  private static ClassPool pool;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    pool = ClassPool.getDefault();
    pool.insertClassPath( new ClassClassPath( ExtensionPointIntegrationTest.class ) );
    for ( HopExtensionPoint ep : HopExtensionPoint.values() ) {
      ExtensionPointPluginType.getInstance().registerCustom( createClassRuntime( ep ),
          "custom", "id" + ep.id, ep.id, "no description", null );
    }

    HopClientEnvironment.init();
  }

  @Test
  public void test() throws Exception {
    // check that all extension points are added to the map
    assertEquals( HopExtensionPoint.values().length, ExtensionPointMap.getInstance().getNumberOfRows() );

    // check that all extension points are executed
    final LogChannelInterface log = mock( LogChannelInterface.class );
    for ( HopExtensionPoint ep : HopExtensionPoint.values() ) {
      final ExtensionPointInterface currentEP = ExtensionPointMap.getInstance().getTableValue( ep.id, "id" + ep.id );
      assertFalse( currentEP.getClass().getField( EXECUTED_FIELD_NAME ).getBoolean( currentEP ) );
      ExtensionPointHandler.callExtensionPoint( log, ep.id, null );
      assertTrue( currentEP.getClass().getField( EXECUTED_FIELD_NAME ).getBoolean( currentEP ) );
    }

    // check modification of extension point
    final HopExtensionPoint jobAfterOpen = HopExtensionPoint.JobAfterOpen;
    final ExtensionPointInterface int1 = ExtensionPointMap.getInstance().getTableValue( jobAfterOpen.id, "id" + jobAfterOpen.id );
    ExtensionPointPluginType.getInstance().registerCustom( createClassRuntime( jobAfterOpen, "Edited" ), "custom", "id"
            + jobAfterOpen.id, jobAfterOpen.id,
        "no description", null );
    assertNotSame( int1, ExtensionPointMap.getInstance().getTableValue( jobAfterOpen.id, "id" + jobAfterOpen.id ) );
    assertEquals( HopExtensionPoint.values().length, ExtensionPointMap.getInstance().getNumberOfRows() );

    // check removal of extension point
    PluginRegistry.getInstance().removePlugin( ExtensionPointPluginType.class, PluginRegistry.getInstance().getPlugin(
        ExtensionPointPluginType.class, "id" + jobAfterOpen.id ) );
    assertTrue( ExtensionPointMap.getInstance().getTableValue( jobAfterOpen.id, "id" + jobAfterOpen.id ) == null );
    assertEquals( HopExtensionPoint.values().length - 1, ExtensionPointMap.getInstance().getNumberOfRows() );
  }

  private static Class createClassRuntime( HopExtensionPoint ep ) throws NotFoundException, CannotCompileException {
    return createClassRuntime( ep, "" );
  }

  /**
   * Create ExtensionPointInterface subclass in runtime
   *
   * @param ep extension point id
   * @param addition addition to class name to avoid duplicate classes
   * @return class
   * @throws NotFoundException
   * @throws CannotCompileException
   */
  private static Class createClassRuntime( HopExtensionPoint ep, String addition )
      throws NotFoundException, CannotCompileException {
    final CtClass ctClass = pool.makeClass( "Plugin" + ep.id + addition );
    ctClass.addInterface( pool.get( ExtensionPointInterface.class.getCanonicalName() ) );
    ctClass.addField( CtField.make( "public boolean " + EXECUTED_FIELD_NAME + ";", ctClass ) );
    ctClass.addMethod( CtNewMethod.make(
        "public void callExtensionPoint( org.apache.hop.core.logging.LogChannelInterface log, Object object ) "
            + "throws org.apache.hop.core.exception.HopException { " + EXECUTED_FIELD_NAME + " = true; }",
        ctClass ) );
    return ctClass.toClass();
  }

  @Test
  public void testExtensionPointMapConcurrency() throws InterruptedException {
    final LogChannelInterface log = mock( LogChannelInterface.class );

    List<Runnable> parallelTasksList = new ArrayList<>( TOTAL_THREADS_TO_RUN );
    for ( int i = 0; i < TOTAL_THREADS_TO_RUN; i++ ) {
      parallelTasksList.add( () -> {
        HopExtensionPoint kettleExtensionPoint = getRandomHopExtensionPoint();
        PluginInterface pluginInterface = PluginRegistry.getInstance().getPlugin( ExtensionPointPluginType.class,
          "id" + kettleExtensionPoint.id );

        try {
          PluginRegistry.getInstance().removePlugin( ExtensionPointPluginType.class, pluginInterface );
          PluginRegistry.getInstance().registerPlugin( ExtensionPointPluginType.class, pluginInterface );
        } catch ( HopPluginException e ) {
          e.printStackTrace();
        } catch ( NullPointerException e ) {
          //NullPointerException can be thrown if trying to remove a plugin that doesn't exit, discarding occurence
        }

        ExtensionPointMap.getInstance().reInitialize();

        try {
          ExtensionPointMap.getInstance().callExtensionPoint( log, kettleExtensionPoint.id, null );
        } catch ( HopException e ) {
          e.printStackTrace();
        }
      } );
    }

    assertConcurrent( parallelTasksList );
  }

  private static HopExtensionPoint getRandomHopExtensionPoint() {
    HopExtensionPoint[] kettleExtensionPoints = HopExtensionPoint.values();
    int randomInd = ThreadLocalRandom.current().nextInt( 0, kettleExtensionPoints.length );
    return kettleExtensionPoints[randomInd];
  }

  private static void assertConcurrent( final List<? extends Runnable> runnables )  throws InterruptedException {
    final int numThreads = runnables.size();
    final List<Throwable> exceptions = Collections.synchronizedList( new ArrayList<>() );
    final ExecutorService threadPool = Executors.newFixedThreadPool( numThreads );

    try {
      final CountDownLatch allExecutorThreadsReady = new CountDownLatch( numThreads );
      final CountDownLatch afterInitBlocker = new CountDownLatch( 1 );
      final CountDownLatch allDone = new CountDownLatch( numThreads );
      for ( final Runnable submittedTestRunnable : runnables ) {
        threadPool.submit( () -> {
          allExecutorThreadsReady.countDown();
          try {
            afterInitBlocker.await();
            submittedTestRunnable.run();
          } catch ( final Throwable e ) {
            exceptions.add( e );
          } finally {
            allDone.countDown();
          }
        } );
      }
      // wait until all threads are ready
      assertTrue(
        "Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent",
        allExecutorThreadsReady.await( 10L * runnables.size(), TimeUnit.MILLISECONDS ) );
      // start all test runners
      afterInitBlocker.countDown();
      assertTrue( String.format( "Timeout! Run took more than %s seconds", MAX_TIMEOUT_SECONDS ),
        allDone.await( MAX_TIMEOUT_SECONDS, TimeUnit.SECONDS ) );
    } finally {
      threadPool.shutdownNow();
    }
    assertTrue( String.format( " Run failed with exception(s): %s", exceptions ), exceptions.isEmpty() );
  }
}
