/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.hop.core.annotations.HopLifecyclePlugin;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.lifecycle.HopLifecycleListener;
import org.apache.hop.core.lifecycle.LifecycleException;
import org.apache.hop.core.plugins.HopLifecyclePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVFS;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the Hop Environment
 */
public class HopEnvironmentIT {

  private static final AtomicBoolean environmentInitCalled = new AtomicBoolean( false );
  private static final String pluginId = "MockLifecycleListener";

  @HopLifecyclePlugin( id = pluginId )
  public static class MockLifecycleListener implements HopLifecycleListener {
    @Override
    public void onEnvironmentInit() throws LifecycleException {
      environmentInitCalled.set( true );
    }

    @Override
    public void onEnvironmentShutdown() {
    }
  }

  @HopLifecyclePlugin( id = pluginId )
  public static class FailingMockLifecycleListener extends MockLifecycleListener {
    @Override
    public void onEnvironmentInit() throws LifecycleException {
      throw new LifecycleException( false );
    }
  }

  @HopLifecyclePlugin( id = pluginId )
  public static class SevereFailingMockLifecycleListener extends MockLifecycleListener {
    @Override
    public void onEnvironmentInit() throws LifecycleException {
      throw new LifecycleException( true );
    }
  }

  @HopLifecyclePlugin( id = pluginId )
  public static class ThrowableFailingMockLifecycleListener extends MockLifecycleListener {
    @Override
    public void onEnvironmentInit() throws LifecycleException {
      // Simulate a LifecycleListener that wasn't updated to the latest API
      throw new AbstractMethodError();
    }
  }

  private void resetHopEnvironmentInitializationFlag() throws SecurityException, NoSuchFieldException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
    InstantiationException {
    Field f = HopEnvironment.class.getDeclaredField( "initialized" );
    f.setAccessible( true );
    f.set( null, new AtomicReference<SettableFuture<Boolean>>( null ) );
    Constructor<HopVFS> constructor;
    constructor = HopVFS.class.getDeclaredConstructor();
    constructor.setAccessible( true );
    HopVFS KVFS = constructor.newInstance();
    f = KVFS.getClass().getDeclaredField( "kettleVFS" );
    f.setAccessible( true );
    Field modifiersField = Field.class.getDeclaredField( "modifiers" );
    modifiersField.setAccessible( true );
    modifiersField.setInt( f, f.getModifiers() & ~Modifier.FINAL | Modifier.VOLATILE );
    f.set( null, KVFS );
    f = KVFS.getClass().getDeclaredField( "defaultVariableSpace" );
    f.setAccessible( true );
    modifiersField.setInt( f, f.getModifiers() & ~Modifier.FINAL );
    Variables var = new Variables();
    var.initializeVariablesFrom( null );
    f.set( null, var );

  }

  /**
   * Validate that a LifecycleListener's environment init callback is called when the Hop Environment is initialized.
   */
  @Test
  public void lifecycleListenerEnvironmentInitCallback() throws Exception {
    resetHopEnvironmentInitializationFlag();
    assertFalse( "This test only works if the Hop Environment is not yet initialized", HopEnvironment
      .isInitialized() );
    System.setProperty( Const.HOP_PLUGIN_CLASSES, MockLifecycleListener.class.getName() );
    HopEnvironment.init();

    PluginInterface pi = PluginRegistry.getInstance().findPluginWithId( HopLifecyclePluginType.class, pluginId );
    MockLifecycleListener l =
      (MockLifecycleListener) PluginRegistry.getInstance().loadClass( pi, HopLifecycleListener.class );
    assertNotNull( "Test plugin not registered properly", l );

    assertTrue( environmentInitCalled.get() );
  }

  /**
   * Validate that a LifecycleListener's environment init callback is called when the Hop Environment is initialized.
   */
  @Test
  public void lifecycleListenerEnvironmentInitCallback_exception_thrown() throws Exception {
    resetHopEnvironmentInitializationFlag();
    assertFalse( "This test only works if the Hop Environment is not yet initialized", HopEnvironment
      .isInitialized() );
    System.setProperty( Const.HOP_PLUGIN_CLASSES, FailingMockLifecycleListener.class.getName() );
    HopEnvironment.init();

    PluginInterface pi = PluginRegistry.getInstance().findPluginWithId( HopLifecyclePluginType.class, pluginId );
    MockLifecycleListener l =
      (MockLifecycleListener) PluginRegistry.getInstance().loadClass( pi, HopLifecycleListener.class );
    assertNotNull( "Test plugin not registered properly", l );

    assertTrue( HopEnvironment.isInitialized() );
  }

  /**
   * Validate that a LifecycleListener's environment init callback is called when the Hop Environment is initialized.
   */
  @Test
  public void lifecycleListenerEnvironmentInitCallback_exception_thrown_severe() throws Exception {
    resetHopEnvironmentInitializationFlag();
    assertFalse( "This test only works if the Hop Environment is not yet initialized", HopEnvironment
      .isInitialized() );
    System.setProperty( Const.HOP_PLUGIN_CLASSES, SevereFailingMockLifecycleListener.class.getName() );
    try {
      HopEnvironment.init();
      fail( "Expected exception" );
    } catch ( HopException ex ) {
      assertEquals( LifecycleException.class, ex.getCause().getClass() );
      ex.printStackTrace();
    }

    assertFalse( HopEnvironment.isInitialized() );
  }

  @Test
  public void lifecycleListenerEnvironmentInitCallback_throwable_thrown() throws Exception {
    resetHopEnvironmentInitializationFlag();
    assertFalse( "This test only works if the Hop Environment is not yet initialized", HopEnvironment
      .isInitialized() );
    System.setProperty( Const.HOP_PLUGIN_CLASSES, ThrowableFailingMockLifecycleListener.class.getName() );
    try {
      HopEnvironment.init();
      fail( "Expected exception" );
    } catch ( HopException ex ) {
      assertEquals( AbstractMethodError.class, ex.getCause().getClass() );
      ex.printStackTrace();
    }

    assertFalse( HopEnvironment.isInitialized() );
  }

}
