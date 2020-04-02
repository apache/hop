/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2016 - 2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.lifecycle;

import org.apache.hop.core.plugins.HopLifecyclePluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.IPluginTypeListener;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class HopLifecycleSupportTest {

  private PluginRegistry registry;
  private List<IPlugin> registeredPlugins;
  private ArgumentCaptor<IPluginTypeListener> typeListenerRegistration;

  @Before
  public void setUpPluginRegistry() throws Exception {
    // Intercept access to registry
    registry = LifecycleSupport.registry = HopLifecycleSupport.registry = mock( PluginRegistry.class );
    registeredPlugins = new ArrayList<IPlugin>();
    when( registry.getPlugins( HopLifecyclePluginType.class ) ).thenReturn( registeredPlugins );
    typeListenerRegistration = ArgumentCaptor.forClass( IPluginTypeListener.class );
    doNothing().when( registry ).addPluginListener( eq( HopLifecyclePluginType.class ), typeListenerRegistration.capture() );
  }

  @Test
  public void testOnEnvironmentInit() throws Exception {
    final List<IHopLifecycleListener> listeners = new ArrayList<IHopLifecycleListener>();
    listeners.add( createLifecycleListener() );
    HopLifecycleSupport kettleLifecycleSupport = new HopLifecycleSupport();
    assertNotNull( typeListenerRegistration.getValue() );

    IHopLifecycleListener preInit = createLifecycleListener();
    listeners.add( preInit );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        listeners.add( createLifecycleListener() );
        return null;
      }
    } ).when( preInit ).onEnvironmentInit();

    verifyNoMoreInteractions( listeners.toArray() );

    // Init environment
    kettleLifecycleSupport.onEnvironmentInit();
    for ( IHopLifecycleListener listener : listeners ) {
      verify( listener ).onEnvironmentInit();
    }
    verifyNoMoreInteractions( listeners.toArray() );

    IHopLifecycleListener postInit = createLifecycleListener();
    verify( postInit ).onEnvironmentInit();

    verifyNoMoreInteractions( listeners.toArray() );
  }

  private IHopLifecycleListener createLifecycleListener() throws org.apache.hop.core.exception.HopPluginException {
    IPlugin pluginInterface = mock( IPlugin.class );
    IHopLifecycleListener kettleLifecycleListener = mock( IHopLifecycleListener.class );
    registeredPlugins.add( pluginInterface );
    when( registry.loadClass( pluginInterface, IHopLifecycleListener.class ) ).thenReturn( kettleLifecycleListener );
    when( registry.loadClass( pluginInterface ) ).thenReturn( kettleLifecycleListener );
    if ( !typeListenerRegistration.getAllValues().isEmpty() ) {
      typeListenerRegistration.getValue().pluginAdded( pluginInterface );
    }
    return kettleLifecycleListener;
  }
}
