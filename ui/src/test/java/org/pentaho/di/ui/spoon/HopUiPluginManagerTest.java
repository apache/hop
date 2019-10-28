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
package org.apache.hop.ui.hopui;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class HopUiPluginManagerTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  @Spy
  private HopUiPluginManager hopUiPluginManager;

  @Mock
  private PluginRegistry pluginRegistry;

  @Mock
  private PluginInterface plugin1, plugin2;

  @Mock
  private HopUiPerspective hopUiPerspective;

  @Mock
  private HopUiPerspectiveManager hopUiPerspectiveManager;

  @Mock
  private XulDomContainer xulDomContainer;

  private HopUiPluginInterface hopUiPluginInterface1 = new DummyPluginInterface();
  private HopUiPluginInterface hopUiPluginInterface2 = new DummyPluginInterface();
  private DummyLifecycleListener dummyLifecycleListener = new DummyLifecycleListener();

  private Map<HopUiPluginInterface, Integer> applies = new HashMap<>();
  private AtomicInteger notifications = new AtomicInteger();

  @Before
  public void setUp() throws HopPluginException {
    when( hopUiPluginManager.getPluginRegistry() ).thenReturn( pluginRegistry );
    when( hopUiPluginManager.getSpoonPerspectiveManager() ).thenReturn( hopUiPerspectiveManager );
    when( pluginRegistry.loadClass( any( PluginInterface.class ) ) )
        .thenReturn( hopUiPluginInterface1, hopUiPluginInterface2 );
  }

  @Test
  public void testPluginAdded() throws Exception {
    hopUiPluginManager.pluginAdded( plugin1 );

    verify( hopUiPerspectiveManager ).addPerspective( hopUiPerspective );
    assertEquals( 1, hopUiPluginManager.getPlugins().size() );
    assertSame( hopUiPluginInterface1, hopUiPluginManager.getPlugins().get( 0 ) );
  }

  @Test
  public void testPluginRemoved() throws Exception {
    hopUiPluginManager.pluginAdded( plugin1 );
    hopUiPluginManager.pluginRemoved( plugin1 );

    verify( hopUiPerspectiveManager ).removePerspective( hopUiPerspective );
  }

  @Test
  public void testApplyPluginsForContainer() throws Exception {
    hopUiPluginManager.pluginAdded( plugin1 );
    hopUiPluginManager.pluginAdded( plugin2 );
    hopUiPluginManager.applyPluginsForContainer( "trans-graph", xulDomContainer );

    assertEquals( 2, applies.size() );
    assertEquals( 1, (int) applies.get( hopUiPluginInterface1 ) );
    assertEquals( 1, (int) applies.get( hopUiPluginInterface2 ) );
  }

  @Test
  public void testGetPlugins() throws Exception {
    hopUiPluginManager.pluginAdded( plugin1 );
    hopUiPluginManager.pluginAdded( plugin2 );

    List<HopUiPluginInterface> pluginInterfaces = hopUiPluginManager.getPlugins();

    assertEquals( 2, pluginInterfaces.size() );
    assertTrue( pluginInterfaces
        .containsAll( Arrays.asList( hopUiPluginInterface1, hopUiPluginInterface2 ) ) );
  }

  @Test
  public void testNotifyLifecycleListeners() throws Exception {
    hopUiPluginManager.pluginAdded( plugin1 );
    hopUiPluginManager.pluginAdded( plugin2 );

    hopUiPluginManager.notifyLifecycleListeners( HopUiLifecycleListener.SpoonLifeCycleEvent.STARTUP );

    assertEquals( 2, notifications.get() );
  }

  @HopUiPluginCategories( { "trans-graph" } )
  private class DummyPluginInterface implements HopUiPluginInterface {
    @Override public void applyToContainer( String category, XulDomContainer container ) throws XulException {
      if ( applies.get( this ) == null ) {
        applies.put( this, 1 );
      } else {
        applies.put( this, applies.get( this ) + 1 );
      }
    }

    @Override public HopUiLifecycleListener getLifecycleListener() {
      return dummyLifecycleListener;
    }

    @Override public HopUiPerspective getPerspective() {
      return hopUiPerspective;
    }
  }

  private class DummyLifecycleListener implements HopUiLifecycleListener {
    @Override public void onEvent( SpoonLifeCycleEvent evt ) {
      if ( evt == SpoonLifeCycleEvent.STARTUP ) {
        notifications.incrementAndGet();
      }
    }
  }
}
