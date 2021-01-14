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

package org.apache.hop.core.plugins;

import org.apache.hop.core.exception.HopPluginClassMapException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.extension.IPluginMock;
import org.apache.hop.core.logging.LoggingPluginType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PluginRegistryUnitTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @Test
  public void getGetPluginInformation() throws HopPluginException {
    PluginRegistry.getInstance().reset();
    RowBuffer result = PluginRegistry.getInstance().getPluginInformation( BasePluginType.class );
    assertNotNull( result );
    assertEquals( 9, result.getRowMeta().size() );

    for ( IValueMeta vmi : result.getRowMeta().getValueMetaList() ) {
      assertEquals( IValueMeta.TYPE_STRING, vmi.getType() );
    }
  }

  /**
   * Test that additional plugin mappings can be added via the PluginRegistry.
   */
  @Test
  public void testSupplementalPluginMappings() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin mockPlugin = mock( IPlugin.class );
    when( mockPlugin.getIds() ).thenReturn( new String[] { "mockPlugin" } );
    when( mockPlugin.matches( "mockPlugin" ) ).thenReturn( true );
    when( mockPlugin.getName() ).thenReturn( "mockPlugin" );
    doReturn( LoggingPluginType.class ).when( mockPlugin ).getPluginType();
    registry.registerPlugin( LoggingPluginType.class, mockPlugin );

    registry.addClassFactory( LoggingPluginType.class, String.class, "mockPlugin", () -> "Foo" );
    String result = registry.loadClass( LoggingPluginType.class, "mockPlugin", String.class );
    assertEquals( "Foo", result );
    assertEquals( 2, registry.getPlugins( LoggingPluginType.class ).size() );

    // Now add another mapping and verify that it works and the existing supplementalPlugin was reused.
    UUID uuid = UUID.randomUUID();
    registry.addClassFactory( LoggingPluginType.class, UUID.class, "mockPlugin", () -> uuid );
    UUID out = registry.loadClass( LoggingPluginType.class, "mockPlugin", UUID.class );
    assertEquals( uuid, out );
    assertEquals( 2, registry.getPlugins( LoggingPluginType.class ).size() );
  }

  /**
   * Test that several plugin jar can share the same classloader.
   */
  @Test
  public void testPluginClassloaderGroup() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin mockPlugin1 = mock( IPlugin.class );
    when( mockPlugin1.getIds() ).thenReturn( new String[] { "mockPlugin" } );
    when( mockPlugin1.matches( "mockPlugin" ) ).thenReturn( true );
    when( mockPlugin1.getName() ).thenReturn( "mockPlugin" );
    when( mockPlugin1.getClassMap() ).thenReturn( new HashMap<Class<?>, String>() {{
      put( IPluginType.class, String.class.getName() );
    }} );
    when( mockPlugin1.getClassLoaderGroup() ).thenReturn( "groupPlugin" );
    doReturn( BasePluginType.class ).when( mockPlugin1 ).getPluginType();

    IPlugin mockPlugin2 = mock( IPlugin.class );
    when( mockPlugin2.getIds() ).thenReturn( new String[] { "mockPlugin2" } );
    when( mockPlugin2.matches( "mockPlugin2" ) ).thenReturn( true );
    when( mockPlugin2.getName() ).thenReturn( "mockPlugin2" );
    when( mockPlugin2.getClassMap() ).thenReturn( new HashMap<Class<?>, String>() {{
      put( IPluginType.class, Integer.class.getName() );
    }} );
    when( mockPlugin2.getClassLoaderGroup() ).thenReturn( "groupPlugin" );
    doReturn( BasePluginType.class ).when( mockPlugin2 ).getPluginType();

    registry.registerPlugin( BasePluginType.class, mockPlugin1 );
    registry.registerPlugin( BasePluginType.class, mockPlugin2 );

    // test they share the same classloader
    ClassLoader ucl = registry.getClassLoader( mockPlugin1 );
    assertEquals( ucl, registry.getClassLoader( mockPlugin2 ) );

    // test removing a shared plugin creates a new classloader
    registry.removePlugin( BasePluginType.class, mockPlugin2 );
    assertNotEquals( ucl, registry.getClassLoader( mockPlugin1 ) );
  }

  @Test( expected = HopPluginClassMapException.class )
  public void testClassloadingPluginNoClassRegistered() throws HopPluginException {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPluginMock plugin = mock( IPluginMock.class );
    when( plugin.loadClass( any() ) ).thenReturn( null );
    registry.loadClass( plugin, Class.class );
  }

  @Test
  public void testMergingPluginFragment() throws HopPluginException {
    // setup
    // initialize Fragment Type
    PluginRegistry registry = PluginRegistry.getInstance();
    BaseFragmentType<ValueMetaPlugin> fragmentType = new BaseFragmentType<ValueMetaPlugin>( ValueMetaPlugin.class, "", "", ValueMetaPluginType.class ) {
      @Override protected void initListeners( Class<? extends IPluginType> aClass,
                                              Class<? extends IPluginType> typeToTrack ) {
        super.initListeners( BaseFragmentType.class, typeToTrack );
      }

      @Override protected String extractID( ValueMetaPlugin annotation ) {
        return null;
      }

      @Override protected String extractImageFile( ValueMetaPlugin annotation ) {
        return null;
      }

      @Override protected String extractDocumentationUrl( ValueMetaPlugin annotation ) {
        return null;
      }

      @Override protected String extractCasesUrl( ValueMetaPlugin annotation ) {
        return null;
      }

      @Override protected String extractForumUrl( ValueMetaPlugin annotation ) {
        return null;
      }

      @Override protected String extractSuggestion( ValueMetaPlugin annotation ) {
        return null;
      }
    };
    assertTrue( fragmentType.isFragment() );

    IPlugin plugin = mock( IPlugin.class );
    when( plugin.getIds() ).thenReturn( new String[] { "mock" } );
    when( plugin.matches( any() ) ).thenReturn( true );
    doReturn( ValueMetaPluginType.class ).when( plugin ).getPluginType();
    doAnswer( invocationOnMock -> null ).when( plugin ).merge( any( IPlugin.class ) );

    IPlugin fragment = mock( IPlugin.class );
    when( fragment.getIds() ).thenReturn( new String[] { "mock" } );
    when( fragment.matches( any() ) ).thenReturn( true );
    doReturn( BaseFragmentType.class ).when( fragment ).getPluginType();
    doAnswer( invocationOnMock -> null ).when( fragment ).merge( any( IPlugin.class ) );

    // test
    registry.registerPlugin( ValueMetaPluginType.class, plugin );
    verify( plugin, atLeastOnce() ).merge( any() );

    registry.registerPlugin( BaseFragmentType.class, fragment );
    verify( fragment, never() ).merge( any() );
    verify( plugin, atLeast( 2 ) ).merge( any() );

    // verify that the order doesn't influence
    registry.removePlugin( ValueMetaPluginType.class, plugin );
    registry.registerPlugin( ValueMetaPluginType.class, plugin );
    verify( plugin, atLeast( 3 ) ).merge( any() );

    // verify plugin changes
    registry.registerPlugin( ValueMetaPluginType.class, plugin );
    verify( plugin, atLeast( 4 ) ).merge( any() );
  }
}
