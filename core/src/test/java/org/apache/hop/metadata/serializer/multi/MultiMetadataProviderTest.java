/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.metadata.serializer.multi;

import junit.framework.TestCase;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiMetadataProviderTest extends MetadataTestBase {

  public void testGetDescription() {
    String description = multiMetadataProvider.getDescription();
    assertNotNull(description);
    assertEquals("Multi Metadata Provider: Provider1, Provider2, Provider3", description);
  }

  public void testGetMetadataClasses() {
    List<Class<IHopMetadata>> list = multiMetadataProvider.getMetadataClasses();
    assertEquals( 2, list.size() );
    assertTrue( list.contains( MetadataType1.class ));
    assertTrue( list.contains( MetadataType2.class ));
  }

  public void testGetMetadataClassForKey() throws HopException {
    assertEquals( MetadataType1.class, multiMetadataProvider.getMetadataClassForKey( "type-1" ) );
    assertEquals( MetadataType2.class, multiMetadataProvider.getMetadataClassForKey( "type-2" ) );
  }

  public void testFindProvider() {
    IHopMetadataProvider look1 = multiMetadataProvider.findProvider("Provider1");
    assertNotNull(look1);
    assertEquals(provider1, look1);
    IHopMetadataProvider look2 = multiMetadataProvider.findProvider("Provider2");
    assertNotNull(look2);
    assertEquals(provider2, look2);
    IHopMetadataProvider look3 = multiMetadataProvider.findProvider("Provider3");
    assertNotNull(look3);
    assertEquals(provider3, look3);
  }

  public void testGetProviders() {
    List<IHopMetadataProvider> list = multiMetadataProvider.getProviders();
    assertEquals(3, list.size());
  }

  public void testSetProviders() {
    List<IHopMetadataProvider> list = multiMetadataProvider.getProviders();
    MemoryMetadataProvider provider4 = new MemoryMetadataProvider( new HopTwoWayPasswordEncoder(), Variables.getADefaultVariableSpace() );
    provider4.setDescription("Provider4");
    list.add(provider4);
    multiMetadataProvider.setProviders( list );
    assertEquals( 4, multiMetadataProvider.getProviders().size() );
    // see if the description has changed...
    //
    assertEquals("Multi Metadata Provider: Provider1, Provider2, Provider3, Provider4", multiMetadataProvider.getDescription());
  }
}
