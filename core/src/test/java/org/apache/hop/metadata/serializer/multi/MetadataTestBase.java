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
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Ignore
public class MetadataTestBase extends TestCase {
  protected MultiMetadataProvider multiMetadataProvider;
  protected MemoryMetadataProvider provider1;
  protected MemoryMetadataProvider provider2;
  protected MemoryMetadataProvider provider3;

  @Override protected void setUp() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginType( MetadataPluginType.class);

    registry.registerPluginClass(
      MetadataType1.class.getName(), MetadataPluginType.class, HopMetadata.class);
    assertNotNull(registry.findPluginWithId(MetadataPluginType.class, "type-1"));
    registry.registerPluginClass(
      MetadataType2.class.getName(), MetadataPluginType.class, HopMetadata.class);
    assertNotNull(registry.findPluginWithId(MetadataPluginType.class, "type-2"));

    IVariables variables = Variables.getADefaultVariableSpace();
    ITwoWayPasswordEncoder twoWayPasswordEncoder = new HopTwoWayPasswordEncoder();

    provider1 = new MemoryMetadataProvider(twoWayPasswordEncoder, variables);
    provider1.setDescription("Provider1");
    provider2 = new MemoryMetadataProvider(twoWayPasswordEncoder, variables);
    provider2.setDescription("Provider2");
    provider3 = new MemoryMetadataProvider(twoWayPasswordEncoder, variables);
    provider3.setDescription("Provider3");

    List<IHopMetadataProvider> providers =
      new ArrayList<>( Arrays.asList(provider1, provider2, provider3));

    multiMetadataProvider = new MultiMetadataProvider(twoWayPasswordEncoder, providers, variables);
  }
}
