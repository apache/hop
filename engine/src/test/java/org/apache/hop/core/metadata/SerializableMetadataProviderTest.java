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

package org.apache.hop.core.metadata;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.server.HopServer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SerializableMetadataProviderTest {

  @Before
  public void before() throws Exception {
    HopEnvironment.init();
  }

  @Test
  public void testRoundTrip() throws Exception {
    MemoryMetadataProvider source = new MemoryMetadataProvider();
    IHopMetadataSerializer<HopServer> sourceSerializer = source.getSerializer( HopServer.class );
    HopServer sourceServer1 = new HopServer("server1", "hostname1", "8181", "username1", "password1", null, null, null, false);
    sourceSerializer.save( sourceServer1 );
    HopServer sourceServer2 = new HopServer("server2", "hostname2", "8282", "username2", "password2", null, null, null, true);
    sourceSerializer.save( sourceServer2 );


    SerializableMetadataProvider serializableMetadataProvider = new SerializableMetadataProvider( source );
    String json = serializableMetadataProvider.toJson();
    SerializableMetadataProvider target = new SerializableMetadataProvider( json );
    IHopMetadataSerializer<HopServer> targetSerializer = target.getSerializer( HopServer.class );
    HopServer targetServer1 = targetSerializer.load( "server1" );
    assertNotNull( targetServer1 );
    assertEquals(sourceServer1.getName(), targetServer1.getName());
    assertEquals(sourceServer1.getHostname(), targetServer1.getHostname());
    assertEquals(sourceServer1.getPort(), targetServer1.getPort());
    assertEquals(sourceServer1.getUsername(), targetServer1.getUsername());
    assertEquals(sourceServer1.getPassword(), targetServer1.getPassword());

    HopServer targetServer2 = targetSerializer.load( "server2" );
    assertNotNull( targetServer2 );
    assertEquals(sourceServer2.getName(), targetServer2.getName());
    assertEquals(sourceServer2.getHostname(), targetServer2.getHostname());
    assertEquals(sourceServer2.getPort(), targetServer2.getPort());
    assertEquals(sourceServer2.getUsername(), targetServer2.getUsername());
    assertEquals(sourceServer2.getPassword(), targetServer2.getPassword());

  }

}