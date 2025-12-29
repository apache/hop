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

package org.apache.hop.mongo.wrapper;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultMongoClientFactoryTest {

  private DefaultMongoClientFactory factory;

  @BeforeEach
  void setUp() {
    factory = new DefaultMongoClientFactory();
  }

  @Test
  void testGetMongoClientWithSingleServer() {
    List<ServerAddress> servers = Collections.singletonList(new ServerAddress("localhost", 27017));
    List<MongoCredential> credentials = new ArrayList<>();
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    MongoClient client = factory.getMongoClient(servers, credentials, settingsBuilder, false);

    assertNotNull(client);
    client.close();
  }

  @Test
  void testGetMongoClientWithMultipleServers() {
    List<ServerAddress> servers =
        Arrays.asList(
            new ServerAddress("localhost", 27017),
            new ServerAddress("localhost", 27018),
            new ServerAddress("localhost", 27019));
    List<MongoCredential> credentials = new ArrayList<>();
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    MongoClient client = factory.getMongoClient(servers, credentials, settingsBuilder, false);

    assertNotNull(client);
    client.close();
  }

  @Test
  void testGetMongoClientWithReplicaSet() {
    List<ServerAddress> servers = Collections.singletonList(new ServerAddress("localhost", 27017));
    List<MongoCredential> credentials = new ArrayList<>();
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    MongoClient client = factory.getMongoClient(servers, credentials, settingsBuilder, true);

    assertNotNull(client);
    client.close();
  }

  @Test
  void testGetMongoClientWithCredentials() {
    List<ServerAddress> servers = Collections.singletonList(new ServerAddress("localhost", 27017));
    List<MongoCredential> credentials =
        Collections.singletonList(
            MongoCredential.createCredential("user", "admin", "password".toCharArray()));
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    MongoClient client = factory.getMongoClient(servers, credentials, settingsBuilder, false);

    assertNotNull(client);
    client.close();
  }

  @Test
  void testGetMongoClientWithEmptyCredentials() {
    List<ServerAddress> servers = Collections.singletonList(new ServerAddress("localhost", 27017));
    List<MongoCredential> credentials = new ArrayList<>();
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    MongoClient client = factory.getMongoClient(servers, credentials, settingsBuilder, false);

    assertNotNull(client);
    client.close();
  }

  @Test
  void testGetMongoClientWithNullCredentials() {
    List<ServerAddress> servers = Collections.singletonList(new ServerAddress("localhost", 27017));
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    MongoClient client = factory.getMongoClient(servers, null, settingsBuilder, false);

    assertNotNull(client);
    client.close();
  }

  @Test
  void testGetMongoClientMultipleServersNotReplicaSet() {
    // When multiple servers are provided, even without replica set flag, should use MULTIPLE mode
    List<ServerAddress> servers =
        Arrays.asList(new ServerAddress("host1", 27017), new ServerAddress("host2", 27017));
    List<MongoCredential> credentials = new ArrayList<>();
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    MongoClient client = factory.getMongoClient(servers, credentials, settingsBuilder, false);

    assertNotNull(client);
    client.close();
  }
}
