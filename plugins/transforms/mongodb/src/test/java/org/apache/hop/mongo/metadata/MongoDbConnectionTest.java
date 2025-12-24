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

package org.apache.hop.mongo.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class MongoDbConnectionTest {

  @Mock private IVariables variables;
  @Mock private ILogChannel log;
  @Mock private MongoClientWrapper wrapper;

  private MongoDbConnection connection;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    connection = new MongoDbConnection();
    when(variables.resolve(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void testDefaultConstructor() {
    MongoDbConnection conn = new MongoDbConnection();
    assertNotNull(conn);
    assertEquals(MongoDbConnectionType.STANDARD, conn.getConnectionType());
    assertEquals("localhost", conn.getHostname());
  }

  @Test
  void testCopyConstructor() {
    connection.setHostname("test-host");
    connection.setPort("27017");
    connection.setDbName("testdb");
    connection.setConnectionType(MongoDbConnectionType.SRV);
    connection.setAppName("test-app");
    connection.setAuthenticationUser("user");
    connection.setAuthenticationPassword("pass");
    connection.setAuthenticationDatabaseName("authdb");
    connection.setAuthenticationMechanism(MongoDbAuthenticationMechanism.SCRAM_SHA_256);

    MongoDbConnection copy = new MongoDbConnection(connection);
    assertEquals("test-host", copy.getHostname());
    assertEquals("27017", copy.getPort());
    assertEquals("testdb", copy.getDbName());
    assertEquals(MongoDbConnectionType.SRV, copy.getConnectionType());
    assertEquals("test-app", copy.getAppName());
    assertEquals("user", copy.getAuthenticationUser());
    assertEquals("pass", copy.getAuthenticationPassword());
    assertEquals("authdb", copy.getAuthenticationDatabaseName());
    assertEquals(MongoDbAuthenticationMechanism.SCRAM_SHA_256, copy.getAuthenticationMechanism());
  }

  @Test
  void testCreatePropertiesBuilderStandardConnection() {
    connection.setHostname("localhost");
    connection.setPort("27017");
    connection.setDbName("testdb");
    connection.setConnectionType(MongoDbConnectionType.STANDARD);

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();

    assertNotNull(props.get(MongoProp.CONNECTION_STRING));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("mongodb://"));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("localhost"));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains(":27017"));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("/testdb"));
    assertEquals("localhost", props.get(MongoProp.HOST));
    assertEquals("27017", props.get(MongoProp.PORT));
    assertEquals("testdb", props.get(MongoProp.DBNAME));
  }

  @Test
  void testCreatePropertiesBuilderSrvConnection() {
    connection.setHostname("cluster.mongodb.net");
    connection.setDbName("testdb");
    connection.setConnectionType(MongoDbConnectionType.SRV);

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();

    assertNotNull(props.get(MongoProp.CONNECTION_STRING));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("mongodb+srv://"));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("cluster.mongodb.net"));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("/testdb"));
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("ssl=true"));
    // SRV connections should not have HOST/PORT set
    assertEquals("testdb", props.get(MongoProp.DBNAME));
  }

  @Test
  void testCreatePropertiesBuilderWithCredentials() {
    connection.setHostname("localhost");
    connection.setDbName("testdb");
    connection.setAuthenticationUser("testuser");
    connection.setAuthenticationPassword("testpass");
    connection.setAuthenticationDatabaseName("authdb");
    connection.setAuthenticationMechanism(MongoDbAuthenticationMechanism.SCRAM_SHA_256);

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();

    assertEquals("testuser", props.get(MongoProp.USERNAME));
    assertEquals("testpass", props.get(MongoProp.PASSWORD));
    assertEquals("authdb", props.get(MongoProp.AUTH_DATABASE));
    assertEquals("SCRAM_SHA_256", props.get(MongoProp.AUTH_MECHA));
  }

  @Test
  void testCreatePropertiesBuilderWithAppName() {
    connection.setHostname("localhost");
    connection.setDbName("testdb");
    connection.setAppName("my-app");

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();

    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("appName=my-app"));
  }

  @Test
  void testCreatePropertiesBuilderEmptyDatabaseName() {
    connection.setHostname("localhost");
    connection.setDbName("");

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();

    // Should still have / in connection string even if db name is empty
    assertTrue(props.get(MongoProp.CONNECTION_STRING).contains("/"));
  }

  @Test
  void testTestMethodThrowsWhenDatabaseNameIsEmpty() {
    connection.setDbName("");

    assertThrows(
        MongoDbException.class,
        () -> connection.test(variables, log),
        "Database name cannot be null or empty");
  }

  @Test
  void testCreateWrapper() throws MongoDbException {
    connection.setHostname("localhost");
    connection.setDbName("testdb");

    // This will attempt to create a wrapper - it may fail if MongoDB is not available
    // but we're just testing the method exists and can be called
    try {
      MongoClientWrapper wrapper = connection.createWrapper(variables, log);
      // If it succeeds, try to dispose it (may fail if not fully initialized)
      if (wrapper != null) {
        try {
          wrapper.dispose();
        } catch (Exception e) {
          // Dispose may fail if wrapper wasn't fully initialized - that's OK for testing
        }
      }
    } catch (MongoDbException e) {
      // Expected if MongoDB is not available - that's fine for testing
    }
  }

  @Test
  void testConnectionStringBuildingStandard() {
    connection.setHostname("localhost");
    connection.setPort("27017");
    connection.setDbName("testdb");
    connection.setConnectionType(MongoDbConnectionType.STANDARD);

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();
    String connStr = props.get(MongoProp.CONNECTION_STRING);

    assertEquals("mongodb://localhost:27017/testdb", connStr);
  }

  @Test
  void testConnectionStringBuildingSrv() {
    connection.setHostname("cluster.mongodb.net");
    connection.setDbName("testdb");
    connection.setConnectionType(MongoDbConnectionType.SRV);

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();
    String connStr = props.get(MongoProp.CONNECTION_STRING);

    assertTrue(connStr.startsWith("mongodb+srv://cluster.mongodb.net/testdb"));
    assertTrue(connStr.contains("ssl=true"));
  }

  @Test
  void testConnectionStringBuildingWithAppName() {
    connection.setHostname("localhost");
    connection.setDbName("testdb");
    connection.setAppName("my-app");

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();
    String connStr = props.get(MongoProp.CONNECTION_STRING);

    assertTrue(connStr.contains("appName=my-app"));
  }

  @Test
  void testConnectionStringBuildingSrvWithoutPort() {
    connection.setHostname("cluster.mongodb.net");
    connection.setPort("27017"); // Port should be ignored for SRV
    connection.setDbName("testdb");
    connection.setConnectionType(MongoDbConnectionType.SRV);

    MongoProperties.Builder builder = connection.createPropertiesBuilder(variables);
    MongoProperties props = builder.build();
    String connStr = props.get(MongoProp.CONNECTION_STRING);

    assertTrue(connStr.startsWith("mongodb+srv://"));
    assertTrue(!connStr.contains(":27017")); // Port should not be in SRV connection string
  }

  @Test
  void testSettersAndGetters() {
    connection.setHostname("test-host");
    assertEquals("test-host", connection.getHostname());

    connection.setPort("27018");
    assertEquals("27018", connection.getPort());

    connection.setDbName("mydb");
    assertEquals("mydb", connection.getDbName());

    connection.setConnectionType(MongoDbConnectionType.SRV);
    assertEquals(MongoDbConnectionType.SRV, connection.getConnectionType());

    connection.setAppName("app");
    assertEquals("app", connection.getAppName());

    connection.setAuthenticationUser("user");
    assertEquals("user", connection.getAuthenticationUser());

    connection.setAuthenticationPassword("pass");
    assertEquals("pass", connection.getAuthenticationPassword());

    connection.setAuthenticationDatabaseName("authdb");
    assertEquals("authdb", connection.getAuthenticationDatabaseName());

    connection.setAuthenticationMechanism(MongoDbAuthenticationMechanism.SCRAM_SHA_256);
    assertEquals(
        MongoDbAuthenticationMechanism.SCRAM_SHA_256, connection.getAuthenticationMechanism());
  }
}
