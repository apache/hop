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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatabaseMetaObjectFactoryTest {

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().searchPlugins();
  }

  @Test
  void testCreateObjectWithPluginId() throws Exception {
    DatabaseMetaObjectFactory factory = new DatabaseMetaObjectFactory();
    Object object = factory.createObject("NONE", null);
    assertNotNull(object);
    assertTrue(object instanceof IDatabase);
    IDatabase database = (IDatabase) object;
    assertEquals("NONE", database.getPluginId());
    assertEquals("No connection type", database.getPluginName());
  }

  @Test
  void testCreateObjectWithPluginName() throws Exception {
    DatabaseMetaObjectFactory factory = new DatabaseMetaObjectFactory();
    Object object = factory.createObject("No connection type", null);
    assertNotNull(object);
    assertTrue(object instanceof IDatabase);
    IDatabase database = (IDatabase) object;
    assertEquals("NONE", database.getPluginId());
    assertEquals("No connection type", database.getPluginName());
  }

  @Test
  void testCreateObjectCaseInsensitive() throws Exception {
    DatabaseMetaObjectFactory factory = new DatabaseMetaObjectFactory();
    Object object = factory.createObject("none", null);
    assertNotNull(object);
    assertTrue(object instanceof IDatabase);
    IDatabase database = (IDatabase) object;
    assertEquals("NONE", database.getPluginId());
    assertEquals("No connection type", database.getPluginName());
  }

  @Test
  void testCreateObjectNotFound() {
    DatabaseMetaObjectFactory factory = new DatabaseMetaObjectFactory();
    assertThrows(HopException.class, () -> factory.createObject("UNKNOWN_DATABASE_TYPE", null));
  }

  @Test
  void testGetObjectId() throws Exception {
    DatabaseMetaObjectFactory factory = new DatabaseMetaObjectFactory();
    NoneDatabaseMeta meta = new NoneDatabaseMeta();
    assertEquals("NONE", factory.getObjectId(meta));
    assertThrows(HopException.class, () -> factory.getObjectId("Not an IDatabase"));
  }

  @Test
  void testBaseDatabaseMetaConstructorAndLazyResolution() {
    NoneDatabaseMeta meta = new NoneDatabaseMeta();
    assertEquals("NONE", meta.getPluginId());
    assertEquals("No connection type", meta.getPluginName());

    meta.setPluginName(null);
    assertEquals("No connection type", meta.getPluginName());

    meta.setPluginId(null);
    assertEquals("NONE", meta.getPluginId());
  }

  @Test
  void testDatabaseMetaSelfHealingGetters() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    NoneDatabaseMeta noneMeta = new NoneDatabaseMeta();
    noneMeta.setPluginName(null);
    databaseMeta.setIDatabase(noneMeta);

    assertEquals("No connection type", databaseMeta.getPluginName());
    assertEquals("NONE", databaseMeta.getPluginId());
  }

  @Test
  void testDeserializationWithoutPluginName() throws Exception {
    String json =
        "{\n"
            + "  \"name\": \"test_db\",\n"
            + "  \"rdbms\": {\n"
            + "    \"NONE\": {\n"
            + "      \"databaseName\": \"test_db\",\n"
            + "      \"pluginId\": \"NONE\",\n"
            + "      \"hostname\": \"localhost\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

    JsonMetadataProvider provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            "/tmp/test-metadata",
            Variables.getADefaultVariableSpace());
    JsonMetadataParser<DatabaseMeta> parser =
        new JsonMetadataParser<>(DatabaseMeta.class, provider);

    JsonFactory jsonFactory = new JsonFactory();
    try (com.fasterxml.jackson.core.JsonParser jsonParser = jsonFactory.createParser(json)) {
      jsonParser.nextToken();
      DatabaseMeta databaseMeta = parser.loadJsonObject(DatabaseMeta.class, jsonParser);
      assertNotNull(databaseMeta);
      assertEquals("test_db", databaseMeta.getName());
      assertEquals("NONE", databaseMeta.getPluginId());
      assertEquals("No connection type", databaseMeta.getPluginName());
    }
  }
}
