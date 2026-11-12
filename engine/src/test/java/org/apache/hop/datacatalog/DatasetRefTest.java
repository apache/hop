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

package org.apache.hop.datacatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasetRefTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private static DatabaseMeta postgres() {
    DatabaseMeta db = mock(DatabaseMeta.class);
    when(db.getPluginId()).thenReturn("POSTGRESQL");
    when(db.getHostname()).thenReturn("${DB_HOST}");
    when(db.getPort()).thenReturn("5432");
    when(db.getDefaultDatabasePort()).thenReturn(5432);
    when(db.getDatabaseName()).thenReturn("sales");
    when(db.getPreferredSchemaName()).thenReturn("public");
    return db;
  }

  private static IVariables variables() {
    IVariables variables = new Variables();
    variables.setVariable("DB_HOST", "DB.example.com");
    return variables;
  }

  @Test
  void aTableGetsTheIdentityLineageGivesIt() {
    DatasetRef ref = DatasetRef.ofTable(postgres(), variables(), null, "orders");

    assertEquals("postgres://db.example.com:5432", ref.namespace());
    assertEquals("sales.public.orders", ref.name());
    assertTrue(ref.isComplete());
  }

  @Test
  void aSchemaOfItsOwnWinsOverThePreferredSchema() {
    DatasetRef ref = DatasetRef.ofTable(postgres(), variables(), "staging", "orders");

    assertEquals("sales.staging.orders", ref.name());
  }

  @Test
  void aLocalFileIsNamedByItsPath(@TempDir Path folder) throws Exception {
    Path file = folder.resolve("orders.csv");
    IVariables variables = new Variables();
    variables.setVariable("IN", folder.toString());

    DatasetRef ref = DatasetRef.ofFile("${IN}/orders.csv", variables);

    assertEquals("file", ref.namespace());
    assertEquals(file.toUri().getPath(), ref.name());
  }

  @Test
  void aConnectionWithoutAHostCannotBeLookedFor() {
    DatabaseMeta db = postgres();
    when(db.getHostname()).thenReturn("");

    DatasetRef ref = DatasetRef.ofTable(db, variables(), null, "orders");

    assertFalse(ref.isComplete());
  }

  @Test
  void aCatalogIdIsComplete() {
    DatasetRef ref =
        DatasetRef.ofCatalogId("urn:li:dataset:(urn:li:dataPlatform:postgres,sales.x,PROD)");

    assertTrue(ref.isComplete());
    assertEquals("urn:li:dataset:(urn:li:dataPlatform:postgres,sales.x,PROD)", ref.describe());
  }
}
