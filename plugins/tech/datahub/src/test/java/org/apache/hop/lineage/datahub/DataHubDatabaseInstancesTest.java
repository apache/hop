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

package org.apache.hop.lineage.datahub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataHubDatabaseInstancesTest {

  private MemoryMetadataProvider metadata;
  private IVariables variables;

  @BeforeEach
  void setUp() throws Exception {
    metadata = new MemoryMetadataProvider();
    save("sales", "${DB_HOST}", "5432");
    save("sales-reporting", "db.example.com", "");
    save("crm", "crm.example.com", "5432");
    save("embedded", "", "");
    variables = new Variables();
    variables.setVariable("DB_HOST", "DB.example.com");
    variables.setVariable("INSTANCE", "warehouse");
  }

  private void save(String name, String host, String port) throws HopException {
    DatabaseMeta db = mock(DatabaseMeta.class);
    when(db.getName()).thenReturn(name);
    when(db.getPluginId()).thenReturn("POSTGRESQL");
    when(db.getHostname()).thenReturn(host);
    when(db.getPort()).thenReturn(port);
    when(db.getDefaultDatabasePort()).thenReturn(5432);
    metadata.getSerializer(DatabaseMeta.class).save(db);
  }

  private Map<String, String> resolve(DataHubDatabaseInstance... rows) throws HopException {
    return DataHubDatabaseInstances.resolve(
        new ArrayList<>(Arrays.asList(rows)), variables, metadata);
  }

  @Test
  void eachConnectionGivesItsServerAnInstance() throws Exception {
    Map<String, String> instances =
        resolve(
            new DataHubDatabaseInstance("sales", "${INSTANCE}"),
            new DataHubDatabaseInstance("crm", " crm "),
            new DataHubDatabaseInstance("", ""),
            null);

    assertEquals(
        Map.of(
            "postgres://db.example.com:5432",
            "warehouse",
            "postgres://crm.example.com:5432",
            "crm"),
        instances);
  }

  @Test
  void connectionsToOneServerMayRepeatItsInstanceButNotChangeIt() throws Exception {
    assertEquals(
        Map.of("postgres://db.example.com:5432", "warehouse"),
        resolve(
            new DataHubDatabaseInstance("sales", "warehouse"),
            new DataHubDatabaseInstance("sales-reporting", "warehouse")));

    assertMessage(
        "different platform instances: 'warehouse' and 'reporting'",
        new DataHubDatabaseInstance("sales", "warehouse"),
        new DataHubDatabaseInstance("sales-reporting", "reporting"));
  }

  @Test
  void aRowThatCannotNameItsTablesIsAnError() {
    assertMessage(
        "needs both a connection and an instance", new DataHubDatabaseInstance("sales", ""));
    assertMessage("needs both a connection and an instance", new DataHubDatabaseInstance("", "x"));
    assertMessage(
        "'gone' of a platform instance does not exist", new DataHubDatabaseInstance("gone", "x"));
    assertMessage("'embedded' has no host", new DataHubDatabaseInstance("embedded", "x"));
    assertMessage("contains a ',' or a '='", new DataHubDatabaseInstance("sales", "a,b"));
  }

  @Test
  void noRowsIsNoInstances() throws Exception {
    assertTrue(DataHubDatabaseInstances.resolve(null, variables, metadata).isEmpty());
    assertTrue(DataHubDatabaseInstances.resolve(List.of(), variables, metadata).isEmpty());
  }

  @Test
  void theSettingValueReadsBackTheSame() throws Exception {
    Map<String, String> instances =
        resolve(
            new DataHubDatabaseInstance("sales", "warehouse"),
            new DataHubDatabaseInstance("crm", "crm"));

    String value = DataHubDatabaseInstances.format(instances);

    assertEquals(
        "postgres://db.example.com:5432=warehouse,postgres://crm.example.com:5432=crm", value);
    assertEquals(instances, DataHubDatabaseInstances.parse(value));
    assertEquals(
        Map.of("mysql://db:3306", "a"), DataHubDatabaseInstances.parse(" mysql://db:3306 = a ,"));
    assertTrue(DataHubDatabaseInstances.parse(" ").isEmpty());
  }

  @Test
  void aSettingPairNeedsANamespaceAndAnInstance() {
    assertThrows(
        IllegalArgumentException.class, () -> DataHubDatabaseInstances.parse("postgres://db:5432"));
    assertThrows(IllegalArgumentException.class, () -> DataHubDatabaseInstances.parse("=a"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DataHubDatabaseInstances.parse("postgres://db:5432="));
  }

  private void assertMessage(String expected, DataHubDatabaseInstance... rows) {
    HopException e = assertThrows(HopException.class, () -> resolve(rows));
    assertTrue(e.getMessage().contains(expected), e.getMessage());
  }
}
