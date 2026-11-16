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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.lineage.openlineage.OpenLineageSinkConfig;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataHubSinkConfigurationTest {

  private MemoryMetadataProvider metadata;
  private IVariables variables;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    DataHubDataCatalog dataHub = new DataHubDataCatalog();
    dataHub.setPluginId("datahub");
    dataHub.setPluginName("DataHub");
    dataHub.setUrl("${DATAHUB_URL}");
    dataHub.setToken("${DATAHUB_TOKEN}");
    dataHub.setEnvironment("dev");
    dataHub.setTrustStorePath("/etc/hop/catalog-truststore.p12");
    metadata = new MemoryMetadataProvider();
    metadata.getSerializer(DataCatalog.class).save(new DataCatalog("datahub", null, dataHub));
    metadata
        .getSerializer(DataCatalog.class)
        .save(new DataCatalog("other", null, new NotDataHub()));
    variables = new Variables();
    variables.setVariable("DATAHUB_URL", "http://datahub-gms:8080/");
    variables.setVariable("DATAHUB_TOKEN", "s3cr3t");
  }

  private static IVariables variablesOf(Map<String, String> settings) {
    Variables result = new Variables();
    settings.forEach(result::setVariable);
    return result;
  }

  @Test
  void whereDataHubIsComesFromTheDataCatalog() throws Exception {
    DataHubSinkConfiguration configuration = new DataHubSinkConfiguration();
    configuration.setDataCatalog("datahub");
    configuration.setPlatformInstance("hop-dev");
    configuration.setDatasetSchema(DataHubSinkConfig.SchemaPolicy.NEVER);
    configuration.setLifecycle(true);
    configuration.setSpoolDirectory("/var/spool/hop-datahub");

    Map<String, String> settings = configuration.toSettings(variables, metadata);
    DataHubSinkConfig config = DataHubSinkConfig.fromVariables(variablesOf(settings));
    OpenLineageSinkConfig delivery = config.toOpenLineageConfig();

    assertEquals("http://datahub-gms:8080", config.getGmsUrl());
    assertEquals("s3cr3t", config.getToken());
    assertEquals("DEV", config.getEnv());
    assertEquals("hop-dev", config.getPlatformInstance());
    assertEquals(DataHubSinkConfig.SchemaPolicy.NEVER, config.getSchemaPolicy());
    assertTrue(config.isLifecycleEnabled());
    assertEquals("urn:li:corpuser:datahub", config.getActor());
    assertEquals("/var/spool/hop-datahub", delivery.getSpoolDirectory());
    assertEquals("/etc/hop/catalog-truststore.p12", delivery.getTrustStorePath());
    assertTrue(delivery.isSqlParseEnabled());
  }

  @Test
  void theSinksOwnTrustStoreWinsOverTheCatalogs() throws Exception {
    DataHubSinkConfiguration configuration = new DataHubSinkConfiguration();
    configuration.setDataCatalog("datahub");
    configuration.setTrustStorePath("/etc/hop/sink-truststore.p12");

    Map<String, String> settings = configuration.toSettings(variables, metadata);

    assertEquals(
        "/etc/hop/sink-truststore.p12",
        DataHubSinkConfig.fromVariables(variablesOf(settings))
            .toOpenLineageConfig()
            .getTrustStorePath());
  }

  @Test
  void theDataCatalogMustBeADataHubOne() {
    DataHubSinkConfiguration configuration = new DataHubSinkConfiguration();

    assertMessage(configuration, "needs a DataHub data catalog");
    configuration.setDataCatalog("gone");
    assertMessage(configuration, "'gone' does not exist");
    configuration.setDataCatalog("other");
    assertMessage(configuration, "'other' is not a DataHub catalog");
    configuration.setDataCatalog("datahub");
    variables.setVariable("DATAHUB_URL", "");
    assertMessage(configuration, "'datahub' has no DataHub URL");
  }

  // The catalog's platform instances reach the sink, so it writes the URNs the catalog reads.
  @Test
  void theDatabasePlatformInstancesComeFromTheDataCatalog() throws Exception {
    DatabaseMeta sales = mock(DatabaseMeta.class);
    when(sales.getName()).thenReturn("sales");
    when(sales.getPluginId()).thenReturn("POSTGRESQL");
    when(sales.getHostname()).thenReturn("db");
    when(sales.getPort()).thenReturn("5432");
    metadata.getSerializer(DatabaseMeta.class).save(sales);
    DataHubDataCatalog dataHub =
        (DataHubDataCatalog) metadata.getSerializer(DataCatalog.class).load("datahub").getCatalog();
    DataHubSinkConfiguration configuration = new DataHubSinkConfiguration();
    configuration.setDataCatalog("datahub");

    Map<String, String> settings = configuration.toSettings(variables, metadata);
    assertFalse(settings.containsKey(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_DATABASE_INSTANCES));

    dataHub.getDatabaseInstances().add(new DataHubDatabaseInstance("sales", "warehouse"));
    settings = configuration.toSettings(variables, metadata);
    assertEquals(
        Map.of("postgres://db:5432", "warehouse"),
        DataHubSinkConfig.fromVariables(variablesOf(settings)).getDatabaseInstances());

    dataHub.getDatabaseInstances().add(new DataHubDatabaseInstance("gone", "elsewhere"));
    assertMessage(configuration, "'gone' of a platform instance does not exist");
  }

  @Test
  void theSinkHandsOutADataHubConfiguration() {
    assertInstanceOf(DataHubSinkConfiguration.class, new DataHubLineageSink().newConfiguration());
  }

  private void assertMessage(DataHubSinkConfiguration configuration, String expected) {
    HopException e =
        assertThrows(HopException.class, () -> configuration.toSettings(variables, metadata));
    assertTrue(e.getMessage().contains(expected), e.getMessage());
  }

  /** A data catalog of another type. */
  public static class NotDataHub implements IDataCatalog {
    @Override
    public String getPluginId() {
      return "other";
    }

    @Override
    public void setPluginId(String pluginId) {
      // fixed
    }

    @Override
    public String getPluginName() {
      return "Other";
    }

    @Override
    public void setPluginName(String pluginName) {
      // fixed
    }

    @Override
    public IDataCatalog clone() {
      return this;
    }

    @Override
    public void initialize(IVariables variables, IHopMetadataProvider metadataProvider) {
      // nothing to prepare
    }

    @Override
    public CatalogAnswer<String> ping() {
      return CatalogAnswer.notFound();
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      return CatalogAnswer.notFound();
    }

    @Override
    public void close() {
      // nothing to release
    }
  }
}
