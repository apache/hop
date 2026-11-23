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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.plugin.DataCatalogPlugin;
import org.apache.hop.datacatalog.plugin.DataCatalogPluginType;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DataCatalogTest {

  /** A catalog type that only remembers its configuration and whether it was used. */
  @DataCatalogPlugin(id = "fake", name = "Fake catalog")
  @Getter
  @Setter
  public static class FakeCatalog implements IDataCatalog, IDeprecationCapability {
    private String pluginId;
    private String pluginName;

    @HopMetadataProperty private String url;

    @HopMetadataProperty(password = true)
    private String token;

    private String resolvedUrl;
    private boolean closed;

    @Override
    public FakeCatalog clone() {
      FakeCatalog copy = new FakeCatalog();
      copy.pluginId = pluginId;
      copy.pluginName = pluginName;
      copy.url = url;
      copy.token = token;
      return copy;
    }

    @Override
    public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
        throws HopException {
      resolvedUrl = variables.resolve(url);
      if (resolvedUrl.isEmpty()) {
        throw new HopException("No URL");
      }
    }

    @Override
    public CatalogAnswer<String> ping() {
      return CatalogAnswer.known("fake at " + resolvedUrl);
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      return CatalogAnswer.notFound();
    }

    @Override
    public CatalogAnswer<Deprecation> deprecation(CatalogEntityId id) {
      return CatalogAnswer.notFound();
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @BeforeAll
  static void registerTheFakeCatalog() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.addPluginType(DataCatalogPluginType.getInstance());
    PluginRegistry.getInstance()
        .registerPluginClass(
            FakeCatalog.class.getName(), DataCatalogPluginType.class, DataCatalogPlugin.class);
  }

  private static DataCatalog catalog(String url) {
    FakeCatalog fake = new FakeCatalog();
    fake.setPluginId("fake");
    fake.setPluginName("Fake catalog");
    fake.setUrl(url);
    fake.setToken("s3cr3t-token");
    return new DataCatalog("prod", "The production catalog", fake);
  }

  @Test
  void theCatalogTypeAndItsConfigurationSurviveASaveAndLoad() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    JsonMetadataParser<DataCatalog> parser = new JsonMetadataParser<>(DataCatalog.class, provider);

    String json = parser.getJsonObject(catalog("https://catalog:8080")).toJSONString();
    assertTrue(json.contains("\"fake\""), json);
    assertFalse(json.contains("s3cr3t-token"), "the token is stored encoded: " + json);

    DataCatalog loaded;
    try (JsonParser jsonParser = new JsonFactory().createParser(json)) {
      jsonParser.nextToken();
      loaded = parser.loadJsonObject(DataCatalog.class, jsonParser);
    }
    assertEquals("The production catalog", loaded.getDescription());
    FakeCatalog fake = assertInstanceOf(FakeCatalog.class, loaded.getCatalog());
    assertEquals("fake", fake.getPluginId());
    assertEquals("https://catalog:8080", fake.getUrl());
    assertEquals("s3cr3t-token", fake.getToken());
  }

  @Test
  void connectGivesAnInitializedCopy() throws Exception {
    DataCatalog catalog = catalog("${CATALOG_URL}");
    IVariables variables = new Variables();
    variables.setVariable("CATALOG_URL", "https://catalog:8080");

    try (IDataCatalog connected = catalog.connect(variables, new MemoryMetadataProvider())) {
      assertNotSame(catalog.getCatalog(), connected);
      assertEquals(CatalogAnswer.known("fake at https://catalog:8080"), connected.ping());
      assertTrue(connected.capability(IDeprecationCapability.class).isPresent());
      assertFalse(connected.capability(IFreshnessCapability.class).isPresent());
    }
  }

  @Test
  void aCatalogThatFailsToInitializeCannotConnect() {
    DataCatalog catalog = catalog("${NOT_SET}");
    IVariables variables = new Variables();
    variables.setVariable("NOT_SET", "");

    assertThrows(
        HopException.class, () -> catalog.connect(variables, new MemoryMetadataProvider()));
  }

  @Test
  void aCatalogWithoutATypeCannotConnect() {
    DataCatalog catalog = new DataCatalog("empty", null, null);

    HopException e =
        assertThrows(
            HopException.class,
            () -> catalog.connect(new Variables(), new MemoryMetadataProvider()));
    assertTrue(e.getMessage().contains("empty"));
  }

  @Test
  void aTypeThatIsNotInstalledSaysSo() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> new IDataCatalog.DataCatalogObjectFactory().createObject("not-installed", null));
    assertTrue(e.getMessage().contains("not-installed"));
  }

  @Test
  void anUnknownAnswerKeepsItsReasonForAnotherQuestion() {
    CatalogAnswer<String> unknown =
        CatalogAnswer.unknown(CatalogAnswer.Reason.TIMEOUT, "no answer in 10s");

    CatalogAnswer<Integer> same = unknown.unknownAs();

    assertEquals(CatalogAnswer.unknown(CatalogAnswer.Reason.TIMEOUT, "no answer in 10s"), same);
  }
}
