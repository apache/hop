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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSchemasTest {

  private static final CatalogField ID =
      new CatalogField("id", "INTEGER", "NUMBER", false, Set.of(), Set.of());

  private static final CatalogDataset DATASET =
      new CatalogDataset(CatalogDatasetType.CATALOG_ID, null, null, "urn:${TABLE}");

  private MemoryMetadataProvider provider;
  private Variables variables;

  @BeforeEach
  void setUp() throws Exception {
    SchemaCatalog.resolved = CatalogAnswer.known(new CatalogEntityId("urn:customers"));
    SchemaCatalog.schema = CatalogAnswer.known(List.of(ID));
    SchemaCatalog.asked = null;
    provider = new MemoryMetadataProvider();
    provider
        .getSerializer(DataCatalog.class)
        .save(new DataCatalog("catalog", null, new SchemaCatalog()));
    provider
        .getSerializer(DataCatalog.class)
        .save(new DataCatalog("plain", null, new PlainCatalog()));
    variables = new Variables();
    variables.setVariable("CATALOG", "catalog");
    variables.setVariable("TABLE", "customers");
  }

  @Test
  void theFieldsOfTheDatasetAreRead() throws Exception {
    List<CatalogField> fields = CatalogSchemas.read(variables, provider, "${CATALOG}", DATASET);

    assertEquals(List.of(ID), fields);
    assertEquals("urn:customers", SchemaCatalog.asked.catalogId());
  }

  @Test
  void aDatasetTheCatalogDoesNotHaveIsAnError() {
    SchemaCatalog.resolved = CatalogAnswer.notFound();

    HopException e =
        assertThrows(
            HopException.class, () -> CatalogSchemas.read(variables, provider, "catalog", DATASET));
    assertTrue(e.getMessage().contains("has no dataset"), e.getMessage());
  }

  @Test
  void aCatalogThatCannotAnswerSaysWhy() {
    SchemaCatalog.schema = CatalogAnswer.unknown(CatalogAnswer.Reason.UNAUTHORIZED, "bad token");

    HopException e =
        assertThrows(
            HopException.class, () -> CatalogSchemas.read(variables, provider, "catalog", DATASET));
    assertTrue(e.getMessage().contains("UNAUTHORIZED: bad token"), e.getMessage());
  }

  @Test
  void aCatalogWithoutSchemasOrAMissingCatalogIsAnError() {
    assertThrows(
        HopException.class, () -> CatalogSchemas.read(variables, provider, "plain", DATASET));
    assertThrows(
        HopException.class, () -> CatalogSchemas.read(variables, provider, "nope", DATASET));
    assertThrows(HopException.class, () -> CatalogSchemas.read(variables, provider, "", DATASET));
  }

  /** A data catalog without a schema capability. */
  public static class PlainCatalog implements IDataCatalog {
    private String pluginId = "plain";
    private String pluginName = "Plain catalog";

    @Override
    public String getPluginId() {
      return pluginId;
    }

    @Override
    public void setPluginId(String pluginId) {
      this.pluginId = pluginId;
    }

    @Override
    public String getPluginName() {
      return pluginName;
    }

    @Override
    public void setPluginName(String pluginName) {
      this.pluginName = pluginName;
    }

    @Override
    public PlainCatalog clone() {
      return new PlainCatalog();
    }

    @Override
    public void initialize(IVariables variables, IHopMetadataProvider metadataProvider) {
      // Nothing to prepare
    }

    @Override
    public CatalogAnswer<String> ping() {
      return CatalogAnswer.known("plain");
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      return CatalogAnswer.known(new CatalogEntityId(dataset.catalogId()));
    }

    @Override
    public void close() {
      // Nothing to release
    }
  }

  /** A data catalog that answers with the schema each test sets. */
  public static class SchemaCatalog extends PlainCatalog implements ISchemaCapability {
    static CatalogAnswer<CatalogEntityId> resolved;
    static CatalogAnswer<List<CatalogField>> schema;
    static DatasetRef asked;

    @Override
    public SchemaCatalog clone() {
      return new SchemaCatalog();
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      asked = dataset;
      return resolved;
    }

    @Override
    public CatalogAnswer<List<CatalogField>> schema(CatalogEntityId id) {
      return schema;
    }
  }
}
