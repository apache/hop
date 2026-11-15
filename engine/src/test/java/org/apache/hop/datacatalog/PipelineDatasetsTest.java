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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.PipelineDatasets.TouchedDataset;
import org.apache.hop.lineage.api.IDeclaresLineage;
import org.apache.hop.lineage.api.RelationalLineage;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The datasets a pipeline declares, named for a catalog. */
class PipelineDatasetsTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private static TransformMeta transform(String name, IDeclaresLineage.Declaration... declared) {
    ITransformMeta meta =
        mock(ITransformMeta.class, withSettings().extraInterfaces(IDeclaresLineage.class));
    when(((IDeclaresLineage) meta).declaredLineage(any(), any())).thenReturn(List.of(declared));
    TransformMeta transformMeta = new TransformMeta(name, name, meta);
    transformMeta.setTransformPluginId("Fake");
    return transformMeta;
  }

  /** A transform that declares the table it writes, as Table output does. */
  @RelationalLineage(operation = RelationalIoOperation.WRITE)
  static class TableWriteMeta extends BaseTransformMeta<ITransform, ITransformData> {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "sales";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";
  }

  private static DatabaseMeta postgres() {
    DatabaseMeta db = mock(DatabaseMeta.class);
    when(db.getName()).thenReturn("sales");
    when(db.getPluginId()).thenReturn("POSTGRESQL");
    when(db.getHostname()).thenReturn("db.example.com");
    when(db.getPort()).thenReturn("5432");
    when(db.getDefaultDatabasePort()).thenReturn(5432);
    when(db.getDatabaseName()).thenReturn("sales");
    when(db.getPreferredSchemaName()).thenReturn("public");
    return db;
  }

  @Test
  void eachFileAndTableOnceWithTheTransformsThatReadAndWriteIt() {
    IVariables variables = new Variables();
    variables.setVariable("OUT", "/data/out");
    PipelineMeta meta = spy(new PipelineMeta());
    meta.setName("orders");
    doReturn(postgres()).when(meta).findDatabase(eq("sales"), any(IVariables.class));
    meta.addTransform(
        transform(
            "Read",
            new IDeclaresLineage.DeclaredFile(FileIoOperation.READ, "/data/in.csv", null),
            new IDeclaresLineage.DeclaredSql(
                RelationalIoOperation.READ, "sales", "SELECT id FROM orders")));
    meta.addTransform(
        transform(
            "Write",
            new IDeclaresLineage.DeclaredFile(FileIoOperation.WRITE, "${OUT}/orders.csv", null)));
    TransformMeta load = new TransformMeta("Load", new TableWriteMeta());
    load.setTransformPluginId("TableOutput");
    meta.addTransform(load);
    meta.addTransform(
        transform(
            "Check",
            new IDeclaresLineage.DeclaredFile(FileIoOperation.READ, "${OUT}/orders.csv", null),
            new IDeclaresLineage.DeclaredSql(
                RelationalIoOperation.READ, "no such connection", "SELECT 1")));

    List<TouchedDataset> datasets = PipelineDatasets.of(meta, variables);

    // The tables in SQL are only known once the SQL is parsed, which the OpenLineage plugin does
    assertEquals(3, datasets.size(), datasets.toString());
    assertEquals(DatasetRef.of("file", "/data/in.csv"), datasets.get(0).dataset());
    assertEquals(Set.of("Read"), datasets.get(0).readBy());
    TouchedDataset orders = datasets.get(1);
    assertEquals(DatasetRef.of("file", "/data/out/orders.csv"), orders.dataset());
    assertEquals(Set.of("Write"), orders.writtenBy());
    assertEquals(Set.of("Check"), orders.readBy());
    assertEquals(
        DatasetRef.of("postgres://db.example.com:5432", "sales.public.orders"),
        datasets.get(2).dataset());
    assertEquals(Set.of("Load"), datasets.get(2).writtenBy());
  }

  @Test
  void aPipelineThatDeclaresNothingHasNoDatasets() {
    PipelineMeta meta = new PipelineMeta();
    meta.addTransform(transform("Nothing"));

    assertEquals(List.of(), PipelineDatasets.of(meta, new Variables()));
  }
}
