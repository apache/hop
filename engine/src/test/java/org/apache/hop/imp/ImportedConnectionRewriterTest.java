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
package org.apache.hop.imp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ImportedConnectionRewriterTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  static class SampleMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "DATABASE";

    @HopMetadataProperty(key = "other")
    String other = "leave";
  }

  @Test
  void rewriteObjectUsesTheNameMap() {
    SampleMeta meta = new SampleMeta();
    ConnectionNameMap map =
        ConnectionNameMap.build(List.of("Database", "DATABASE"), List.of("Database"), null);

    int changed = ImportedConnectionRewriter.rewriteObject(meta, map);

    assertEquals(1, changed);
    assertEquals("Database", meta.connection);
    assertEquals("leave", meta.other);
  }

  @Test
  void rewriteRenamesCaseOnlyConnectionMetadata() throws Exception {
    MemoryMetadataProvider memory = new MemoryMetadataProvider();
    IHopMetadataSerializer<DatabaseMeta> serializer = memory.getSerializer(DatabaseMeta.class);
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("sales");
    serializer.save(databaseMeta);

    TestImport hopImport = new TestImport();
    hopImport.setMetadataProvider(
        new MultiMetadataProvider(null, List.of(memory), hopImport.getVariables()));
    hopImport.getConnectionsList().add(databaseMeta);
    hopImport.getSharedConnectionNames().add("Sales");

    ImportedConnectionRewriter.Result result = ImportedConnectionRewriter.rewrite(hopImport);

    assertEquals(1, result.getConnectionsRenamed());
    assertNull(serializer.load("sales"));
    assertEquals("Sales", serializer.load("Sales").getName());
  }

  @Test
  void rewriteSkipsWorkWhenNamesAlreadyMatch() throws Exception {
    MemoryMetadataProvider memory = new MemoryMetadataProvider();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("Sales");
    memory.getSerializer(DatabaseMeta.class).save(databaseMeta);

    TestImport hopImport = new TestImport();
    hopImport.setMetadataProvider(
        new MultiMetadataProvider(null, List.of(memory), hopImport.getVariables()));
    hopImport.getConnectionsList().add(databaseMeta);

    ImportedConnectionRewriter.Result result = ImportedConnectionRewriter.rewrite(hopImport);

    assertEquals(0, result.getConnectionsRenamed());
    assertEquals("Sales", memory.getSerializer(DatabaseMeta.class).load("Sales").getName());
  }

  private static final class TestImport extends HopImportBase {
    @Override
    public void importFiles() {}

    @Override
    public void findFilesToImport() {}

    @Override
    public void importConnections() {}

    @Override
    public void importVariables() {}

    @Override
    public String getImportReport() {
      return "";
    }
  }
}
