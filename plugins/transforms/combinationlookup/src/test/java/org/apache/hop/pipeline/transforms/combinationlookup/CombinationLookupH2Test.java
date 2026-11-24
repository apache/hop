/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.combinationlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import org.apache.hop.core.Counters;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Combination lookup/update against H2, end to end: a new combination of key values gets the next
 * technical key, a known one is looked up, with and without the cache.
 */
class CombinationLookupH2Test {

  private static final String CONNECTION = "h2";

  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  @BeforeEach
  void setUp() throws Exception {
    String databaseName =
        "mem:combi" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    // The "table maximum" key counters are kept per table for the whole JVM.
    Counters.getInstance().clear();
    databaseMeta = new DatabaseMeta(CONNECTION, "H2", "Native", "", databaseName, "", "sa", "");
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      db.execStatement(
          "CREATE TABLE d_combo (tk BIGINT PRIMARY KEY, country VARCHAR(10), status VARCHAR(10))");
      db.execStatement("INSERT INTO d_combo VALUES (5, 'FR', 'open')");
      db.commit(true);
    }
  }

  private List<String> table() throws Exception {
    List<String> content = new ArrayList<>();
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      for (Object[] row :
          db.getRows("SELECT tk, country, status FROM d_combo WHERE tk > 0 ORDER BY tk", -1)) {
        content.add(row[0] + ":" + row[1] + ":" + row[2]);
      }
    }
    return content;
  }

  private CombinationLookup combination(
      int cacheSize, boolean preload, Queue<Object[]> input, List<Object[]> output)
      throws Exception {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("country"));
    inputRowMeta.addValueMeta(new ValueMetaString("status"));

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    CombinationLookupMeta meta = new CombinationLookupMeta();
    meta.setDefault();
    meta.setConnectionName(CONNECTION);
    meta.setSchemaName("");
    meta.setTableName("d_combo");
    meta.setCacheSize(cacheSize);
    meta.setPreloadCache(preload);
    meta.getFields().getKeyFields().add(new KeyField("country", "country"));
    meta.getFields().getKeyFields().add(new KeyField("status", "status"));
    meta.getFields().getReturnFields().setTechnicalKeyField("tk");
    meta.getFields()
        .getReturnFields()
        .setTechKeyCreation(CombinationLookupMeta.CREATION_METHOD_TABLEMAX);

    TransformMeta transformMeta = new TransformMeta("combination", meta);
    pipelineMeta.addTransform(transformMeta);
    return new CombinationLookup(
        transformMeta,
        meta,
        new CombinationLookupData(),
        0,
        pipelineMeta,
        new LocalPipelineEngine()) {
      @Override
      public IRowMeta getInputRowMeta() {
        return inputRowMeta;
      }

      @Override
      public Object[] getRow() {
        Object[] row = input.poll();
        return row == null ? null : row.clone();
      }

      @Override
      public void putRow(IRowMeta rowMeta, Object[] row) {
        output.add(row);
      }
    };
  }

  private static void run(CombinationLookup transform) throws Exception {
    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }
  }

  /**
   * @param cacheSize 0 caches every combination, -1 caches nothing
   */
  @ParameterizedTest(name = "cache size {0}, preload {1}")
  @CsvSource({"0, false", "-1, false", "0, true"})
  void newCombinationsGetTheNextKeyKnownOnesAreLookedUp(int cacheSize, boolean preload)
      throws Exception {
    Queue<Object[]> input = new ArrayDeque<>();
    input.add(new Object[] {"BE", "open"});
    input.add(new Object[] {"FR", "open"});
    input.add(new Object[] {"NL", "open"});
    input.add(new Object[] {"BE", "open"});
    input.add(new Object[] {"BE", "closed"});
    input.add(new Object[] {"NL", "open"});
    List<Object[]> output = new ArrayList<>();

    run(combination(cacheSize, preload, input, output));

    List<Object> keys = new ArrayList<>();
    for (Object[] row : output) {
      keys.add(row[2]);
    }
    assertEquals(List.of(6L, 5L, 7L, 6L, 8L, 7L), keys);
    assertEquals(List.of("5:FR:open", "6:BE:open", "7:NL:open", "8:BE:closed"), table());
  }
}
