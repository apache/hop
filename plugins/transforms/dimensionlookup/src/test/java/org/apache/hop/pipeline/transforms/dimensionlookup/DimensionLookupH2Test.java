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
package org.apache.hop.pipeline.transforms.dimensionlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLField;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLKey;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Dimension lookup/update against H2, end to end: new keys get a first version, a changed attribute
 * a new version, and a lookup-only run finds the version valid at the stream date, with and without
 * the cache.
 */
class DimensionLookupH2Test {

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
        "mem:dim" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    // The "table maximum" key counters are kept per table for the whole JVM.
    Counters.getInstance().clear();
    databaseMeta = new DatabaseMeta(CONNECTION, "H2", "Native", "", databaseName, "", "sa", "");
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      db.execStatement(
          "CREATE TABLE d_customer (tk BIGINT PRIMARY KEY, version INTEGER,"
              + " date_from TIMESTAMP, date_to TIMESTAMP, cust_id BIGINT, name VARCHAR(10))");
      db.commit(true);
    }
  }

  private static Date date(int year, int month, int day) {
    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(year, month - 1, day);
    return calendar.getTime();
  }

  private List<String> table() throws Exception {
    List<String> content = new ArrayList<>();
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      for (Object[] row :
          db.getRows(
              "SELECT tk, version, cust_id, name FROM d_customer WHERE tk > 0 ORDER BY tk", -1)) {
        content.add(row[0] + ":" + row[1] + ":" + row[2] + ":" + row[3]);
      }
    }
    return content;
  }

  private DimensionLookup dimension(
      boolean update, int cacheSize, boolean preload, Queue<Object[]> input, List<Object[]> output)
      throws Exception {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("cust_id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaDate("updated"));

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    DimensionLookupMeta meta = new DimensionLookupMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setSchemaName("");
    meta.setTableName("d_customer");
    meta.setUpdate(update);
    meta.setCommitSize(100);
    meta.setCacheSize(cacheSize);
    meta.setPreloadingCache(preload);
    DLKey key = new DLKey();
    key.setName("cust_id");
    key.setLookup("cust_id");
    meta.getFields().getKeys().add(key);
    if (update) {
      DLField field = new DLField();
      field.setName("name");
      field.setLookup("name");
      field.setUpdate(DimensionLookupMeta.DimensionUpdateType.INSERT.getCode());
      meta.getFields().getFields().add(field);
    }
    meta.getFields().getDate().setName("updated");
    meta.getFields().getReturns().setKeyField("tk");
    meta.getFields().getReturns().setCreationMethod(TechnicalKeyCreationMethod.TABLE_MAXIMUM);
    meta.getFields().getReturns().setVersionField("version");

    TransformMeta transformMeta = new TransformMeta("dimension", meta);
    pipelineMeta.addTransform(transformMeta);
    return new DimensionLookup(
        transformMeta,
        meta,
        new DimensionLookupData(),
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

  private static void run(DimensionLookup transform) throws Exception {
    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }
  }

  private static List<Object> keys(List<Object[]> output) {
    List<Object> keys = new ArrayList<>();
    for (Object[] row : output) {
      keys.add(row[3]);
    }
    return keys;
  }

  /** Loads two versions of customer 1 and one of customer 2. */
  private void load(int cacheSize) throws Exception {
    Queue<Object[]> input = new ArrayDeque<>();
    input.add(new Object[] {1L, "a", date(2020, 1, 1)});
    input.add(new Object[] {2L, "b", date(2020, 1, 1)});
    input.add(new Object[] {1L, "a", date(2020, 6, 1)}); // unchanged
    input.add(new Object[] {1L, "z", date(2021, 1, 1)}); // new version
    input.add(new Object[] {2L, "b", date(2021, 1, 1)}); // unchanged
    List<Object[]> output = new ArrayList<>();
    run(dimension(true, cacheSize, false, input, output));
    assertEquals(List.of(1L, 2L, 1L, 3L, 2L), keys(output));
  }

  /**
   * @param cacheSize 0 caches every row, -1 caches nothing
   */
  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void updateAddsVersionsForChangedAttributesOnly(int cacheSize) throws Exception {
    load(cacheSize);
    assertEquals(List.of("1:1:1:a", "2:1:2:b", "3:2:1:z"), table());
  }

  @ParameterizedTest(name = "cache size {0}, preload {1}")
  @CsvSource({"0, false", "-1, false", "0, true"})
  void lookupFindsTheVersionValidAtTheStreamDate(int cacheSize, boolean preload) throws Exception {
    load(-1);
    Queue<Object[]> input = new ArrayDeque<>();
    input.add(new Object[] {1L, null, date(2020, 3, 1)});
    input.add(new Object[] {1L, null, date(2022, 3, 1)});
    input.add(new Object[] {2L, null, date(2022, 3, 1)});
    input.add(new Object[] {9L, null, date(2022, 3, 1)});
    input.add(new Object[] {1L, null, date(2020, 3, 1)});
    List<Object[]> output = new ArrayList<>();

    run(dimension(false, cacheSize, preload, input, output));

    assertEquals(List.of(1L, 3L, 2L, 0L, 1L), keys(output));
  }
}
