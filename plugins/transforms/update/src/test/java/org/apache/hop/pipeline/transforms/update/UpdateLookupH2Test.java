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
package org.apache.hop.pipeline.transforms.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Update without batching against H2: every row looks up the current values of the update fields
 * and is only updated when one of them differs, compared with the layout the lookup reports.
 */
class UpdateLookupH2Test {

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
        "mem:updlookup" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    databaseMeta = new DatabaseMeta(CONNECTION, "H2", "Native", "", databaseName, "", "sa", "");
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      db.execStatement(
          "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(10), amount DOUBLE,"
              + " born DATE)");
      db.execStatement("INSERT INTO target VALUES (1, 'one', 1.5, DATE '2020-01-15')");
      db.execStatement("INSERT INTO target VALUES (2, 'two', 2.5, DATE '2021-06-30')");
      db.execStatement("INSERT INTO target VALUES (3, 'three', 3.5, NULL)");
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
      for (Object[] row : db.getRows("SELECT id, name, amount, born FROM target ORDER BY id", -1)) {
        content.add(row[0] + ":" + row[1] + ":" + row[2] + ":" + (row[3] == null ? "-" : "d"));
      }
    }
    return content;
  }

  private Update update(Queue<Object[]> input, List<Object[]> output) throws Exception {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaNumber("amount"));
    inputRowMeta.addValueMeta(new ValueMetaDate("born"));

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    UpdateMeta meta = new UpdateMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setCommitSize("100");
    meta.setUseBatchUpdate(false);
    meta.setErrorIgnored(true);
    meta.getLookupField().setSchemaName("");
    meta.getLookupField().setTableName("target");
    meta.getLookupField().getLookupKeys().add(new UpdateKeyField("id", "id", "="));
    meta.getLookupField().getUpdateFields().add(new UpdateField("name", "name"));
    meta.getLookupField().getUpdateFields().add(new UpdateField("amount", "amount"));
    meta.getLookupField().getUpdateFields().add(new UpdateField("born", "born"));
    TransformMeta transformMeta = new TransformMeta("update", meta);
    pipelineMeta.addTransform(transformMeta);

    return new Update(
        transformMeta, meta, new UpdateData(), 0, pipelineMeta, new LocalPipelineEngine()) {
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

  private static void run(Update update) throws Exception {
    assertTrue(update.init());
    try {
      while (update.processRow()) {
        // keep going
      }
    } finally {
      update.dispose();
    }
  }

  @Test
  void onlyRowsWithAChangedValueAreUpdated() throws Exception {
    Queue<Object[]> input = new ArrayDeque<>();
    input.add(new Object[] {1L, "one", 1.5, date(2020, 1, 15)}); // unchanged
    input.add(new Object[] {2L, "TWO", 2.5, date(2021, 6, 30)}); // name changed
    input.add(new Object[] {3L, "three", 3.5, null}); // unchanged, null date
    input.add(new Object[] {9L, "nine", 9.0, null}); // not found, ignored
    input.add(new Object[] {1L, "one", 1.75, date(2020, 1, 15)}); // amount changed
    input.add(new Object[] {3L, "three", 3.5, date(2022, 2, 2)}); // date set
    List<Object[]> output = new ArrayList<>();

    Update update = update(input, output);
    run(update);

    assertEquals(6, output.size());
    assertEquals(3, update.getLinesUpdated());
    assertEquals(
        List.of("1:one:1.75:d", "2:TWO:2.5:d", "3:three:3.5:d"), table(), String.valueOf(table()));
  }
}
