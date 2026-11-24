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
package org.apache.hop.pipeline.transforms.insertupdate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
 * Insert/Update against H2: every row looks up the key; a missing key is inserted, a found row is
 * only updated when a value marked for update differs, compared with the layout the lookup reports.
 */
class InsertUpdateH2Test {

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
        "mem:insupd" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    databaseMeta = new DatabaseMeta(CONNECTION, "H2", "Native", "", databaseName, "", "sa", "");
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      db.execStatement(
          "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(10), amount DOUBLE,"
              + " note VARCHAR(10))");
      db.execStatement("INSERT INTO target VALUES (1, 'one', 1.5, 'n1')");
      db.execStatement("INSERT INTO target VALUES (2, 'two', 2.5, 'n2')");
      db.commit(true);
    }
  }

  private List<String> table() throws Exception {
    List<String> content = new ArrayList<>();
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      for (Object[] row : db.getRows("SELECT id, name, amount, note FROM target ORDER BY id", -1)) {
        content.add(row[0] + ":" + row[1] + ":" + row[2] + ":" + row[3]);
      }
    }
    return content;
  }

  private InsertUpdate insertUpdate(Queue<Object[]> input, List<Object[]> output) throws Exception {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaNumber("amount"));
    inputRowMeta.addValueMeta(new ValueMetaString("note"));

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    InsertUpdateMeta meta = new InsertUpdateMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setCommitSize("100");
    InsertUpdateLookupField lookup = meta.getInsertUpdateLookupField();
    lookup.setSchemaName("");
    lookup.setTableName("target");
    lookup.getLookupKeys().add(new InsertUpdateKeyField("id", "id", "="));
    lookup.getValueFields().add(new InsertUpdateValue("id", "id", false));
    lookup.getValueFields().add(new InsertUpdateValue("name", "name", true));
    lookup.getValueFields().add(new InsertUpdateValue("amount", "amount", true));
    // Inserted, never updated
    lookup.getValueFields().add(new InsertUpdateValue("note", "note", false));
    TransformMeta transformMeta = new TransformMeta("insertupdate", meta);
    pipelineMeta.addTransform(transformMeta);

    return new InsertUpdate(
        transformMeta, meta, new InsertUpdateData(), 0, pipelineMeta, new LocalPipelineEngine()) {
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

  private static void run(InsertUpdate transform) throws Exception {
    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }
  }

  @Test
  void missingKeysAreInsertedAndOnlyChangedRowsUpdated() throws Exception {
    Queue<Object[]> input = new ArrayDeque<>();
    input.add(new Object[] {1L, "one", 1.5, "x"}); // unchanged: note is not compared
    input.add(new Object[] {2L, "TWO", 2.5, "x"}); // name changed
    input.add(new Object[] {3L, "three", 3.5, "n3"}); // inserted
    input.add(new Object[] {3L, "three", 3.5, "y"}); // unchanged now
    input.add(new Object[] {1L, "one", 1.75, "z"}); // amount changed
    List<Object[]> output = new ArrayList<>();

    InsertUpdate transform = insertUpdate(input, output);
    run(transform);

    assertEquals(5, output.size());
    assertEquals(1, transform.getLinesOutput());
    assertEquals(2, transform.getLinesUpdated());
    assertEquals(List.of("1:one:1.75:n1", "2:TWO:2.5:n2", "3:three:3.5:n3"), table());
  }
}
