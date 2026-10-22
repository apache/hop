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

package org.apache.hop.pipeline.transforms.execsqlrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Execute row SQL script with an error hop on PostgreSQL, where a failed statement aborts the
 * transaction: only the failing row may go to the error hop, the rows after it must still run.
 */
class ExecSqlRowPostgresIT {

  private static final String CONNECTION = "pg";
  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = new PostgreSQLContainer<>("postgres:16-alpine");
    container.start();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  private static DatabaseMeta databaseMeta() {
    return new DatabaseMeta(
        CONNECTION,
        "POSTGRESQL",
        "Native",
        container.getHost(),
        container.getDatabaseName(),
        Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
        container.getUsername(),
        container.getPassword());
  }

  private static List<Object[]> query(String sql) throws Exception {
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta())) {
      db.connect();
      return db.getRows(sql, -1);
    }
  }

  /**
   * @param commitSize 1 commits after every row, 100 only at the end
   */
  @ParameterizedTest
  @ValueSource(ints = {100, 1})
  void onlyTheFailingStatementGoesToTheErrorHop(int commitSize) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta())) {
      db.connect();
      db.execStatement("DROP TABLE IF EXISTS exec_row_target");
      db.execStatement("CREATE TABLE exec_row_target (id INTEGER PRIMARY KEY)");
    }

    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();
    Deque<Object[]> input = new ArrayDeque<>();
    for (String id : new String[] {"1", "2", "2", "3", "4", "5"}) {
      input.add(new Object[] {"INSERT INTO exec_row_target VALUES (" + id + ")"});
    }
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("sql"));

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta());
    pipelineMeta.setMetadataProvider(metadataProvider);

    ExecSqlRowMeta meta = new ExecSqlRowMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setSqlFieldName("sql");
    meta.setSendOneStatement(true);
    meta.setCommitSize(commitSize);
    TransformMeta transformMeta = new TransformMeta("exec row sql", meta);
    pipelineMeta.addTransform(transformMeta);

    ExecSqlRow transform =
        new ExecSqlRow(
            transformMeta, meta, new ExecSqlRowData(), 0, pipelineMeta, new LocalPipelineEngine()) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            return input.poll();
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) {
            output.add(row);
          }

          @Override
          public void putError(
              IRowMeta rowMeta,
              Object[] row,
              long nrErrors,
              String errorDescriptions,
              String fieldNames,
              String errorCodes) {
            errors.add(row);
          }
        };

    TransformMeta target = new TransformMeta("errors", new ExecSqlRowMeta());
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
    TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
    errorMeta.setEnabled(true);
    transformMeta.setTransformErrorMeta(errorMeta);

    assertEquals(true, transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }

    assertEquals(5, output.size());
    assertEquals(1, errors.size());
    assertEquals("INSERT INTO exec_row_target VALUES (2)", errors.get(0)[0]);
    List<Object[]> rows = query("SELECT id FROM exec_row_target ORDER BY id");
    assertEquals(5, rows.size());
    assertEquals(5L, ((Number) rows.get(4)[0]).longValue());
  }
}
