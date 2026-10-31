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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.List;
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
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLField;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLKey;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Dimension lookup/update with an error hop on PostgreSQL, where a failed statement aborts the
 * transaction: only the failing rows may go to the error hop, the rows after them must still be
 * written, and a failed new version must not leave its earlier statements (closing the previous
 * version) or a cache entry behind.
 */
class DimensionLookupPostgresIT {

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
   * @param cacheSize 0 caches every row, -1 caches nothing
   * @param commitSize 1 commits inside every row, 100 only at the end
   */
  @ParameterizedTest
  @CsvSource({"0, 100", "-1, 100", "0, 1", "-1, 1"})
  void onlyTheFailingRowsGoToTheErrorHop(int cacheSize, int commitSize) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta())) {
      db.connect();
      db.execStatement("DROP TABLE IF EXISTS d_customer");
      db.execStatement(
          "CREATE TABLE d_customer (tk BIGINT PRIMARY KEY, version INTEGER, date_from TIMESTAMP,"
              + " date_to TIMESTAMP, cust_id BIGINT, name VARCHAR(5))");
    }

    // Rows 3 and 5 are too long for the column: row 3 is a new customer, row 5 a new version of
    // customer 1, which first closes the current version and then fails to insert the new one.
    String[][] rows = {
      {"1", "a"}, {"2", "b"}, {"3", "toolong"}, {"4", "d"}, {"1", "toolong"}, {"5", "e"}, {"1", "z"}
    };
    Date updated = new Date();
    Deque<Object[]> input = new ArrayDeque<>();
    for (String[] row : rows) {
      input.add(new Object[] {Long.valueOf(row[0]), row[1], updated});
    }
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("cust_id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaDate("updated"));
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta());
    pipelineMeta.setMetadataProvider(metadataProvider);

    DimensionLookupMeta meta = new DimensionLookupMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setSchemaName("");
    meta.setTableName("d_customer");
    meta.setUpdate(true);
    meta.setCommitSize(commitSize);
    meta.setCacheSize(cacheSize);
    DLKey key = new DLKey();
    key.setName("cust_id");
    key.setLookup("cust_id");
    meta.getFields().getKeys().add(key);
    DLField field = new DLField();
    field.setName("name");
    field.setLookup("name");
    field.setUpdate(DimensionLookupMeta.DimensionUpdateType.INSERT.getCode());
    meta.getFields().getFields().add(field);
    meta.getFields().getDate().setName("updated");
    meta.getFields().getReturns().setKeyField("tk");
    meta.getFields().getReturns().setCreationMethod(TechnicalKeyCreationMethod.TABLE_MAXIMUM);
    meta.getFields().getReturns().setVersionField("version");

    TransformMeta transformMeta = new TransformMeta("dimension", meta);
    pipelineMeta.addTransform(transformMeta);
    DimensionLookup transform =
        new DimensionLookup(
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

    TransformMeta target = new TransformMeta("errors", new DimensionLookupMeta());
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
    TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
    errorMeta.setEnabled(true);
    transformMeta.setTransformErrorMeta(errorMeta);

    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }

    assertEquals(5, output.size());
    assertEquals(2, errors.size());
    assertEquals(3L, errors.get(0)[0]);
    assertEquals(1L, errors.get(1)[0]);
    assertEquals(
        List.of("1:1:a:closed", "1:2:z:open", "2:1:b:open", "4:1:d:open", "5:1:e:open"),
        query(
                "SELECT cust_id, version, name, date_to < '2100-01-01' FROM d_customer"
                    + " WHERE tk > 0 ORDER BY cust_id, version")
            .stream()
            .map(
                r ->
                    r[0]
                        + ":"
                        + r[1]
                        + ":"
                        + r[2]
                        + ":"
                        + (Boolean.TRUE.equals(r[3]) ? "closed" : "open"))
            .toList());
  }
}
