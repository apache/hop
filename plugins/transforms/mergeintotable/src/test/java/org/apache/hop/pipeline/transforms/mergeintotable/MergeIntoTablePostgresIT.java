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

package org.apache.hop.pipeline.transforms.mergeintotable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Merge into table on PostgreSQL 18 with an error hop, where the MERGE casts its parameters to the
 * column types: a name too long for its column must go to the error hop, as a plain INSERT or
 * UPDATE refuses it, and the other rows must be written. Needs Docker; run with {@code
 * -Dit.test=MergeIntoTablePostgresIT}.
 */
class MergeIntoTablePostgresIT {

  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = new PostgreSQLContainer<>("postgres:18");
    container.start();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  private static MergeIntoTableH2Harness harness() throws Exception {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            MergeIntoTableH2Harness.CONNECTION,
            "POSTGRESQL",
            "Native",
            container.getHost(),
            container.getDatabaseName(),
            Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
            container.getUsername(),
            container.getPassword());
    MergeIntoTableH2Harness harness = MergeIntoTableH2Harness.on(databaseMeta);
    harness.execute(
        "DROP TABLE IF EXISTS merge_target",
        "CREATE TABLE merge_target (id BIGINT PRIMARY KEY, name VARCHAR(5))",
        "INSERT INTO merge_target SELECT g, 'x' FROM generate_series(1, 10) g");
    return harness;
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static MergeIntoTableMeta meta(boolean batch) {
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setDefault();
    meta.setTableName("merge_target");
    meta.setCommitSize("100");
    meta.setUseBatchUpdate(batch);
    meta.getKeyFields().add(new MergeKeyField("id", "id"));
    meta.getValueFields().add(new MergeValueField("name", "name", true));
    return meta;
  }

  /** Rows 1..20: 1..10 exist and are updated, 11..20 are inserted; 7 and 14 are too long. */
  private static List<Object[]> rows() {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 20; id++) {
      rows.add(new Object[] {id, id % 7 == 0 ? "toolong" : "a"});
    }
    return rows;
  }

  private static List<String> namesOtherThanA(MergeIntoTableH2Harness harness) throws Exception {
    return harness.query("SELECT id, name FROM merge_target WHERE name <> 'a' ORDER BY id").stream()
        .map(r -> r[0] + ":" + r[1])
        .toList();
  }

  /**
   * The MERGE casts its parameters: the overlong names of row 7 (an update) and row 14 (an insert)
   * go to the error hop, the other rows are written, in batches (recovered as PostgreSQL loses the
   * whole batch) and row by row (behind a savepoint per row). A cast to {@code varchar(5)} would
   * cut them to 'toolo' and write them.
   */
  @ParameterizedTest(name = "batch {0}")
  @ValueSource(booleans = {true, false})
  void overlongNamesGoToTheErrorHop(boolean batch) throws Exception {
    MergeIntoTableH2Harness harness = harness();
    MergeIntoTable transform = harness.run(meta(batch), inputRowMeta(), rows(), true);

    assertEquals(List.of(7L, 14L), harness.errors.stream().map(e -> (Long) e.row()[0]).toList());
    assertEquals(18, harness.output.size());
    assertEquals(18, transform.getLinesOutput());
    assertEquals(2, transform.getLinesRejected());
    assertEquals(0, transform.getErrors());
    assertEquals(List.of("7:x"), namesOtherThanA(harness));
  }
}
