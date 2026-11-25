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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Merge into table on DuckDB with its own dialect: its MERGE and its ON CONFLICT, in batches
 * (DuckDB loses the whole batch on a failure and closes the statement) and row by row, with an
 * error hop.
 */
class MergeIntoTableDuckDbTest {

  @TempDir Path folder;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    MergeIntoTableH2Harness.registerDuckDb();
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  @ParameterizedTest(name = "{0}, batch {1}")
  @CsvSource({"MERGE, true", "MERGE, false", "ON_CONFLICT, true", "ON_CONFLICT, false"})
  void errorHopTakesOnlyTheBadRows(MergeUpsertForm form, boolean batch) throws Exception {
    MergeIntoTableH2Harness duck =
        MergeIntoTableH2Harness.on(
            MergeIntoTableH2Harness.duckDb(folder.resolve("merge.duckdb").toString()));
    duck.execute(
        "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL)",
        "INSERT INTO target VALUES (1, 'old'), (2, 'old')");
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setDefault();
    meta.setTableName("target");
    meta.setCommitSize("3");
    meta.setUseBatchUpdate(batch);
    meta.setUpsertForm(form);
    meta.getKeyFields().add(new MergeKeyField("id", "id"));
    meta.getValueFields().add(new MergeValueField("name", "name", true));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 8; id++) {
      rows.add(new Object[] {id, id == 2 || id == 6 ? null : "n" + id});
    }

    MergeIntoTable transform = duck.run(meta, inputRowMeta(), rows, true);

    assertEquals(List.of(2L, 6L), duck.errors.stream().map(e -> (Long) e.row()[0]).toList());
    assertEquals(
        List.of(1L, 3L, 4L, 5L, 7L, 8L), duck.output.stream().map(r -> (Long) r[0]).toList());
    assertEquals(6, transform.getLinesOutput());
    assertEquals(2, transform.getLinesRejected());
    assertEquals(0, transform.getErrors());
    List<String> table = new ArrayList<>();
    for (Object[] r : duck.query("SELECT id, name FROM target ORDER BY id")) {
      table.add(r[0] + ":" + r[1]);
    }
    assertEquals(List.of("1:n1", "2:old", "3:n3", "4:n4", "5:n5", "7:n7", "8:n8"), table);
  }
}
