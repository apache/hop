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

package org.apache.hop.pipeline.transforms.execsqlrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** The SQL scripts Execute row SQL script keeps for lineage, and how it reports them. */
class ExecSqlRowLineageTest {

  private TransformMockHelper<ExecSqlRowMeta, ExecSqlRowData> mockHelper;
  private ExecSqlRowData data;
  private ExecSqlRow transform;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "Execute row SQL script", ExecSqlRowMeta.class, ExecSqlRowData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    data = new ExecSqlRowData();
    transform =
        new ExecSqlRow(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            data,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void withLineageOffNothingIsKept() {
    try (MockedStatic<LineageHub> hub = lineageEnabled(false)) {
      transform.recordLineageScript("UPDATE orders SET status = 'done' WHERE id = 1");
    }

    assertTrue(data.lineageScripts.isEmpty());
  }

  @Test
  void eachDistinctScriptIsKeptOnce() {
    try (MockedStatic<LineageHub> hub = lineageEnabled(true)) {
      transform.recordLineageScript("DELETE FROM staging.orders");
      transform.recordLineageScript("INSERT INTO sales.orders SELECT * FROM staging.orders");
      transform.recordLineageScript("DELETE FROM staging.orders");
      transform.recordLineageScript(null);
      transform.recordLineageScript("");
    }

    assertEquals(2, data.lineageScripts.size());
    assertFalse(data.lineageTruncated);
  }

  /** A script per row with its values in it is a new script on every row: the set stops growing. */
  @Test
  void theScriptsKeptStopAtTheCap() {
    try (MockedStatic<LineageHub> hub = lineageEnabled(true)) {
      for (int i = 0; i < ExecSqlRow.MAX_LINEAGE_SCRIPTS + 200; i++) {
        transform.recordLineageScript("UPDATE orders SET status = 'done' WHERE id = " + i);
      }
    }

    assertEquals(ExecSqlRow.MAX_LINEAGE_SCRIPTS, data.lineageScripts.size());
    assertTrue(data.lineageTruncated);
  }

  /** Large scripts: what is kept stops at the character cap, not only at the script count. */
  @Test
  void theScriptsKeptStopAtTheCharacterCap() {
    String filler = "x".repeat(200 * 1024);
    try (MockedStatic<LineageHub> hub = lineageEnabled(true)) {
      for (int i = 0; i < 10; i++) {
        transform.recordLineageScript("INSERT INTO t VALUES ('" + i + filler + "')");
      }
    }

    assertEquals(5, data.lineageScripts.size());
    assertTrue(data.lineageChars <= ExecSqlRow.MAX_LINEAGE_CHARS);
    assertTrue(data.lineageTruncated);
  }

  /**
   * A script file is read when it ran, once, so a temporary file deleted afterwards still has its
   * tables reported.
   */
  @Test
  void aScriptFileIsReadWhenItRuns(@TempDir Path folder) throws Exception {
    when(mockHelper.iTransformMeta.isSqlFromfile()).thenReturn(true);
    Path script = folder.resolve("load.sql");
    Files.writeString(script, "INSERT INTO sales.orders SELECT * FROM staging.orders;\n");

    try (MockedStatic<LineageHub> hub = lineageEnabled(true)) {
      transform.recordLineageScript(script.toString());
      transform.recordLineageScript(script.toString());
    }
    Files.delete(script);

    assertEquals(
        "INSERT INTO sales.orders SELECT * FROM staging.orders;\n", transform.lineageSql());
  }

  /** One script for all of them, so no table gets the row counts of every statement. */
  @Test
  void theScriptsAreReportedAsOneScript() {
    data.lineageScripts.add("DELETE FROM staging.orders;");
    data.lineageScripts.add("  INSERT INTO sales.orders SELECT * FROM staging.orders ;; ");
    data.lineageScripts.add("UPDATE a SET x = 1; UPDATE b SET y = 2");
    data.lineageScripts.add(" ; ");

    assertEquals(
        "DELETE FROM staging.orders;\n"
            + "INSERT INTO sales.orders SELECT * FROM staging.orders;\n"
            + "UPDATE a SET x = 1; UPDATE b SET y = 2;\n",
        transform.lineageSql());
  }

  /**
   * Stubs the hub's on/off switch directly, as {@code TableOutputLineageTest} does: the variable
   * HOP_LINEAGE_ENABLED is not reliable once another test initialised the Hop environment.
   */
  private static MockedStatic<LineageHub> lineageEnabled(boolean enabled) {
    LineageHub hub = mock(LineageHub.class);
    when(hub.isEnabled()).thenReturn(enabled);
    MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class);
    staticHub.when(LineageHub::getInstance).thenReturn(hub);
    return staticHub;
  }
}
