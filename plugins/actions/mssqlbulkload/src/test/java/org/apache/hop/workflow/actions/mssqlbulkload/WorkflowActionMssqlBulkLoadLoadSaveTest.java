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

package org.apache.hop.workflow.actions.mssqlbulkload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.jupiter.api.Test;

class WorkflowActionMssqlBulkLoadLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionMssqlBulkLoad> {

  @Override
  protected Class<ActionMssqlBulkLoad> getActionClass() {
    return ActionMssqlBulkLoad.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList(
        "schemaName",
        "tableName",
        "fileName",
        "dataFileType",
        "fieldTerminator",
        "lineTerminated",
        "codePage",
        "specificCodePage",
        "formatFileName",
        "fireTriggers",
        "checkConstraints",
        "keepNulls",
        "keepIdentity",
        "tabLock",
        "startFile",
        "endFile",
        "orderBy",
        "orderDirection",
        "maxErrors",
        "batchSize",
        "rowsPerBatch",
        "errorFileName",
        "addDatetime",
        "addFileToResult",
        "truncate",
        "connection");
  }

  @Test
  void testNewSerialization() throws Exception {
    ActionMssqlBulkLoad meta =
        ActionSerializationTestUtil.testSerialization(
            "/mssql-bulkloader-action.xml", ActionMssqlBulkLoad.class);

    assertEquals("dbo", meta.getSchemaName());
    assertEquals("[test-table]", meta.getTableName());
    assertEquals("/tmp/mssql_bulkload.csv", meta.getFileName());
    assertEquals("char", meta.getDataFileType());
    assertEquals(",", meta.getFieldTerminator());
    assertEquals("RAW", meta.getCodePage());
    assertEquals("mssql-test-db", meta.getConnection());
    assertFalse(meta.isFireTriggers());
    assertFalse(meta.isCheckConstraints());
    assertFalse(meta.isKeepIdentity());
    assertFalse(meta.isKeepNulls());
  }

  @Test
  void testClone() throws Exception {
    ActionMssqlBulkLoad meta =
        ActionSerializationTestUtil.testSerialization(
            "/mssql-bulkloader-action.xml", ActionMssqlBulkLoad.class);

    ActionMssqlBulkLoad clone = (ActionMssqlBulkLoad) meta.clone();

    assertEquals(clone.getSchemaName(), meta.getSchemaName());
    assertEquals(clone.getTableName(), meta.getTableName());
    assertEquals(clone.getFileName(), meta.getFileName());
    assertEquals(clone.getDataFileType(), meta.getDataFileType());
    assertEquals(clone.getFieldTerminator(), meta.getFieldTerminator());
    assertEquals(clone.getCodePage(), meta.getCodePage());
    assertEquals(clone.getConnection(), meta.getConnection());
    assertEquals(clone.isFireTriggers(), meta.isFireTriggers());
    assertEquals(clone.isCheckConstraints(), meta.isCheckConstraints());
    assertEquals(clone.isKeepIdentity(), meta.isKeepIdentity());
    assertEquals(clone.isKeepNulls(), meta.isKeepNulls());
  }
}
