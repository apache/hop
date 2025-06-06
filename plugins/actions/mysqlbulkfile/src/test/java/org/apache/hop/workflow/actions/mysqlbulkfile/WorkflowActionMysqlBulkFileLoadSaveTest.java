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

package org.apache.hop.workflow.actions.mysqlbulkfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.jupiter.api.Test;

public class WorkflowActionMysqlBulkFileLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionMysqlBulkFile> {

  @Override
  protected Class<ActionMysqlBulkFile> getActionClass() {
    return ActionMysqlBulkFile.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList(
        "schemaName",
        "tableName",
        "fileName",
        "separator",
        "enclosed",
        "optionEnclosed",
        "lineTerminated",
        "limitLines",
        "listColumn",
        "highPriority",
        "outDumpValue",
        "ifFileExists",
        "addFileToResult",
        "connection");
  }

  @Test
  void testNewSerialization() throws Exception {
    ActionMysqlBulkFile meta =
        ActionSerializationTestUtil.testSerialization(
            "/mysql-bulkfile-action.xml", ActionMysqlBulkFile.class);

    assertEquals("testSchema", meta.getSchemaName());
    assertEquals("testSourceTable", meta.getTableName());
    assertEquals("/tmp/file", meta.getFileName());
    assertEquals(",", meta.getSeparator());
    assertEquals("\"", meta.getEnclosed());
    assertEquals("mysql", meta.getConnection());
    assertFalse(meta.isOptionEnclosed());
    assertFalse(meta.isAddFileToResult());
  }

  @Test
  void testClone() throws Exception {
    ActionMysqlBulkFile meta =
        ActionSerializationTestUtil.testSerialization(
            "/mysql-bulkfile-action.xml", ActionMysqlBulkFile.class);

    ActionMysqlBulkFile clone = (ActionMysqlBulkFile) meta.clone();

    assertEquals(clone.getSchemaName(), meta.getSchemaName());
    assertEquals(clone.getTableName(), meta.getTableName());
    assertEquals(clone.getFileName(), meta.getFileName());
    assertEquals(clone.getSeparator(), meta.getSeparator());
    assertEquals(clone.getEnclosed(), meta.getEnclosed());
    assertEquals(clone.getConnection(), meta.getConnection());
    assertEquals(clone.isOptionEnclosed(), meta.isOptionEnclosed());
    assertEquals(clone.isAddFileToResult(), meta.isAddFileToResult());
  }
}
