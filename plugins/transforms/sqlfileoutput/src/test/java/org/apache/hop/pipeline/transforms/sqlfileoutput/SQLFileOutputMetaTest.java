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
package org.apache.hop.pipeline.transforms.sqlfileoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class SQLFileOutputMetaTest {
  @Test
  void testSerialization() throws Exception {
    SQLFileOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/sql-file-output-transform.xml", SQLFileOutputMeta.class);

    assertEquals("testtable", meta.getTableName());
    assertEquals("${DATABASE_NAME}", meta.getConnection());
    assertEquals("public", meta.getSchemaName());
    assertFalse(meta.isTruncateTable());
    assertTrue(meta.isStartNewLine());

    assertEquals("${PROJECT_HOME}/output/filename", meta.getFile().getFileName());
    assertFalse(meta.getFile().isFileAppended());
    assertFalse(meta.getFile().isTransformNrInFilename());
    assertEquals(0, meta.getFile().getSplitEvery());

    // Pipelines saved before these options existed must keep the old behaviour
    assertFalse(meta.isDoNotAddInsertStatements());
    assertFalse(meta.isSpecifyFields());
    assertTrue(meta.getSqlFileOutputFields().isEmpty());
  }

  @Test
  void testClone() throws Exception {
    SQLFileOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/sql-file-output-transform.xml", SQLFileOutputMeta.class);

    SQLFileOutputMeta clone = (SQLFileOutputMeta) meta.clone();

    assertEquals(clone.getTableName(), meta.getTableName());
    assertEquals(clone.getConnection(), meta.getConnection());
    assertEquals(clone.getSchemaName(), meta.getSchemaName());
    assertEquals(clone.isTruncateTable(), meta.isTruncateTable());
    assertEquals(clone.isStartNewLine(), meta.isStartNewLine());

    assertEquals(clone.getFile().getFileName(), meta.getFile().getFileName());
    assertEquals(clone.getFile().isFileAppended(), meta.getFile().isFileAppended());
    assertEquals(
        clone.getFile().isTransformNrInFilename(), meta.getFile().isTransformNrInFilename());
    assertEquals(clone.getFile().getSplitEvery(), meta.getFile().getSplitEvery());
  }

  @Test
  void testSerializationWithSelectedFields() throws Exception {
    SQLFileOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/sql-file-output-transform-fields.xml", SQLFileOutputMeta.class);

    assertTrue(meta.isDoNotAddInsertStatements());
    assertTrue(meta.isSpecifyFields());
    assertEquals(2, meta.getSqlFileOutputFields().size());
    assertEquals("id", meta.getSqlFileOutputFields().get(0).getName());
    assertEquals("client_id", meta.getSqlFileOutputFields().get(0).getRename());
    assertEquals("label", meta.getSqlFileOutputFields().get(1).getName());

    SQLFileOutputMeta clone = (SQLFileOutputMeta) meta.clone();
    assertEquals(meta.isDoNotAddInsertStatements(), clone.isDoNotAddInsertStatements());
    assertEquals(meta.isSpecifyFields(), clone.isSpecifyFields());
    assertEquals(meta.getSqlFileOutputFields().size(), clone.getSqlFileOutputFields().size());
    for (int i = 0; i < meta.getSqlFileOutputFields().size(); i++) {
      assertEquals(
          meta.getSqlFileOutputFields().get(i).getName(),
          clone.getSqlFileOutputFields().get(i).getName());
      assertEquals(
          meta.getSqlFileOutputFields().get(i).getRename(),
          clone.getSqlFileOutputFields().get(i).getRename());
    }
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("label"));
    rowMeta.addValueMeta(new ValueMetaString("comment"));
    return rowMeta;
  }

  private static SQLFileOutputField field(String name, String rename) {
    SQLFileOutputField field = new SQLFileOutputField();
    field.setName(name);
    field.setRename(rename);
    return field;
  }

  @Test
  void testSqlRowMetaUsesAllFieldsByDefault() throws Exception {
    SQLFileOutputMeta meta = new SQLFileOutputMeta();
    meta.setDefault();

    IRowMeta sqlRowMeta = meta.getSqlRowMeta(inputRowMeta());

    assertEquals(3, sqlRowMeta.size());
    assertEquals("id", sqlRowMeta.getValueMeta(0).getName());
    assertEquals("label", sqlRowMeta.getValueMeta(1).getName());
    assertEquals("comment", sqlRowMeta.getValueMeta(2).getName());
  }

  @Test
  void testSqlRowMetaKeepsSelectedFieldsAndRenames() throws Exception {
    SQLFileOutputMeta meta = new SQLFileOutputMeta();
    meta.setDefault();
    meta.setSpecifyFields(true);
    meta.setSqlFileOutputFields(List.of(field("label", null), field("id", "client_id")));

    IRowMeta sqlRowMeta = meta.getSqlRowMeta(inputRowMeta());

    // Order of the grid, "comment" left out, "id" renamed
    assertEquals(2, sqlRowMeta.size());
    assertEquals("label", sqlRowMeta.getValueMeta(0).getName());
    assertEquals("client_id", sqlRowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, sqlRowMeta.getValueMeta(1).getType());
  }

  @Test
  void testSqlRowMetaFailsWhenNoFieldIsSelected() {
    SQLFileOutputMeta meta = new SQLFileOutputMeta();
    meta.setDefault();
    meta.setSpecifyFields(true);

    assertThrows(HopTransformException.class, () -> meta.getSqlRowMeta(inputRowMeta()));
  }

  @Test
  void testSqlRowMetaFailsOnUnknownField() {
    SQLFileOutputMeta meta = new SQLFileOutputMeta();
    meta.setDefault();
    meta.setSpecifyFields(true);
    meta.setSqlFileOutputFields(List.of(field("unknown", null)));

    assertThrows(HopTransformException.class, () -> meta.getSqlRowMeta(inputRowMeta()));
  }
}
