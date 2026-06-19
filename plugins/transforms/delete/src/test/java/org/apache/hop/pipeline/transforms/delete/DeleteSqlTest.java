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

package org.apache.hop.pipeline.transforms.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/** Unit test for {@link DatabaseMeta} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class DeleteSqlTest {
  private DeleteMeta meta;
  private DeleteData data;
  private Delete deleteTransform;

  @BeforeEach
  void setUp() {
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getQuotedSchemaTableCombination(any(), eq("public"), eq("customers")))
        .thenReturn("\"public\".\"customers\"");
    when(databaseMeta.quoteField(anyString()))
        .thenAnswer(invocation -> "\"" + invocation.getArgument(0) + "\"");
    when(databaseMeta.stripCR(anyString())).thenAnswer(invocation -> invocation.getArgument(0));

    PipelineMeta pipelineMeta = spy(new PipelineMeta());
    pipelineMeta.setName("delete-sql-test");
    doReturn(databaseMeta).when(pipelineMeta).findDatabase(anyString(), any());

    meta = new DeleteMeta();
    meta.setConnection("unit-test-db");
    meta.getLookup().setSchemaName("public");
    meta.getLookup().setTableName("customers");

    data = new DeleteData();
    TransformMeta transformMeta = new TransformMeta("delete", meta);
    pipelineMeta.addTransform(transformMeta);

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    deleteTransform = new Delete(transformMeta, meta, data, 0, pipelineMeta, pipeline);
    data.schemaTable = "\"public\".\"customers\"";
  }

  @Test
  void testPrepareDeleteWithEqualsCondition() throws Exception {
    meta.getLookup().getFields().add(new DeleteKeyField("id", "=", "streamId", null));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("streamId"));

    prepareDeleteAndCaptureSql(rowMeta);

    assertEquals(1, data.deleteParameterRowMeta.size());
    assertTrue(capturedSql().contains("DELETE FROM"));
    assertTrue(capturedSql().contains("WHERE"));
    assertTrue(capturedSql().contains("\"id\" = ?"));
  }

  @Test
  void testPrepareDeleteWithBetweenCondition() throws Exception {
    meta.getLookup().getFields().add(new DeleteKeyField("age", "BETWEEN", "minAge", "maxAge"));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("minAge"));
    rowMeta.addValueMeta(new ValueMetaInteger("maxAge"));

    prepareDeleteAndCaptureSql(rowMeta);

    assertEquals(2, data.deleteParameterRowMeta.size());
    assertTrue(capturedSql().contains("\"age\" BETWEEN ? AND ?"));
  }

  @Test
  void testPrepareDeleteWithIsNullCondition() throws Exception {
    meta.getLookup().getFields().add(new DeleteKeyField("deleted_at", "IS NULL", "", null));

    IRowMeta rowMeta = new RowMeta();

    prepareDeleteAndCaptureSql(rowMeta);

    assertEquals(0, data.deleteParameterRowMeta.size());
    assertTrue(capturedSql().contains("\"deleted_at\" IS NULL"));
  }

  @Test
  void testPrepareDeleteWithIsNotNullCondition() throws Exception {
    meta.getLookup().getFields().add(new DeleteKeyField("updated_at", "IS NOT NULL", "", null));

    IRowMeta rowMeta = new RowMeta();

    prepareDeleteAndCaptureSql(rowMeta);

    assertEquals(0, data.deleteParameterRowMeta.size());
    assertTrue(capturedSql().contains("\"updated_at\" IS NOT NULL"));
  }

  @Test
  void testPrepareDeleteWithMultipleKeyConditions() throws Exception {
    meta.getLookup().getFields().add(new DeleteKeyField("id", "=", "streamId", null));
    meta.getLookup().getFields().add(new DeleteKeyField("name", "<>", "streamName", null));
    meta.getLookup().getFields().add(new DeleteKeyField("status", "IS NULL", "", null));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("streamId"));
    rowMeta.addValueMeta(new ValueMetaString("streamName"));

    prepareDeleteAndCaptureSql(rowMeta);

    assertEquals(2, data.deleteParameterRowMeta.size());
    String sql = capturedSql();
    assertTrue(sql.contains("\"id\" = ?"));
    assertTrue(sql.contains("AND"));
    assertTrue(sql.contains("\"name\" <> ?"));
    assertTrue(sql.contains("\"status\" IS NULL"));
  }

  private String capturedSqlValue;

  private void prepareDeleteAndCaptureSql(IRowMeta rowMeta) throws Exception {
    Database db = mock(Database.class);
    Connection connection = mock(Connection.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);

    when(db.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

    data.db = db;
    deleteTransform.prepareDelete(rowMeta);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(sqlCaptor.capture());
    capturedSqlValue = sqlCaptor.getValue();
  }

  private String capturedSql() {
    return capturedSqlValue;
  }
}
