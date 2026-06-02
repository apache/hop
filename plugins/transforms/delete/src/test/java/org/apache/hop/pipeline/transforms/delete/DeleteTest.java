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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/** Unit test for {@link Delete} */
@MockitoSettings(strictness = Strictness.LENIENT)
class DeleteTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<DeleteMeta, DeleteData> mockHelper;
  private DeleteMeta meta;
  private DeleteData data;
  private PipelineMeta pipelineMeta;

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper = new TransformMockHelper<>("DeleteTest", DeleteMeta.class, DeleteData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.isPartitioned()).thenReturn(false);

    pipelineMeta = spy(new PipelineMeta());
    pipelineMeta.setName("delete-test");

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getQuotedSchemaTableCombination(any(), anyString(), anyString()))
        .thenReturn("\"customers\"");
    doReturn(databaseMeta).when(pipelineMeta).findDatabase(anyString(), any());

    meta = new DeleteMeta();
    meta.setConnection("unit-test-db");
    meta.getLookup().setTableName("customers");
    meta.getLookup().getFields().add(new DeleteKeyField("id", "=", "streamId", null));

    data = new DeleteData();
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  private Delete newSpyTransform() {
    TransformMeta transformMeta = new TransformMeta("delete", meta);
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    Delete delete = spy(new Delete(transformMeta, meta, data, 0, pipelineMeta, pipeline));
    delete.setMetadataProvider(new MemoryMetadataProvider());
    return delete;
  }

  @Test
  void testInitFailsWhenConnectionMissing() {
    meta.setConnection(null);
    Delete delete = newSpyTransform();
    assertFalse(delete.init());
  }

  @Test
  void testInitFailsWhenConnectionNotFound() {
    meta.setConnection("missing-connection");
    Delete delete = newSpyTransform();
    assertFalse(delete.init());
  }

  @Test
  void testProcessRowUsesBatchWhenEnabled() throws HopException {
    meta.setUseBatchUpdate(true);

    Database db = mock(Database.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    data.db = db;

    Delete delete = newSpyTransform();

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("streamId"));
    doReturn(new Object[] {1L}).doReturn(null).when(delete).getRow();
    doReturn(inputRowMeta).when(delete).getInputRowMeta();
    doAnswer(
            inv -> {
              data.prepStatementDelete = preparedStatement;
              data.deleteParameterRowMeta = new RowMeta();
              data.deleteParameterRowMeta.addValueMeta(new ValueMetaInteger("streamId"));
              return null;
            })
        .when(delete)
        .prepareDelete(any());

    assertTrue(delete.processRow());
    verify(db).insertRow(preparedStatement, true, true);
    assertFalse(delete.processRow());
  }

  @Test
  void testProcessRowDoesNotUseBatchWhenDisabled() throws HopException {
    meta.setUseBatchUpdate(false);

    Database db = mock(Database.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    data.db = db;

    Delete delete = newSpyTransform();

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("streamId"));
    doReturn(new Object[] {1L}).doReturn(null).when(delete).getRow();
    doReturn(inputRowMeta).when(delete).getInputRowMeta();
    doAnswer(
            inv -> {
              data.prepStatementDelete = preparedStatement;
              data.deleteParameterRowMeta = new RowMeta();
              data.deleteParameterRowMeta.addValueMeta(new ValueMetaInteger("streamId"));
              return null;
            })
        .when(delete)
        .prepareDelete(any());

    assertTrue(delete.processRow());
    verify(db).insertRow(preparedStatement, false, true);
  }

  @Test
  void testDisposeCallsEmptyAndCommitWhenBatchEnabled() throws Exception {
    meta.setUseBatchUpdate(true);

    Database db = mock(Database.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(db.isAutoCommit()).thenReturn(false);

    data.db = db;
    data.prepStatementDelete = preparedStatement;

    Delete delete = newSpyTransform();
    delete.dispose();

    verify(db).emptyAndCommit(preparedStatement, true);
    verify(db).disconnect();
  }

  @Test
  void testDisposeCallsEmptyAndCommitWhenBatchDisabled() throws Exception {
    meta.setUseBatchUpdate(false);

    Database db = mock(Database.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(db.isAutoCommit()).thenReturn(false);

    data.db = db;
    data.prepStatementDelete = preparedStatement;

    Delete delete = newSpyTransform();
    delete.dispose();

    verify(db).emptyAndCommit(preparedStatement, false);
    verify(db).disconnect();
  }
}
