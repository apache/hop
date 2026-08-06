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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RelationalWriteColumn;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LineageRelationalIoEmitterTest {

  /**
   * A Table Output-style write: known target table, resolved relational namespace, column schema.
   */
  @Test
  void emitTransformRelationalWrite_postsRelationalEventWithContractIdentity() {
    LineageHub hub = mock(LineageHub.class);
    ITransform tr = transform("tr-log-w", "Table output", "TableOutput");
    DatabaseMeta db = postgres("db", "5432");

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageRelationalIoEmitter.emitTransformRelationalWrite(
          tr, db, "analytics", "staging", "orders", rowMeta, true, null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (e.getKind() != LineageEventKind.RELATIONAL_IO) {
                    return false;
                  }
                  if (e.getContext().getSubjectType() != LineageSubjectType.TRANSFORM) {
                    return false;
                  }
                  if (!(e.getPayload() instanceof RelationalIoLineagePayload p)) {
                    return false;
                  }
                  if (p.getOperation() != RelationalIoOperation.WRITE
                      || !"postgres://db:5432".equals(p.getDatasetNamespace())
                      || p.getInputs().size() != 0
                      || p.getOutputs().size() != 1) {
                    return false;
                  }
                  RelationalTable out = p.getOutputs().get(0);
                  boolean tableOk =
                      "analytics".equals(out.getDatabase())
                          && "staging".equals(out.getSchema())
                          && "orders".equals(out.getTable());
                  boolean schemaOk =
                      p.getOutputSchema() != null
                          && p.getOutputSchema().getColumns().size() == 2
                          && "id".equals(p.getOutputSchema().getColumns().get(0).getName());
                  return tableOk && schemaOk && p.getSqlText() == null && p.isSuccess();
                }));
  }

  /** A Table Input-style read: SQL retained for downstream parsing, inputs deferred (empty). */
  @Test
  void emitTransformRelationalRead_carriesSqlAndDefersInputParsing() {
    LineageHub hub = mock(LineageHub.class);
    ITransform tr = transform("tr-log-r", "Table input", "TableInput");
    DatabaseMeta db = postgres("db", "5432");

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));

    String sql = "SELECT id FROM analytics.staging.orders";

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageRelationalIoEmitter.emitTransformRelationalRead(
          tr, db, sql, null, rowMeta, true, null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (!(e.getPayload() instanceof RelationalIoLineagePayload p)) {
                    return false;
                  }
                  return p.getOperation() == RelationalIoOperation.READ
                      && sql.equals(p.getSqlText())
                      && p.getInputs().isEmpty()
                      && p.getOutputs().isEmpty()
                      && p.getOutputSchema() != null
                      && p.getOutputSchema().getColumns().size() == 1
                      && "postgres://db:5432".equals(p.getDatasetNamespace());
                }));
  }

  /** Blank port falls back to the database default port, per contract §3.1. */
  @Test
  void emitTransformRelationalWrite_blankPortUsesDefault() {
    LineageHub hub = mock(LineageHub.class);
    ITransform tr = transform("tr-log-dp", "Table output", "TableOutput");
    DatabaseMeta db = postgres("db", "");

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageRelationalIoEmitter.emitTransformRelationalWrite(
          tr, db, "analytics", "staging", "orders", null, true, null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) ->
                    e.getPayload() instanceof RelationalIoLineagePayload p
                        && "postgres://db:5432".equals(p.getDatasetNamespace())
                        && p.getOutputSchema() == null));
  }

  /** No connection means no identity to key on — the emitter is a silent no-op. */
  @Test
  void emitTransformRelationalWrite_nullDatabaseMeta_doesNotEmit() {
    LineageHub hub = mock(LineageHub.class);
    ITransform tr = mock(ITransform.class);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageRelationalIoEmitter.emitTransformRelationalWrite(
          tr, null, "analytics", "staging", "orders", null, true, null);
    }

    verifyNoInteractions(hub);
  }

  private static ITransform transform(String logId, String name, String pluginId) {
    ITransform tr = mock(ITransform.class);
    ILogChannel logCh = mock(ILogChannel.class);
    when(logCh.getLogChannelId()).thenReturn(logId);
    when(tr.getLogChannel()).thenReturn(logCh);
    when(tr.getTransformName()).thenReturn(name);
    when(tr.getCopy()).thenReturn(0);
    when(tr.getTransformPluginId()).thenReturn(pluginId);
    when(tr.resolve(anyString())).thenAnswer(inv -> inv.getArgument(0));

    PipelineMeta pm = new PipelineMeta();
    pm.setName("pipe1");
    pm.setFilename("/tmp/pipe1.hpl");
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pm);
    when(pipeline.getFilename()).thenReturn("/tmp/pipe1.hpl");
    when(pipeline.getLogChannelId()).thenReturn("pipe-log-1");
    when(tr.getPipeline()).thenReturn(pipeline);
    return tr;
  }

  private static DatabaseMeta postgres(String host, String port) {
    DatabaseMeta db = mock(DatabaseMeta.class);
    when(db.getPluginId()).thenReturn("POSTGRESQL");
    when(db.getHostname()).thenReturn(host);
    when(db.getPort()).thenReturn(port);
    when(db.getDefaultDatabasePort()).thenReturn(5432);
    return db;
  }

  @Test
  void writeColumn_mapsStreamFieldToTargetColumnAndResolvesOrigin() {
    RowMeta rowMeta = new RowMeta();
    ValueMetaString amount = new ValueMetaString("amount");
    amount.setOrigin("Read orders_source");
    rowMeta.addValueMeta(amount);

    RelationalWriteColumn column =
        LineageRelationalIoEmitter.writeColumn(rowMeta, "amount_col", "amount");
    assertEquals("amount_col", column.getTargetColumn());
    assertEquals("amount", column.getStreamField());
    assertEquals("Read orders_source", column.getOriginTransform());
  }

  @Test
  void writeColumn_fallsBackToStreamNameWhenTargetBlankAndSkipsWhenUnresolvable() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id")); // no origin set

    RelationalWriteColumn column = LineageRelationalIoEmitter.writeColumn(rowMeta, "", "id");
    assertEquals("id", column.getTargetColumn());
    assertEquals("id", column.getStreamField());
    assertNull(column.getOriginTransform());

    // blank stream field or missing row meta -> null so the caller skips the column
    assertNull(LineageRelationalIoEmitter.writeColumn(rowMeta, "id", ""));
    assertNull(LineageRelationalIoEmitter.writeColumn(null, "id", "id"));
  }

  @Test
  void writeColumnsFromRow_mapsEveryFieldOneToOne() {
    RowMeta rowMeta = new RowMeta();
    ValueMetaString id = new ValueMetaString("id");
    id.setOrigin("Read orders_source");
    ValueMetaString amount = new ValueMetaString("amount");
    amount.setOrigin("Read orders_source");
    rowMeta.addValueMeta(id);
    rowMeta.addValueMeta(amount);

    List<RelationalWriteColumn> columns = LineageRelationalIoEmitter.writeColumnsFromRow(rowMeta);
    assertEquals(2, columns.size());
    assertEquals("id", columns.get(0).getTargetColumn());
    assertEquals("id", columns.get(0).getStreamField());
    assertEquals("Read orders_source", columns.get(0).getOriginTransform());
    assertTrue(LineageRelationalIoEmitter.writeColumnsFromRow(null).isEmpty());
  }

  /**
   * A delete reports the affected table but must not publish the transform's input row shape as the
   * target's column schema — the transform reads those columns, the table does not gain them.
   */
  @Test
  void emitTransformRelationalDelete_reportsTableWithoutColumnSchema() {
    LineageHub hub = mock(LineageHub.class);
    ITransform tr = transform("tr-log-d", "Delete", "Delete");
    DatabaseMeta db = postgres("db", "5432");

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageRelationalIoEmitter.emitTransformRelationalDelete(
          tr, db, "analytics", "staging", "orders", true, null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (!(e.getPayload() instanceof RelationalIoLineagePayload p)) {
                    return false;
                  }
                  return p.getOperation() == RelationalIoOperation.DELETE
                      && p.getOutputs().size() == 1
                      && "orders".equals(p.getOutputs().get(0).getTable())
                      && p.getOutputSchema() == null
                      && p.getWriteColumns().isEmpty();
                }));
  }

  /**
   * The guard the ~15 transform call sites rely on: anything thrown while building the event is
   * absorbed here, so a transform that calls the emitter bare still completes.
   */
  @Test
  void emit_neverPropagatesAFailureToTheTransform() {
    ITransform tr = transform("tr-log-x", "Table output", "TableOutput");
    DatabaseMeta db = postgres("db", "5432");
    // A connection that blows up the moment the emitter asks it for the catalog.
    when(db.getDatabaseName()).thenThrow(new IllegalStateException("connection meta exploded"));

    assertDoesNotThrow(
        () ->
            LineageRelationalIoEmitter.emitTransformRelationalWrite(
                tr, db, "analytics", "staging", "orders", null, true, null));
  }

  /** A connection that cannot be resolved yields null rather than failing the transform. */
  @Test
  void lineageConnection_returnsNullInsteadOfThrowing() {
    ITransform tr = mock(ITransform.class);
    when(tr.getLogChannel()).thenReturn(mock(ILogChannel.class));
    PipelineMeta meta = mock(PipelineMeta.class);
    when(meta.findDatabase(anyString(), Mockito.any()))
        .thenThrow(new IllegalStateException("no such connection"));

    assertNull(LineageRelationalIoEmitter.lineageConnection(tr, meta, "missing"));
  }

  /** Missing arguments short-circuit before any lookup is attempted. */
  @Test
  void lineageConnection_isANoOpForMissingArguments() {
    ITransform tr = mock(ITransform.class);
    assertNull(LineageRelationalIoEmitter.lineageConnection(tr, mock(PipelineMeta.class), ""));
    assertNull(LineageRelationalIoEmitter.lineageConnection(tr, null, "any"));
    assertNull(LineageRelationalIoEmitter.lineageConnection(null, mock(PipelineMeta.class), "any"));
  }
}
