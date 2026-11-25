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

package org.apache.hop.pipeline.transforms.databasejoin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.lineage.LineageRelationalIoEmitter;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** The read Database join reports: at the end of the run, with how the run went. */
class DatabaseJoinLineageTest {

  private static final String QUERY = "SELECT name FROM public.customers WHERE id = ?";

  private DatabaseJoinData data;
  private DatabaseJoin transform;
  private DatabaseMeta databaseMeta;

  @BeforeEach
  void setUp() {
    TransformMeta transformMeta = mock(TransformMeta.class);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(transformMeta.getName()).thenReturn("Look up customer");
    when(pipelineMeta.findTransform(anyString())).thenReturn(transformMeta);
    when(transformMeta.getTargetTransformPartitioningMeta())
        .thenReturn(mock(TransformPartitioningMeta.class));
    Pipeline pipeline = new LocalPipelineEngine();

    data = new DatabaseJoinData();
    data.db = mock(Database.class);
    databaseMeta = mock(DatabaseMeta.class);
    when(data.db.getDatabaseMeta()).thenReturn(databaseMeta);
    transform =
        new DatabaseJoin(
            transformMeta, mock(DatabaseJoinMeta.class), data, 0, pipelineMeta, pipeline);
  }

  /** A run in which no row came in (all filtered upstream) still read nothing but the table. */
  @Test
  void aPreparedQueryIsReportedAtDisposeAlsoWithoutRows() {
    data.parameterSpec = DatabaseJoinMeta.parseSqlParameterSpec(QUERY);
    data.pstmt = mock(PreparedStatement.class);

    try (MockedStatic<LineageRelationalIoEmitter> emitter =
        mockStatic(LineageRelationalIoEmitter.class)) {
      transform.dispose();

      emitter.verify(
          () ->
              LineageRelationalIoEmitter.emitTransformRelationalRead(
                  eq(transform),
                  eq(databaseMeta),
                  eq(QUERY),
                  isNull(),
                  isNull(),
                  eq(true),
                  isNull()));
    }
  }

  @Test
  void aRunWithErrorsIsReportedAsFailed() {
    data.parameterSpec = DatabaseJoinMeta.parseSqlParameterSpec(QUERY);
    data.pstmt = mock(PreparedStatement.class);
    transform.setErrors(1);

    try (MockedStatic<LineageRelationalIoEmitter> emitter =
        mockStatic(LineageRelationalIoEmitter.class)) {
      transform.dispose();

      emitter.verify(
          () ->
              LineageRelationalIoEmitter.emitTransformRelationalRead(
                  any(), any(), eq(QUERY), any(), any(), eq(false), any()));
    }
  }

  @Test
  void aQueryThatWasNeverPreparedIsNotReported() {
    data.parameterSpec = DatabaseJoinMeta.parseSqlParameterSpec(QUERY);
    data.pstmt = null;

    try (MockedStatic<LineageRelationalIoEmitter> emitter =
        mockStatic(LineageRelationalIoEmitter.class)) {
      transform.dispose();

      emitter.verify(
          () ->
              LineageRelationalIoEmitter.emitTransformRelationalRead(
                  any(), any(), any(), any(), any(), anyBoolean(), any()),
          never());
    }
  }
}
