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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Without batch updates, a row whose statement fails goes to the error hop only. It used to be
 * passed on to the output as well, so the transforms after it got rows that were never written.
 */
class SynchronizeAfterMergeErrorRowTest {

  private SynchronizeAfterMerge transform;
  private SynchronizeAfterMergeData data;
  private List<String> output;
  private List<String> errors;

  @BeforeEach
  void setUp() throws Exception {
    SynchronizeAfterMergeMeta meta = mock(SynchronizeAfterMergeMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    doReturn("transform").when(transformMeta).getName();
    doReturn(mock(TransformPartitioningMeta.class))
        .when(transformMeta)
        .getTargetTransformPartitioningMeta();
    doReturn(meta).when(transformMeta).getTransform();
    doReturn(true).when(transformMeta).isDoingErrorHandling();

    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(anyString());

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("op"));

    data = new SynchronizeAfterMergeData();
    data.inputRowMeta = rowMeta;
    data.outputRowMeta = rowMeta;
    data.insertRowMeta = new RowMeta();
    data.insertRowMeta.addValueMeta(new ValueMetaString("name"));
    data.indexOfOperationOrderField = 1;
    data.valueIndexes = new int[] {0};
    data.keyIndexes = new int[0];
    data.keyIndexes2 = new int[0];
    data.insertValue = "insert";
    data.updateValue = "update";
    data.deleteValue = "delete";
    data.insertStatement = mock(PreparedStatement.class);
    data.commitSize = 100;
    data.batchMode = false;
    data.useSavepoints = false;

    // The INSERT fails for a row named "bad".
    Database db = mock(Database.class);
    String[] bound = new String[1];
    doAnswer(
            inv -> {
              Object[] values = inv.getArgument(1);
              bound[0] = (String) values[0];
              return null;
            })
        .when(db)
        .setValues(any(IRowMeta.class), any(Object[].class), any(PreparedStatement.class));
    doAnswer(
            inv -> {
              if ("bad".equals(bound[0])) {
                throw new HopDatabaseException("value too long");
              }
              return true;
            })
        .when(db)
        .insertRow(any(PreparedStatement.class), anyBoolean());
    data.db = db;

    output = new ArrayList<>();
    errors = new ArrayList<>();

    transform =
        spy(
            new SynchronizeAfterMerge(
                transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
    transform.first = false;
    doReturn(transformMeta).when(transform).getTransformMeta();
    doReturn(false).when(transform).isRowLevel();
    doReturn(rowMeta).when(transform).getInputRowMeta();
    doAnswer(
            inv -> {
              output.add((String) ((Object[]) inv.getArgument(1))[0]);
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any());
    doAnswer(
            inv -> {
              errors.add((String) ((Object[]) inv.getArgument(1))[0]);
              return null;
            })
        .when(transform)
        .putError(
            any(IRowMeta.class),
            any(),
            anyLong(),
            anyString(),
            nullable(String.class),
            anyString());
  }

  private void run(Object[]... rows) throws Exception {
    for (Object[] row : rows) {
      doReturn(row).when(transform).getRow();
      assertTrue(transform.processRow());
    }
  }

  @Test
  void aFailedRowGoesToTheErrorHopOnly() throws Exception {
    run(
        new Object[] {"good1", "insert"},
        new Object[] {"bad", "insert"},
        new Object[] {"good2", "insert"});

    assertEquals(List.of("good1", "good2"), output);
    assertEquals(List.of("bad"), errors);
    assertEquals(0, transform.getErrors());
  }

  @Test
  void aSkippedRowStillGoesToTheOutput() throws Exception {
    run(new Object[] {"other", "nothing to do"}, new Object[] {"good", "insert"});

    assertEquals(List.of("other", "good"), output);
    assertTrue(errors.isEmpty());
  }
}
