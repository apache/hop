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

package org.apache.hop.workflow.actions.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

/** Which rows an action hands to the next action once its pipeline finished (#4581, #4559). */
class ActionPipelineResultRowsTest {

  @Test
  void copyRowsToResultWithNoRowsReplacesPreviousRows() throws Exception {
    Result result = resultWithRows(3);

    updateResult(result, pipelineResult(new ArrayList<>()), "RowsToResult", "Dummy");

    assertTrue(result.getRows().isEmpty());
  }

  @Test
  void copyRowsToResultReplacesPreviousRows() throws Exception {
    Result result = resultWithRows(3);
    List<RowMetaAndData> newRows = rows(1);

    updateResult(result, pipelineResult(newRows), "RowsToResult");

    assertSame(newRows, result.getRows());
  }

  @Test
  void pipelineWithoutCopyRowsToResultPassesPreviousRowsOn() throws Exception {
    Result result = resultWithRows(3);
    List<RowMetaAndData> previousRows = result.getRows();

    updateResult(result, pipelineResult(new ArrayList<>()), "Dummy", "WriteToLog");

    assertSame(previousRows, result.getRows());
    assertEquals(3, result.getRows().size());
  }

  @Test
  void missingRowListFromTheEngineBecomesAnEmptyList() throws Exception {
    Result result = resultWithRows(3);

    updateResult(result, pipelineResult(null), "RowsToResult");

    assertNotNull(result.getRows());
    assertTrue(result.getRows().isEmpty());
  }

  @SuppressWarnings("unchecked")
  private static void updateResult(Result result, Result pipelineResult, String... pluginIds)
      throws Exception {
    List<TransformMeta> transforms = new ArrayList<>();
    for (String pluginId : pluginIds) {
      TransformMeta transformMeta = mock(TransformMeta.class);
      when(transformMeta.getTransformPluginId()).thenReturn(pluginId);
      transforms.add(transformMeta);
    }
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(pipelineMeta.getTransforms()).thenReturn(transforms);

    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pipelineMeta);
    when(pipeline.getResult()).thenReturn(pipelineResult);

    ActionPipeline action = new ActionPipeline("run-child");
    Field field = ActionPipeline.class.getDeclaredField("pipeline");
    field.setAccessible(true);
    field.set(action, pipeline);

    action.updateResult(result);
  }

  private static Result pipelineResult(List<RowMetaAndData> rows) {
    Result result = new Result();
    result.setRows(rows);
    return result;
  }

  private static Result resultWithRows(int count) {
    return pipelineResult(rows(count));
  }

  private static List<RowMetaAndData> rows(int count) {
    List<RowMetaAndData> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(new RowMetaAndData());
    }
    return rows;
  }
}
