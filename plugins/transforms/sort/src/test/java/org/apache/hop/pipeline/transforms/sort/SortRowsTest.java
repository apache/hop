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
package org.apache.hop.pipeline.transforms.sort;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SortRowsTest {

  @Test
  void processRowFirstCallInitializesGroupFieldsAndBatchState() throws Exception {
    SortRowsMeta meta = new SortRowsMeta();
    meta.setSortFields(
        Collections.singletonList(new SortRowsField("id", true, true, false, 0, false)));
    SortRowsData data = new SortRowsData();
    SortRows sortRows = newSortRows(meta, data);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    sortRows.setInputRowMeta(inputRowMeta);

    Object[] firstRow = new Object[] {"1"};

    Method processRowFirstCall =
        SortRows.class.getDeclaredMethod("processRowFirstCall", Object[].class);
    processRowFirstCall.setAccessible(true);
    boolean done = (boolean) processRowFirstCall.invoke(sortRows, (Object) firstRow);

    assertFalse(done);
    assertArrayEquals(new int[0], data.groupnrs);
    assertTrue(data.newBatch);
  }

  @Test
  void processRowFirstCallUsesPresortedFieldsForGroupIndices() throws Exception {
    SortRowsMeta meta = new SortRowsMeta();
    meta.setSortFields(
        Collections.singletonList(new SortRowsField("id", true, true, false, 0, true)));
    SortRowsData data = new SortRowsData();
    SortRows sortRows = newSortRows(meta, data);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    sortRows.setInputRowMeta(inputRowMeta);

    Method processRowFirstCall =
        SortRows.class.getDeclaredMethod("processRowFirstCall", Object[].class);
    processRowFirstCall.setAccessible(true);
    processRowFirstCall.invoke(sortRows, (Object) new Object[] {"1"});

    assertArrayEquals(new int[] {0}, data.groupnrs);
  }

  @Test
  void sameGroupReturnsFalseWhenPreviousRowIsNull() throws Exception {
    SortRowsMeta meta = new SortRowsMeta();
    SortRowsData data = new SortRowsData();
    data.fieldnrs = new int[] {0};
    data.groupnrs = new int[] {0};
    SortRows sortRows = newSortRows(meta, data);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    sortRows.setInputRowMeta(inputRowMeta);

    Method sameGroup =
        SortRows.class.getDeclaredMethod("sameGroup", Object[].class, Object[].class);
    sameGroup.setAccessible(true);

    Object[] row = new Object[] {"1"};
    assertDoesNotThrow(() -> sameGroup.invoke(sortRows, null, row));
    boolean result = (boolean) sameGroup.invoke(sortRows, null, row);
    assertFalse(result);
  }

  private SortRows newSortRows(SortRowsMeta meta, SortRowsData data) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("SortRows");
    Pipeline pipeline = Mockito.mock(Pipeline.class);
    when(pipeline.getVariableNames()).thenReturn(new String[0]);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);
    return new SortRows(transformMeta, meta, data, 0, pipelineMeta, pipeline);
  }
}
