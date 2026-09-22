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
package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * Clearing the database cache has to invalidate the row metadata a pipeline cached for transforms
 * which read their layout from a database. See <a
 * href="https://github.com/apache/hop/issues/3312">issue 3312</a>: without this, the "clear cache"
 * buttons only took effect after the pipeline was reloaded.
 */
class PipelineMetaDbCacheInvalidationTest {

  /** Stands in for the columns the database currently reports for the transform's query. */
  private final List<String> databaseColumns = new ArrayList<>(List.of("a", "b"));

  private IVariables variables;
  private PipelineMeta pipelineMeta;
  private TransformMeta databaseTransform;

  @BeforeEach
  void setUp() throws Exception {
    DbCache.getInstance().clear(null);
    variables = new Variables();

    databaseTransform = new TransformMeta("database input", databaseBackedTransformMeta());
    TransformMeta after = new TransformMeta("after", new DummyMeta());

    pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(databaseTransform);
    pipelineMeta.addTransform(after);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(databaseTransform, after));
  }

  @Test
  void fieldsAreCachedUntilTheDatabaseCacheIsCleared() throws Exception {
    assertArrayEquals(new String[] {"a", "b"}, transformFields());

    // The table gains a column outside of Hop, so what we cached is now stale.
    databaseColumns.add("c");
    assertArrayEquals(new String[] {"a", "b"}, transformFields());

    // Which is exactly what the "clear cache" buttons in the GUI are for.
    DbCache.getInstance().clear(null);
    assertArrayEquals(new String[] {"a", "b", "c"}, transformFields());
  }

  @Test
  void clearingASingleConnectionAlsoInvalidatesCachedFields() throws Exception {
    assertArrayEquals(new String[] {"a", "b"}, transformFields());

    databaseColumns.add("c");
    DbCache.getInstance().clear("some connection");

    assertArrayEquals(new String[] {"a", "b", "c"}, transformFields());
  }

  private String[] transformFields() throws HopTransformException {
    return pipelineMeta
        .getTransformFields(variables, databaseTransform, null, mock(IProgressMonitor.class))
        .getFieldNames();
  }

  /** A transform which reports whatever columns the database currently has, like Table Input. */
  private ITransformMeta databaseBackedTransformMeta() throws HopTransformException {
    TransformIOMeta transformIOMeta = mock(TransformIOMeta.class);
    when(transformIOMeta.getInfoTransformNames()).thenReturn(new String[0]);

    ITransformMeta transformMeta = spy(new DummyMeta());
    when(transformMeta.getTransformIOMeta()).thenReturn(transformIOMeta);
    doAnswer(
            (Answer<Void>)
                invocation -> {
                  IRowMeta rowMeta = (IRowMeta) invocation.getArguments()[0];
                  databaseColumns.forEach(
                      column -> rowMeta.addValueMeta(new ValueMetaString(column)));
                  return null;
                })
        .when(transformMeta)
        .getFields(any(), any(), any(), any(), any(), any());

    return transformMeta;
  }
}
