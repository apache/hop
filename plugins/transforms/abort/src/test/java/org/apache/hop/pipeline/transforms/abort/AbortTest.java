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

package org.apache.hop.pipeline.transforms.abort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AbortTest {
  private TransformMockHelper<AbortMeta, AbortData> transformMockHelper;

  @BeforeEach
  public void setup() {
    transformMockHelper = new TransformMockHelper("ABORT TEST", AbortMeta.class, AbortData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  public void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testAbortDoesntAbortWithoutInputRow() throws HopException {
    Abort abort =
        new Abort(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    abort.processRow();
    abort.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet());
    assertFalse(abort.isStopped());
    abort.processRow();
    verify(transformMockHelper.pipeline, never()).stopAll();
    assertFalse(abort.isStopped());
  }

  @Test
  public void testAbortAbortsWithInputRow() throws HopException {
    Abort abort =
        new Abort(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    abort.processRow();
    abort.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));
    assertFalse(abort.isStopped());
    abort.processRow();
    verify(transformMockHelper.pipeline, times(1)).stopAll();
    assertTrue(abort.isStopped());
  }

  @Test
  public void testAbortWithError() throws HopException {
    Abort abort =
        new Abort(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    when(transformMockHelper.iTransformMeta.isSafeStop()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isAbortWithError()).thenReturn(true);
    abort.processRow();
    abort.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));
    abort.processRow();
    assertEquals(1L, abort.getErrors());
    verify(transformMockHelper.pipeline).stopAll();
  }
}
