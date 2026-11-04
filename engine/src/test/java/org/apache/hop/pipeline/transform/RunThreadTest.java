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

package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.Test;

class RunThreadTest {

  @Test
  void disposeFailureStillMarksTheTransformStopped() {
    TransformMockHelper<ITransformMeta, ITransformData> helper =
        new TransformMockHelper<>("run-thread", ITransformMeta.class, ITransformData.class);
    try {
      when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
          .thenReturn(helper.iLogChannel);

      BaseTransform<ITransformMeta, ITransformData> transform =
          new BaseTransform<>(
              helper.transformMeta,
              helper.iTransformMeta,
              helper.iTransformData,
              0,
              helper.pipelineMeta,
              helper.pipeline) {
            @Override
            public boolean processRow() {
              return false;
            }

            @Override
            public void dispose() {
              throw new RuntimeException("close failed");
            }
          };
      transform.setStopped(true);

      TransformMetaDataCombi combi = new TransformMetaDataCombi();
      combi.transform = transform;
      new RunThread(combi).run();

      assertFalse(transform.isRunning());
      assertEquals(ComponentExecutionStatus.STATUS_STOPPED, transform.getStatus());
      assertEquals(1L, transform.getErrors());
    } finally {
      helper.cleanUp();
    }
  }
}
