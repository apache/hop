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

package org.apache.hop.pipeline.engines.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class LocalPipelineEngineTest {

  @BeforeAll
  static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  /**
   * Transforms acquire resources (database connections, files, ...) in init(). When the preparation
   * of the pipeline fails after that init() the transform threads are never started, so nothing
   * else will ever release them. See issue #5970.
   */
  @Test
  void transformsAreDisposedWhenPreparationFailsAfterInit() {
    LocalPipelineEngine pipeline =
        spy(
            new LocalPipelineEngine(new PipelineMeta()) {
              @Override
              public void addTransformExecutionSamplers() throws HopException {
                throw new HopException("Unable to attach the samplers");
              }
            });
    pipeline.setLogChannel(mock(ILogChannel.class));

    HopException exception = assertThrows(HopException.class, pipeline::prepareExecution);

    assertTrue(exception.getMessage().contains("Unable to attach the samplers"));
    verify(pipeline).disposeInitializedTransforms();
    assertTrue(pipeline.isFinished(), "The pipeline should be flagged as finished");
  }

  @Test
  void disposeInitializedTransformsDisposesEveryTransform() throws Exception {
    LocalPipelineEngine pipeline = new LocalPipelineEngine(new PipelineMeta());
    pipeline.setLogChannel(mock(ILogChannel.class));
    pipeline.prepareExecution();

    // One transform which fails to clean up shouldn't keep the others from doing so.
    //
    TransformMetaDataCombi failing =
        combi("failing", ComponentExecutionStatus.STATUS_IDLE, new RuntimeException("Oops"));
    TransformMetaDataCombi initialized =
        combi("initialized", ComponentExecutionStatus.STATUS_IDLE, null);
    TransformMetaDataCombi notInitialized =
        combi("not-initialized", ComponentExecutionStatus.STATUS_STOPPED, null);
    pipeline.getTransforms().add(failing);
    pipeline.getTransforms().add(initialized);
    pipeline.getTransforms().add(notInitialized);

    pipeline.disposeInitializedTransforms();

    verify(failing.transform).dispose();
    verify(initialized.transform).dispose();
    verify(notInitialized.transform).dispose();

    assertEquals(ComponentExecutionStatus.STATUS_HALTED, failing.data.getStatus());
    assertEquals(ComponentExecutionStatus.STATUS_HALTED, initialized.data.getStatus());
    assertEquals(
        ComponentExecutionStatus.STATUS_STOPPED,
        notInitialized.data.getStatus(),
        "A transform which never initialized should keep its stopped status");
  }

  private TransformMetaDataCombi combi(
      String name, ComponentExecutionStatus status, RuntimeException disposeError) {
    ITransform transform = mock(ITransform.class);
    if (disposeError != null) {
      doThrow(disposeError).when(transform).dispose();
    }
    ITransformData data = new BaseTransformData() {};
    data.setStatus(status);

    TransformMetaDataCombi combi = new TransformMetaDataCombi();
    combi.transformName = name;
    combi.transform = transform;
    combi.data = data;
    return combi;
  }
}
