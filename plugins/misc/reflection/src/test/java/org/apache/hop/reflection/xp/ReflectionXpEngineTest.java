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

package org.apache.hop.reflection.xp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.reflection.pipeline.xp.PipelineStartLoggingXp;
import org.apache.hop.reflection.probe.xp.PipelineDataProbeXp;
import org.junit.jupiter.api.Test;

/**
 * The pipeline log and data probe listeners work on Hop's own pipeline engine. Another engine
 * firing the same extension points is left alone instead of failing.
 *
 * <p>The listeners are called through the raw {@link IExtensionPoint}, as the extension point
 * handler calls them, so the engine object is passed whatever its type.
 */
class ReflectionXpEngineTest {

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void anotherPipelineEngineIsLeftAlone() {
    IPipelineEngine<PipelineMeta> engine = mock(IPipelineEngine.class);

    for (IExtensionPoint extensionPoint :
        new IExtensionPoint[] {new PipelineStartLoggingXp(), new PipelineDataProbeXp()}) {
      assertDoesNotThrow(
          () -> extensionPoint.callExtensionPoint(LogChannel.GENERAL, new Variables(), engine),
          extensionPoint.getClass().getSimpleName());
    }
    verifyNoInteractions(engine);
  }

  /** On the local engine they go on: here up to the check that stops them logging themselves. */
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void theLocalEngineIsHandled() throws Exception {
    LocalPipelineEngine pipeline = mock(LocalPipelineEngine.class);
    Map<String, Object> extensionData = new HashMap<>();
    extensionData.put(PipelineStartLoggingXp.PIPELINE_LOGGING_FLAG, true);
    extensionData.put(PipelineDataProbeXp.PIPELINE_DATA_PROBE_FLAG, true);
    when(pipeline.getExtensionDataMap()).thenReturn(extensionData);

    ((IExtensionPoint) new PipelineStartLoggingXp())
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), pipeline);
    ((IExtensionPoint) new PipelineDataProbeXp())
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), pipeline);

    verify(pipeline, times(2)).getExtensionDataMap();
  }
}
