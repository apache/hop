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

package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.transforms.FakeMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for error-handling metadata vs hop enabled state (issue #7303). */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class TransformMetaErrorHandlingTest {

  @Test
  void isDoingErrorHandlingFalseWhenErrorHopDisabled() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta source = new TransformMeta("REST client", new FakeMeta());
    TransformMeta errorTarget = new TransformMeta("Write to log error", new FakeMeta());
    pipelineMeta.addTransform(source);
    pipelineMeta.addTransform(errorTarget);

    PipelineHopMeta errorHop = new PipelineHopMeta(source, errorTarget);
    errorHop.setEnabled(false);
    pipelineMeta.addPipelineHop(errorHop);

    TransformErrorMeta errorMeta = new TransformErrorMeta(source, errorTarget);
    errorMeta.setEnabled(true);
    source.setTransformErrorMeta(errorMeta);
    source.setParentPipelineMeta(pipelineMeta);

    assertFalse(source.isDoingErrorHandling());
  }

  @Test
  void isDoingErrorHandlingTrueWhenErrorHopEnabled() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta source = new TransformMeta("REST client", new FakeMeta());
    TransformMeta errorTarget = new TransformMeta("Write to log error", new FakeMeta());
    pipelineMeta.addTransform(source);
    pipelineMeta.addTransform(errorTarget);

    PipelineHopMeta errorHop = new PipelineHopMeta(source, errorTarget);
    errorHop.setEnabled(true);
    pipelineMeta.addPipelineHop(errorHop);

    TransformErrorMeta errorMeta = new TransformErrorMeta(source, errorTarget);
    errorMeta.setEnabled(true);
    source.setTransformErrorMeta(errorMeta);
    source.setParentPipelineMeta(pipelineMeta);

    assertTrue(source.isDoingErrorHandling());
  }

  @Test
  void isDoingErrorHandlingUsesErrorMetaWhenNoParentPipeline() {
    TransformMeta source = new TransformMeta("REST client", new FakeMeta());
    TransformMeta errorTarget = new TransformMeta("Write to log error", new FakeMeta());

    TransformErrorMeta errorMeta = new TransformErrorMeta(source, errorTarget);
    errorMeta.setEnabled(true);
    source.setTransformErrorMeta(errorMeta);

    assertTrue(source.isDoingErrorHandling());
  }
}
