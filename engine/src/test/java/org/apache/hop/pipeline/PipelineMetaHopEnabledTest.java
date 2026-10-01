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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Disabling a hop needs to invalidate the caches in {@link PipelineMeta}. If it doesn't, the engine
 * allocates row sets with {@link PipelineMeta#findNextTransforms(TransformMeta)} (uncached, so the
 * disabled hop is skipped) while the transforms look up their input with {@link
 * PipelineMeta#findPreviousTransforms(TransformMeta, boolean)} (cached, so the disabled hop is
 * still there). The transform then fails to initialize with "Unable to find input rowset!". See
 * issue #7841.
 */
class PipelineMetaHopEnabledTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  /** Two transforms A and C feeding into transform B. */
  private PipelineMeta createPipelineMeta() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("hop enabled test");
    TransformMeta a = new TransformMeta("A", new DummyMeta());
    TransformMeta c = new TransformMeta("C", new DummyMeta());
    TransformMeta b = new TransformMeta("B", new DummyMeta());
    pipelineMeta.addTransform(a);
    pipelineMeta.addTransform(c);
    pipelineMeta.addTransform(b);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(a, b));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(c, b));
    return pipelineMeta;
  }

  @Test
  void disablingHopInvalidatesPreviousTransformsCache() {
    PipelineMeta pipelineMeta = createPipelineMeta();
    TransformMeta b = pipelineMeta.findTransform("B");

    // Populate the cache while both hops are enabled.
    //
    assertEquals(2, pipelineMeta.findPreviousTransforms(b, true).size());

    pipelineMeta.setHopEnabled(pipelineMeta.getPipelineHop(1), false);

    assertEquals(1, pipelineMeta.findPreviousTransforms(b, true).size());
    assertEquals("A", pipelineMeta.findPreviousTransforms(b, true).get(0).getName());
  }

  @Test
  void disablingHopAfterARunDoesNotBreakTheNextRun() throws Exception {
    PipelineMeta pipelineMeta = createPipelineMeta();

    // A first execution populates the caches while all hops are still enabled.
    //
    LocalPipelineEngine firstRun =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    firstRun.prepareExecution();
    firstRun.startThreads();
    firstRun.waitUntilFinished();

    pipelineMeta.setHopEnabled(pipelineMeta.getPipelineHop(1), false);

    LocalPipelineEngine secondRun =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    secondRun.prepareExecution();
    secondRun.startThreads();
    secondRun.waitUntilFinished();

    assertTrue(secondRun.getErrors() == 0, "the run after disabling a hop reported errors");
  }
}
