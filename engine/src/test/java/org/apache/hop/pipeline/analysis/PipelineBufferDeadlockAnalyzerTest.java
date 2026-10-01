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

package org.apache.hop.pipeline.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.analysis.BufferDeadlockRisk.SpillHop;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class PipelineBufferDeadlockAnalyzerTest {

  @Test
  void linearChainHasNoRisk() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("A");
    TransformMeta b = transform("B");
    TransformMeta c = transform("C");
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    meta.addPipelineHop(new PipelineHopMeta(a, b));
    meta.addPipelineHop(new PipelineHopMeta(b, c));

    assertTrue(PipelineBufferDeadlockAnalyzer.analyze(meta).isEmpty());
  }

  @Test
  void splitRejoinReportsRiskAndSpillHops() {
    // Source ──┬──► Left  ──► Join
    //          └──► Right ──► Join
    PipelineMeta meta = new PipelineMeta();
    TransformMeta source = transform("Source");
    TransformMeta left = transform("Left");
    TransformMeta right = transform("Right");
    TransformMeta join = transform("Join");
    meta.addTransform(source);
    meta.addTransform(left);
    meta.addTransform(right);
    meta.addTransform(join);
    meta.addPipelineHop(new PipelineHopMeta(source, left));
    meta.addPipelineHop(new PipelineHopMeta(source, right));
    meta.addPipelineHop(new PipelineHopMeta(left, join));
    meta.addPipelineHop(new PipelineHopMeta(right, join));

    List<BufferDeadlockRisk> risks = PipelineBufferDeadlockAnalyzer.analyze(meta);
    assertEquals(1, risks.size());
    BufferDeadlockRisk risk = risks.getFirst();
    assertEquals("Join", risk.reconvergence().getName());
    assertEquals("Source", risk.commonAncestor().getName());
    assertEquals(2, risk.inboundPredecessors().size());
    assertEquals(2, risk.spillHops().size());
    assertTrue(PipelineBufferDeadlockAnalyzer.shouldSpill(risk.spillHops(), "Left", "Join"));
    assertTrue(PipelineBufferDeadlockAnalyzer.shouldSpill(risk.spillHops(), "Right", "Join"));
    assertFalse(PipelineBufferDeadlockAnalyzer.shouldSpill(risk.spillHops(), "Source", "Left"));
  }

  @Test
  void independentSourcesNoRisk() {
    // Src1 → Left  ──► Join
    // Src2 → Right ──► Join
    PipelineMeta meta = new PipelineMeta();
    TransformMeta src1 = transform("Src1");
    TransformMeta src2 = transform("Src2");
    TransformMeta left = transform("Left");
    TransformMeta right = transform("Right");
    TransformMeta join = transform("Join");
    meta.addTransform(src1);
    meta.addTransform(src2);
    meta.addTransform(left);
    meta.addTransform(right);
    meta.addTransform(join);
    meta.addPipelineHop(new PipelineHopMeta(src1, left));
    meta.addPipelineHop(new PipelineHopMeta(src2, right));
    meta.addPipelineHop(new PipelineHopMeta(left, join));
    meta.addPipelineHop(new PipelineHopMeta(right, join));

    assertTrue(PipelineBufferDeadlockAnalyzer.analyze(meta).isEmpty());
  }

  @Test
  void collectSpillHopsUnionsRisks() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta source = transform("Source");
    TransformMeta left = transform("Left");
    TransformMeta right = transform("Right");
    TransformMeta join = transform("Join");
    meta.addTransform(source);
    meta.addTransform(left);
    meta.addTransform(right);
    meta.addTransform(join);
    meta.addPipelineHop(new PipelineHopMeta(source, left));
    meta.addPipelineHop(new PipelineHopMeta(source, right));
    meta.addPipelineHop(new PipelineHopMeta(left, join));
    meta.addPipelineHop(new PipelineHopMeta(right, join));

    Set<SpillHop> hops =
        PipelineBufferDeadlockAnalyzer.collectSpillHops(
            PipelineBufferDeadlockAnalyzer.analyze(meta));
    assertEquals(2, hops.size());
  }

  @Test
  void nullMetaReturnsEmpty() {
    assertTrue(PipelineBufferDeadlockAnalyzer.analyze(null).isEmpty());
  }

  private static TransformMeta transform(String name) {
    return new TransformMeta(name, new DummyMeta());
  }
}
