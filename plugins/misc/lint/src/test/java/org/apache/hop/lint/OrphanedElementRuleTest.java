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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What TRANS-002 counts as an orphan.
 *
 * <p>It used to be answered for the pipeline as a whole, which meant the warning could not name the
 * transform it was about, and it counted two things as orphaned that are not: a pipeline holding a
 * single transform, and a transform whose hops happen to be disabled.
 *
 * @see <a href="https://github.com/apache/hop/issues/8294">#8294</a>
 */
public class OrphanedElementRuleTest {

  @AfterEach
  public void clearSubject() {
    CustomRuleExecutor.setSubject(null);
  }

  @Test
  public void aTransformConnectedToNothingIsReportedByName() {
    PipelineMeta pipeline = pipelineOf("read", "write", "left over");
    connect(pipeline, "read", "write");

    List<LintResult> findings = lint(pipeline);

    assertEquals(1, findings.size(), "only the disconnected transform: " + findings);
    assertEquals("left over", findings.get(0).getSource().getName());
  }

  /** A one-transform pipeline has nothing to be disconnected from. */
  @Test
  public void theOnlyTransformInAPipelineIsNotAnOrphan() {
    assertTrue(lint(pipelineOf("do the thing")).isEmpty());
  }

  /**
   * A disabled hop is still a hop. Whether one is a problem is what STRUCT-003 asks, and that ships
   * switched off because most teams treat a disabled hop as work in progress.
   */
  @Test
  public void aTransformWhoseHopsAreDisabledIsNotAnOrphan() {
    PipelineMeta pipeline = pipelineOf("read", "work in progress");
    connect(pipeline, "read", "work in progress").setEnabled(false);

    assertTrue(lint(pipeline).isEmpty(), "a disabled hop is not the same as no hop");
  }

  /** A pipeline of several transforms and no hops at all is still every one of them. */
  @Test
  public void transformsInAPipelineWithNoHopsAreAllOrphans() {
    assertEquals(3, lint(pipelineOf("a", "b", "c")).size());
  }

  private static List<LintResult> lint(PipelineMeta pipeline) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("TRANS-002");
    rule.setName("Orphaned Transform");
    rule.setSeverity("WARNING");
    rule.setEnabled(true);
    rule.setTarget(RuleTarget.TRANSFORM);
    rule.setTargetField("isOrphaned");
    rule.setCondition(RuleCondition.MUST_BE_FALSE);

    CustomRuleExecutor.setSubject(pipeline);
    List<LintResult> findings = new ArrayList<>();
    for (TransformMeta transformMeta : pipeline.getTransforms()) {
      findings.addAll(CustomRuleExecutor.executeRule(rule, transformMeta, "/tmp/orphans.hpl"));
    }
    return findings;
  }

  private static PipelineMeta pipelineOf(String... transformNames) {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("orphans");
    for (String name : transformNames) {
      pipeline.addTransform(new TransformMeta("Dummy", name, null));
    }
    return pipeline;
  }

  private static PipelineHopMeta connect(PipelineMeta pipeline, String from, String to) {
    PipelineHopMeta hop =
        new PipelineHopMeta(pipeline.findTransform(from), pipeline.findTransform(to));
    pipeline.addPipelineHop(hop);
    return hop;
  }
}
