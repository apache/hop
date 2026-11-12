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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class LintProjectIndexTest {

  @AfterEach
  void clearIndex() {
    CustomRuleExecutor.setProjectIndex(null);
  }

  private static CustomLintRule unreferencedPipelineRule() {
    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.PIPELINE);
    rule.setTargetField("isReferenced");
    rule.setCondition(RuleCondition.MUST_BE_TRUE);
    rule.setName("Unreferenced Pipeline");
    rule.setDescription("Nothing calls this pipeline");
    return rule;
  }

  /**
   * The behaviour that matters most: without a project to look at, the rule has to stay silent.
   * Reporting "nothing calls this" from a single-file lint would be a false positive every time,
   * and false positives are what get a linter switched off.
   */
  @Test
  void aRuleNeedingTheProjectIsSkippedWhenThereIsNoIndex() {
    PipelineMeta meta = new PipelineMeta();
    meta.setName("orphan");
    meta.setFilename("/project/pipelines/orphan.hpl");

    List<LintResult> results =
        CustomRuleExecutor.executeRule(
            unreferencedPipelineRule(), meta, "/project/pipelines/orphan.hpl");

    assertTrue(results.isEmpty(), "no project index means no verdict");
  }

  @Test
  void anEmptyIndexIsTreatedAsNoIndexAtAll() {
    CustomRuleExecutor.setProjectIndex(LintProjectIndex.empty());
    PipelineMeta meta = new PipelineMeta();
    meta.setName("orphan");
    meta.setFilename("/project/pipelines/orphan.hpl");

    List<LintResult> results =
        CustomRuleExecutor.executeRule(
            unreferencedPipelineRule(), meta, "/project/pipelines/orphan.hpl");

    assertTrue(results.isEmpty());
    assertFalse(LintProjectIndex.empty().isPopulated());
  }

  @Test
  void anIndexOverRealFilesReportsWhatItIndexed() {
    // No references to find in a list of files which do not exist, but the index still records
    // that it was built, which is what separates it from empty().
    LintProjectIndex index =
        LintProjectIndex.build(
            List.of("/project/pipelines/a.hpl", "/project/workflows/b.hwf"), null, null);

    assertTrue(index.isPopulated());
    assertEquals(2, index.getIndexedFiles().size());
    assertFalse(index.isFileReferenced("/project/pipelines/a.hpl"));
  }

  @Test
  void pipelineParameterDefaultsAreUsedWhenReadingReferences() throws Exception {
    Variables variables = new Variables();
    TransformMeta transformMeta = mock(TransformMeta.class);
    ITransformMeta transform = mock(ITransformMeta.class);
    AtomicReference<String> resolvedMask = new AtomicReference<>();
    when(transformMeta.getTransform()).thenReturn(transform);
    when(transform.getResourceDependencies(any(IVariables.class), any(TransformMeta.class)))
        .thenAnswer(
            invocation -> {
              IVariables pipelineVariables = invocation.getArgument(0);
              String mask = pipelineVariables.resolve("person-info-${PRM_TYPE}.csv");
              resolvedMask.set(mask);
              Pattern.compile(mask);
              return List.of();
            });

    try (MockedConstruction<PipelineMeta> ignored =
        mockConstruction(
            PipelineMeta.class,
            (meta, context) -> {
              when(meta.getTransforms()).thenReturn(List.of(transformMeta));
              when(meta.listParameters()).thenReturn(new String[] {"PRM_TYPE"});
              when(meta.getParameterDefault("PRM_TYPE")).thenReturn("1");
            })) {
      LintProjectIndex.build(List.of("/project/process-files-with-mdi.hpl"), null, variables);
    }

    assertEquals("person-info-1.csv", resolvedMask.get());
    assertNull(variables.getVariable("PRM_TYPE"), "the lint run variables must not be changed");
  }

  @Test
  void nothingIsReferencedInAnEmptyIndex() {
    LintProjectIndex index = LintProjectIndex.empty();

    assertFalse(index.isFileReferenced("/project/pipelines/a.hpl"));
    assertFalse(index.isConnectionReferenced("sales"));
    assertFalse(index.isFileReferenced(null));
    assertFalse(index.isConnectionReferenced(null));
  }
}
