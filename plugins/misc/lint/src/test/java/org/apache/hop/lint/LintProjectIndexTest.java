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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
  void nothingIsReferencedInAnEmptyIndex() {
    LintProjectIndex index = LintProjectIndex.empty();

    assertFalse(index.isFileReferenced("/project/pipelines/a.hpl"));
    assertFalse(index.isConnectionReferenced("sales"));
    assertFalse(index.isFileReferenced(null));
    assertFalse(index.isConnectionReferenced(null));
  }
}
