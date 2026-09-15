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

import java.util.List;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.Test;

public class TransformFieldExtractionTest {

  @Test
  public void flagsBlockingTransformPluginIds() throws Exception {
    TransformMeta sortRows = new TransformMeta();
    sortRows.setName("Sort rows");
    sortRows.setTransformPluginId("SortRows");

    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.TRANSFORM);
    rule.setTargetField("pluginId");
    rule.setCondition(RuleCondition.NOT_MATCHES_PATTERN);
    rule.setConditionValue(".*(SortRows|BlockingTransform|MemoryGroupBy|GroupBy).*");
    rule.setName("Blocking Transform Detection");
    rule.setDescription("Blocking transform");

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, sortRows, "/tmp/test.hpl");

    assertEquals(1, results.size());
    assertTrue(results.get(0).getMessage().contains("Blocking transform"));
  }

  @Test
  public void flagsHighCopyCount() throws Exception {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("Table Input");
    transformMeta.setTransformPluginId("TableInput");
    transformMeta.setCopies(12);

    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.TRANSFORM);
    rule.setTargetField("copies");
    rule.setCondition(RuleCondition.MAX_VALUE);
    rule.setConditionValue("10");
    rule.setName("High Transform Copy Count");
    rule.setDescription("Too many copies");

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, transformMeta, "/tmp/test.hpl");

    assertEquals(1, results.size());
  }

  /**
   * A rule naming the key Hop serialises a property under must resolve, and must keep resolving
   * when the Java field behind it is called something else. The serialised name is what a rule
   * author reads in the .hpl file; the Java field name is not API.
   */
  @Test
  public void resolvesAFieldByItsSerialisedName() throws Exception {
    AnnotatedMeta annotated = new AnnotatedMeta();
    annotated.internalFieldName = "SELECT * FROM sales";

    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("Read sales");
    transformMeta.setTransformPluginId("TableInput");
    transformMeta.setTransform(annotated);

    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("ERROR");
    rule.setTarget(RuleTarget.TRANSFORM);
    // "sql" is the serialised key; the Java field is called something else entirely.
    rule.setTargetField("sql");
    rule.setCondition(RuleCondition.NOT_MATCHES_PATTERN);
    rule.setConditionValue("(?is).*\\bselect\\s+\\*.*");
    rule.setName("SELECT * in SQL");
    rule.setDescription("Avoid SELECT *");

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, transformMeta, "/tmp/test.hpl");

    assertEquals(1, results.size());
  }

  /** A stand-in for a transform meta whose serialised key differs from its Java field name. */
  public static class AnnotatedMeta extends BaseTransformMeta<ITransform, ITransformData> {
    @HopMetadataProperty(key = "sql")
    private String internalFieldName;
  }

  /**
   * Notes on the canvas are one of the four checks the feature request asked for, so the field a
   * rule needs to see them has to exist for pipelines and workflows alike.
   */
  @Test
  public void flagsAPipelineWithNoNotesOnTheCanvas() throws Exception {
    PipelineMeta bare = new PipelineMeta();
    bare.setName("undocumented");

    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.PIPELINE);
    rule.setTargetField("hasNotes");
    rule.setCondition(RuleCondition.MUST_BE_TRUE);
    rule.setName("Pipeline Notes Required");
    rule.setDescription("Explain the pipeline on the canvas");

    assertEquals(1, CustomRuleExecutor.executeRule(rule, bare, "/tmp/test.hpl").size());

    bare.addNote(new NotePadMeta("what this does", 10, 10, 100, 40));
    assertEquals(0, CustomRuleExecutor.executeRule(rule, bare, "/tmp/test.hpl").size());
  }

  @Test
  public void countsNotesOnAWorkflow() throws Exception {
    WorkflowMeta workflow = new WorkflowMeta();
    workflow.setName("undocumented");
    workflow.addNote(new NotePadMeta("step one", 10, 10, 100, 40));
    workflow.addNote(new NotePadMeta("step two", 10, 60, 100, 40));

    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.WORKFLOW);
    rule.setTargetField("noteCount");
    rule.setCondition(RuleCondition.MIN_VALUE);
    rule.setConditionValue("3");
    rule.setName("Workflow Notes");
    rule.setDescription("Not enough notes");

    assertEquals(1, CustomRuleExecutor.executeRule(rule, workflow, "/tmp/test.hwf").size());
  }
}
