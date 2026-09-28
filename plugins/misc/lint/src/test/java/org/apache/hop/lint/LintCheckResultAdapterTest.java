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

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.metadata.validation.ReferencedDatabaseConnectionChecker;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

public class LintCheckResultAdapterTest {

  @Test
  public void deduplicatorPrefersLintOverNativeForSameBucket() {
    LintResult lint =
        new LintResult(
            "TRANS-002",
            "Orphaned Transform",
            "WARNING",
            "Transform is not used",
            "/tmp/test.hpl",
            LintSourceRef.transform("Unused"),
            LintResult.Origin.LINT);
    LintResult nativeResult =
        new LintResult(
            "HOP-CHECK",
            "Unused",
            "WARNING",
            "Transform is not used in the pipeline",
            "/tmp/test.hpl",
            LintSourceRef.transform("Unused"),
            LintResult.Origin.HOP_NATIVE);

    List<LintResult> deduped =
        LintResultDeduplicator.deduplicate(Arrays.asList(lint, nativeResult));
    assertEquals(1, deduped.size());
    assertEquals(LintResult.Origin.LINT, deduped.get(0).getOrigin());
  }

  @Test
  public void severityFailOnThreshold() {
    assertTrue(LintSeverity.meetsFailOnThreshold("ERROR", LintSeverity.FailOn.ERROR));
    assertTrue(LintSeverity.meetsFailOnThreshold("ERROR", LintSeverity.FailOn.WARNING));
    assertTrue(LintSeverity.meetsFailOnThreshold("WARNING", LintSeverity.FailOn.WARNING));
    assertTrue(!LintSeverity.meetsFailOnThreshold("WARNING", LintSeverity.FailOn.ERROR));
  }

  @Test
  public void pipelineLevelLintHasNoTransformSource() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("My Pipeline");
    LintResult doc =
        new LintResult(
            "DOC-001",
            "Pipeline Description Required",
            "WARNING",
            "Missing description",
            "/tmp/test.hpl",
            LintSourceRef.pipeline("My Pipeline"),
            LintResult.Origin.LINT);

    org.apache.hop.core.ICheckResult check =
        LintCheckResultAdapter.toCheckResult(doc, pipelineMeta);

    org.junit.jupiter.api.Assertions.assertNotNull(check);
    org.junit.jupiter.api.Assertions.assertNull(check.getSourceInfo());
  }

  /**
   * A check that sets its own error code is reported under it. The blanket rule names every remark,
   * so taking its id instead left a project unable to tell one check from another.
   *
   * @see <a href="https://github.com/apache/hop/issues/8536">#8536</a>
   */
  @Test
  public void aChecksOwnErrorCodeSurvivesTheBlanketRule() {
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING")));

    LintResult result =
        LintCheckResultAdapter.fromCheckResult(
            missingConnectionRemark(), "/tmp/test.hpl", classifier);

    assertEquals("CONNECTION_DOES_NOT_EXIST", result.getRuleId());
    assertEquals("HOP-CHECK", result.getAliasRuleId());
    assertEquals("WARNING", result.getSeverity(), "the blanket rule still sets the severity");
  }

  @Test
  public void aRemarkWithoutAnErrorCodeIsReportedUnderTheRuleThatClassifiedIt() {
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING")));

    LintResult result =
        LintCheckResultAdapter.fromCheckResult(
            new CheckResult(ICheckResult.TYPE_RESULT_ERROR, "boom", tableInput()),
            "/tmp/test.hpl",
            classifier);

    assertEquals("HOP-CHECK", result.getRuleId());
    assertEquals(List.of("HOP-CHECK"), result.getRuleIds());
  }

  /** A rule a project wrote for one plugin or one check is more specific than a code. */
  @Test
  public void aNarrowedRuleWinsOverTheErrorCode() {
    CustomLintRule tableInput = nativeRule("HOP-CHECK-TABLEINPUT", "ERROR");
    tableInput.setAppliesTo(List.of("TableInput"));
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING"), tableInput));

    LintResult result =
        LintCheckResultAdapter.fromCheckResult(
            missingConnectionRemark(), "/tmp/test.hpl", classifier);

    assertEquals("HOP-CHECK-TABLEINPUT", result.getRuleId());
    assertEquals("ERROR", result.getSeverity());
    assertEquals(
        "CONNECTION_DOES_NOT_EXIST",
        result.getAliasRuleId(),
        "a suppression naming the code must survive the project adding this rule");
  }

  /**
   * Naming one check must not quietly undo a suppression the project already had.
   *
   * <p>The narrowed rule takes the id, so the rule covering every remark stopped naming the finding
   * and a {@code suppress: HOP-CHECK} written beforehand no longer matched it.
   *
   * @see <a href="https://github.com/apache/hop/issues/8536">#8536</a>
   */
  @Test
  public void aNarrowedRuleKeepsTheBlanketRuleAsAName() {
    CustomLintRule tableInput = nativeRule("HOP-CHECK-TABLEINPUT", "ERROR");
    tableInput.setAppliesTo(List.of("TableInput"));
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING"), tableInput));

    LintResult result =
        LintCheckResultAdapter.fromCheckResult(
            missingConnectionRemark(), "/tmp/test.hpl", classifier);

    assertEquals(
        List.of("HOP-CHECK-TABLEINPUT", "CONNECTION_DOES_NOT_EXIST", "HOP-CHECK"),
        result.getRuleIds(),
        "the finding must still answer to the rule that covers every remark");
  }

  /** With no narrowed rule in force the blanket rule is the alias, and is not repeated. */
  @Test
  public void theBlanketRuleIsNamedOnceWhenNoRuleNarrows() {
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING")));

    LintResult result =
        LintCheckResultAdapter.fromCheckResult(
            missingConnectionRemark(), "/tmp/test.hpl", classifier);

    assertEquals(List.of("CONNECTION_DOES_NOT_EXIST", "HOP-CHECK"), result.getRuleIds());
  }

  private static ICheckResult missingConnectionRemark() {
    return new CheckResult(
        ICheckResult.TYPE_RESULT_WARNING,
        ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST,
        "Database connection 'warehouse' assigned on transform 'Table input' does not exist",
        tableInput());
  }

  private static TransformMeta tableInput() {
    return new TransformMeta("TableInput", "Table input", null);
  }

  private static CustomLintRule nativeRule(String id, String severity) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId(id);
    rule.setType(CustomLintRule.TYPE_NATIVE);
    rule.setSeverity(severity);
    rule.setEnabled(true);
    return rule;
  }
}
