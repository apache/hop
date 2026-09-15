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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lint.registry.HopCoreRulePack;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.selectvalues.SelectField;
import org.apache.hop.pipeline.transforms.selectvalues.SelectMetadataChange;
import org.apache.hop.pipeline.transforms.selectvalues.SelectValuesMeta;
import org.junit.jupiter.api.Test;

/**
 * How the linter reports Hop's own verify remarks.
 *
 * @see <a href="https://github.com/apache/hop/issues/8294">#8294</a>
 */
public class NativeCheckClassifierTest {

  private static final String SELECT_VALUES_PACKAGE =
      "org.apache.hop.pipeline.transforms.selectvalues";

  @Test
  public void remarksAreUntouchedWhenNoRuleSpeaksAboutThem() {
    NativeCheckClassifier classifier = new NativeCheckClassifier(List.of());

    assertTrue(classifier.isEmpty());
    assertEquals(
        "ERROR", classifier.classify(remark(ICheckResult.TYPE_RESULT_ERROR, "boom")).severity());
  }

  /**
   * The finding in the issue: a perfectly good pipeline greeted the user with five red errors, all
   * of them Hop's own design-time advice rather than anything the linter could stand behind.
   */
  @Test
  public void theBlanketRuleCapsEveryRemarkAtItsSeverity() {
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING", true)));

    NativeCheckClassifier.Classification classification =
        classifier.classify(remark(ICheckResult.TYPE_RESULT_ERROR, "fields not found"));

    assertNotNull(classification);
    assertEquals("WARNING", classification.severity());
    assertEquals("HOP-CHECK", classification.ruleId(), "the finding stays suppressible by id");
  }

  @Test
  public void aProjectCanPutTheRemarksBackToTheSeverityTheTransformMeant() {
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "ERROR", true)));

    assertEquals(
        "ERROR",
        classifier.classify(remark(ICheckResult.TYPE_RESULT_ERROR, "fields not found")).severity());
  }

  @Test
  public void disablingTheBlanketRuleDropsEveryRemark() {
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING", false)));

    assertNull(classifier.classify(remark(ICheckResult.TYPE_RESULT_ERROR, "fields not found")));
  }

  @Test
  public void aRuleNamingAPluginLeavesOtherPluginsAlone() {
    CustomLintRule scoped = nativeRule("HOP-CHECK-SV", "INFO", true);
    scoped.setAppliesTo(List.of("SelectValues"));
    NativeCheckClassifier classifier =
        new NativeCheckClassifier(List.of(nativeRule("HOP-CHECK", "WARNING", true), scoped));

    assertEquals(
        "INFO",
        classifier
            .classify(remark(ICheckResult.TYPE_RESULT_ERROR, "anything", "SelectValues"))
            .severity());
    assertEquals(
        "WARNING",
        classifier
            .classify(remark(ICheckResult.TYPE_RESULT_ERROR, "anything", "TableInput"))
            .severity());
  }

  /**
   * The narrow rule has to win wherever it sits in the YAML: a pack holds both "native remarks are
   * warnings" and "this one check is wrong", and which one applies cannot depend on file order.
   */
  @Test
  public void theRuleNamingTheCheckWinsOverTheBlanketOne() {
    List<CustomLintRule> rules =
        List.of(
            selectValuesRule(
                "HOP-CHECK-SELECTVALUES-METADATA",
                "SelectValuesMeta.CheckResult.MetadataFieldsNotFound",
                false),
            nativeRule("HOP-CHECK", "WARNING", true));
    NativeCheckClassifier classifier = new NativeCheckClassifier(rules);

    assertNull(
        classifier.classify(metadataFieldsNotFoundRemark()),
        "the check the project switched off produces no finding at all");
    assertEquals(
        "WARNING",
        classifier
            .classify(remark(ICheckResult.TYPE_RESULT_ERROR, "something else", "SelectValues"))
            .severity(),
        "the blanket rule still covers the transform's other checks");
  }

  /** Select Values' remaining checks are real, and stay visible as warnings. */
  @Test
  public void theCorePackKeepsSelectValuesOtherChecksAsWarnings() {
    NativeCheckClassifier classifier = new NativeCheckClassifier(new HopCoreRulePack().loadRules());

    ICheckResult noInput =
        remark(
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(
                SELECT_VALUES_PACKAGE, "SelectValuesMeta.CheckResult.NoInputReceivedError"),
            "SelectValues");

    NativeCheckClassifier.Classification classification = classifier.classify(noInput);
    assertNotNull(classification);
    assertEquals("WARNING", classification.severity());
  }

  @Test
  public void aMessageKeyThatCannotBeResolvedNarrowsToNothing() {
    assertFalse(
        NativeCheckClassifier.printsMessage(
            "Meta-data fields that were not found in input stream:",
            SELECT_VALUES_PACKAGE + ":SelectValuesMeta.CheckResult.RenamedAtSomePoint",
            null),
        "an unresolvable key must match nothing rather than everything");
    assertFalse(NativeCheckClassifier.printsMessage("anything", "no-separator-here", null));
    assertFalse(NativeCheckClassifier.printsMessage("", SELECT_VALUES_PACKAGE + ":a.key", null));
  }

  /**
   * The message key is resolved through the bundle rather than matched as a pattern, which is what
   * lets a rule name a check without naming the English words it happens to use.
   */
  @Test
  public void aMessageKeyIsResolvedAgainstThePluginsOwnBundle() {
    for (String key :
        List.of(
            "SelectValuesMeta.CheckResult.MetadataFieldsNotFound",
            "SelectValuesMeta.CheckResult.DuplicateFieldsSpecified")) {
      String message = BaseMessages.getString(SELECT_VALUES_PACKAGE, key);
      assertFalse(
          message.startsWith("!") && message.endsWith("!"),
          "Select Values no longer prints " + key);
      assertTrue(
          NativeCheckClassifier.printsMessage(
              message + Const.CR + Const.CR + "\t\tvalueToSqrt",
              SELECT_VALUES_PACKAGE + ":" + key,
              SelectValuesMeta.class));
    }
  }

  /**
   * The transform from the issue, checked by Select Values itself.
   *
   * <p>Reproduced rather than hand-written: the point of naming a check by its message key is that
   * the rule matches what the transform actually prints, and only running {@code check()} proves
   * that. "valueToSqrt" renames "value" on the Select &amp; Alter tab and then sets the metadata on
   * the new name, which is ordinary and correct, and names "value" twice, which is how a value is
   * copied under a second name.
   */
  @Test
  public void thePipelineFromTheIssueProducesNoErrors() {
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta("SelectValues", "valueToSqrt", null);

    IRowMeta previousRow = new RowMeta();
    previousRow.addValueMeta(new ValueMetaNumber("value"));

    selectValuesFromTheIssue()
        .check(
            remarks,
            null,
            transformMeta,
            previousRow,
            new String[] {"input"},
            null,
            null,
            null,
            null);

    List<LintResult> reported =
        LintCheckResultAdapter.fromCheckResults(
            remarks,
            "/tmp/sqrt-mapping.hpl",
            new NativeCheckClassifier(new HopCoreRulePack().loadRules()));

    assertTrue(
        reported.isEmpty(),
        "a pipeline with nothing wrong with it must not be greeted with anything: " + reported);
  }

  /** Select Values as the issue's screenshot has it, before any of it reaches the linter. */
  private static SelectValuesMeta selectValuesFromTheIssue() {
    SelectValuesMeta meta = new SelectValuesMeta();

    SelectField copied = new SelectField();
    copied.setName("value");
    copied.setRename("valueToSqrt");
    SelectField kept = new SelectField();
    kept.setName("value");
    meta.getSelectOption().setSelectFields(new ArrayList<>(List.of(copied, kept)));

    // The Metadata tab names the field as it is after the rename, which is the only name it has
    // by then. SelectValuesMeta.check() looks for it in the incoming row, where it is not.
    SelectMetadataChange metadata = new SelectMetadataChange();
    metadata.setName("valueToSqrt");
    meta.getSelectOption().setMeta(new ArrayList<>(List.of(metadata)));

    return meta;
  }

  private static ICheckResult metadataFieldsNotFoundRemark() {
    // Built the way SelectValuesMeta.check() builds it: the heading, then the field names.
    return remark(
        ICheckResult.TYPE_RESULT_ERROR,
        BaseMessages.getString(
                SELECT_VALUES_PACKAGE, "SelectValuesMeta.CheckResult.MetadataFieldsNotFound")
            + Const.CR
            + Const.CR
            + "\t\tvalueToSqrt"
            + Const.CR,
        "SelectValues");
  }

  private static ICheckResult remark(int type, String text) {
    return remark(type, text, "SelectValues");
  }

  private static ICheckResult remark(int type, String text, String pluginId) {
    return new CheckResult(type, text, new TransformMeta(pluginId, "valueToSqrt", null));
  }

  private static CustomLintRule nativeRule(String id, String severity, boolean enabled) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId(id);
    rule.setType(CustomLintRule.TYPE_NATIVE);
    rule.setSeverity(severity);
    rule.setEnabled(enabled);
    return rule;
  }

  private static CustomLintRule selectValuesRule(String id, String key, boolean enabled) {
    CustomLintRule rule = nativeRule(id, "WARNING", enabled);
    rule.setAppliesTo(List.of("SelectValues"));
    rule.setMessageKey(SELECT_VALUES_PACKAGE + ":" + key);
    return rule;
  }
}
