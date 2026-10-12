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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The field, condition and value widgets of the rule editor, and the clause table above them. */
@Tag("uitest")
class RuleBuilderDialogClauseTest extends SwtBotTestBase {

  private static CustomLintRule rule(
      RuleTarget target, String field, RuleCondition condition, String value) {
    CustomLintRule rule = new CustomLintRule("Naming", target, field, condition);
    rule.setId("PROJECT-001");
    rule.setConditionValue(value);
    return rule;
  }

  /** Hidden but kept, the old pattern was saved with NOT_NULL and came back on the next switch. */
  @Test
  void aConditionWithoutValueDropsTheOldValue() {
    CustomLintRule rule =
        rule(RuleTarget.PIPELINE, "name", RuleCondition.MATCHES_PATTERN, "^[A-Z][A-Z0-9_]*$");
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, rule).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog.comboBoxWithLabel("Condition:").setSelection("Not Null");
          dialog.button("OK").click();
        });

    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals(RuleCondition.NOT_NULL, saved.get().getCondition());
    assertNull(saved.get().getConditionValue());
  }

  /** Choosing the field again emptied the condition, and OK then refused to save. */
  @Test
  void choosingTheSameFieldKeepsTheCondition() {
    CustomLintRule rule =
        rule(RuleTarget.TRANSFORM, "isOrphaned", RuleCondition.MUST_BE_FALSE, null);
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();
    AtomicReference<String> tableCondition = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, rule).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog.comboBoxWithLabel("Field:").setSelection("isOrphaned");
          tableCondition.set(dialog.table().cell(0, 1));
          dialog.button("OK").click();
        });

    assertEquals("Must Be False", tableCondition.get());
    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals(RuleCondition.MUST_BE_FALSE, saved.get().getCondition());
  }

  /** A field with a condition both offer keeps the condition and its value. */
  @Test
  void anotherFieldKeepsAConditionItAlsoOffers() {
    CustomLintRule rule = rule(RuleTarget.PIPELINE, "name", RuleCondition.STARTS_WITH, "etl_");
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, rule).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog.comboBoxWithLabel("Field:").setSelection("description");
          dialog.button("OK").click();
        });

    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals("description", saved.get().getTargetField());
    assertEquals(RuleCondition.STARTS_WITH, saved.get().getCondition());
    assertEquals("etl_", saved.get().getConditionValue());
  }

  /** Pipeline to Workflow cleared the field and condition, though workflows have a name too. */
  @Test
  void anotherTargetKeepsAFieldItAlsoHas() {
    CustomLintRule rule = rule(RuleTarget.PIPELINE, "name", RuleCondition.NOT_NULL, null);
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();
    AtomicReference<String> tableField = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, rule).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog
              .comboBoxWithLabel("Target Type:")
              .setSelection(RuleTarget.WORKFLOW.getDisplayName());
          tableField.set(dialog.table().cell(0, 0));
          dialog.button("OK").click();
        });

    assertEquals("name", tableField.get());
    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals(RuleTarget.WORKFLOW, saved.get().getTarget());
    assertEquals("name", saved.get().getTargetField());
    assertEquals(RuleCondition.NOT_NULL, saved.get().getCondition());
  }

  /** A field the new target does not have goes, in the table as well as the dropdowns. */
  @Test
  void anotherTargetClearsAFieldItDoesNotHave() {
    CustomLintRule rule =
        rule(RuleTarget.PIPELINE, "transformCount", RuleCondition.MAX_VALUE, "30");
    AtomicReference<String> tableField = new AtomicReference<>();
    AtomicReference<String> tableCondition = new AtomicReference<>();

    withDialog(
        parent -> new RuleBuilderDialog(parent, rule).open(),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog
              .comboBoxWithLabel("Target Type:")
              .setSelection(RuleTarget.WORKFLOW.getDisplayName());
          tableField.set(dialog.table().cell(0, 0));
          tableCondition.set(dialog.table().cell(0, 1));
          dialog.button("Cancel").click();
        });

    assertEquals("", tableField.get());
    assertEquals("", tableCondition.get());
  }

  /**
   * A trip to another target and back gives a clause its field again, even one the editor does not
   * list, such as a Table Input's sql.
   */
  @Test
  void backOnItsOwnTargetARuleGetsItsFieldsBack() {
    CustomLintRule rule =
        rule(RuleTarget.TRANSFORM, "sql", RuleCondition.NOT_MATCHES_PATTERN, "(?i)select \\*");
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();
    AtomicReference<String> emptiedField = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, rule).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog
              .comboBoxWithLabel("Target Type:")
              .setSelection(RuleTarget.TRANSFORM.getDisplayName());
          dialog
              .comboBoxWithLabel("Target Type:")
              .setSelection(RuleTarget.PIPELINE.getDisplayName());
          emptiedField.set(dialog.table().cell(0, 0));
          dialog
              .comboBoxWithLabel("Target Type:")
              .setSelection(RuleTarget.TRANSFORM.getDisplayName());
          dialog.button("OK").click();
        });

    assertEquals("", emptiedField.get(), "pipelines have no sql");
    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals("sql", saved.get().getTargetField());
    assertEquals(RuleCondition.NOT_MATCHES_PATTERN, saved.get().getCondition());
    assertEquals("(?i)select \\*", saved.get().getConditionValue());
  }

  /** An edit made before the trip to another target is what comes back, not the rule as opened. */
  @Test
  void backOnItsOwnTargetAClauseKeepsItsEdits() {
    CustomLintRule rule = rule(RuleTarget.TRANSFORM, "sql", RuleCondition.NOT_MATCHES_PATTERN, "a");
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, rule).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog.textWithLabel("Value:").setText("b");
          dialog
              .comboBoxWithLabel("Target Type:")
              .setSelection(RuleTarget.PIPELINE.getDisplayName());
          dialog
              .comboBoxWithLabel("Target Type:")
              .setSelection(RuleTarget.TRANSFORM.getDisplayName());
          dialog.button("OK").click();
        });

    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals("sql", saved.get().getTargetField());
    assertEquals("b", saved.get().getConditionValue());
  }

  /** OK checks every clause, and shows the first incomplete one. */
  @Test
  void anEmptyClauseThatIsNotOnShowIsRefused() {
    CustomLintRule rule = rule(RuleTarget.PIPELINE, "name", RuleCondition.NOT_NULL, null);
    rule.setAdditionalClauses(new ArrayList<>(List.of(new RuleClause("", null, ""))));

    Refusal refusal = pressOkExpectingRefusal(rule);

    assertEquals(List.of("Clause 2: Field must be selected"), refusal.errors());
    assertEquals("", refusal.selectedField(), "the empty clause is selected");
  }

  @Test
  void aMissingValueInAClauseThatIsNotOnShowIsRefused() {
    CustomLintRule rule = rule(RuleTarget.PIPELINE, "name", RuleCondition.NOT_NULL, null);
    rule.setAdditionalClauses(
        new ArrayList<>(List.of(new RuleClause("description", RuleCondition.MATCHES_PATTERN, ""))));

    Refusal refusal = pressOkExpectingRefusal(rule);

    assertEquals(List.of("Clause 2: Value is required for this condition"), refusal.errors());
    assertEquals("description", refusal.selectedField());
  }

  /** A value left on a condition that takes none is no reason to refuse; it is just not saved. */
  @Test
  void aLeftoverValueOnAConditionWithoutValueIsDropped() {
    CustomLintRule rule = rule(RuleTarget.PIPELINE, "name", RuleCondition.NOT_NULL, "^X$");
    rule.setAdditionalClauses(
        new ArrayList<>(List.of(new RuleClause("description", RuleCondition.NOT_EMPTY, "left"))));
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();
    List<String> errors = new CopyOnWriteArrayList<>();

    withDialog(
        parent -> {
          RuleBuilderDialog dialog = new RuleBuilderDialog(parent, rule);
          dialog.errorReporter = errors::add;
          saved.set(dialog.open());
        },
        bot -> bot.shell("Edit Lint Rule").bot().button("OK").click());

    assertEquals(List.of(), errors);
    assertNotNull(saved.get(), "the dialog refused to save");
    assertNull(saved.get().getConditionValue());
    assertNull(saved.get().getAdditionalClauses().get(0).getConditionValue());
  }

  private record Refusal(List<String> errors, String selectedField) {}

  /** Press OK, wait for the refusal, note the selected clause's field and cancel. */
  private Refusal pressOkExpectingRefusal(CustomLintRule rule) {
    List<String> errors = new CopyOnWriteArrayList<>();
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();
    AtomicReference<String> selectedField = new AtomicReference<>();

    withDialog(
        parent -> {
          RuleBuilderDialog dialog = new RuleBuilderDialog(parent, rule);
          dialog.errorReporter = errors::add;
          saved.set(dialog.open());
        },
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog.button("OK").click();
          bot.waitUntil(
              new DefaultCondition() {
                @Override
                public boolean test() {
                  return !errors.isEmpty();
                }

                @Override
                public String getFailureMessage() {
                  return "OK was not refused";
                }
              });
          selectedField.set(dialog.table().selection().get(0, 0));
          dialog.button("Cancel").click();
        });

    assertNull(saved.get(), "a refused rule is not saved");
    return new Refusal(errors, selectedField.get());
  }

  @Test
  void aProjectRuleIdCanBeChanged() {
    CustomLintRule rule = rule(RuleTarget.PIPELINE, "name", RuleCondition.NOT_NULL, null);
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, rule).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          dialog.textWithLabel("Rule ID:").setText("NAMING-PIPE");
          dialog.button("OK").click();
        });

    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals("NAMING-PIPE", saved.get().getId());
  }
}
