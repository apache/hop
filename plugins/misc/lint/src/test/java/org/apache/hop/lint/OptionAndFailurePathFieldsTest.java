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
import java.util.Locale;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.selectvalues.SelectValuesMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The option conditions and the fields for TLS and failure-path rules.
 *
 * <p>"PostgreSQL without TLS" needed three allOf clauses with negative-lookahead regular
 * expressions over the text of a connection's attributes, which broke on a value holding a comma or
 * a brace. With {@code OPTION_MATCHES} it is one clause.
 *
 * @see <a href="https://github.com/apache/hop/issues/9500">#9500</a>
 */
class OptionAndFailurePathFieldsTest {

  private static final String TLS = "sslmode=(require|verify-ca|verify-full)";

  /** A type that extends another, as Redshift and Greenplum extend PostgreSQL. */
  @DatabaseMetaPlugin(type = "LINT_CHILD", typeDescription = "Lint test: extends NONE")
  public static class ChildOfNoneDatabaseMeta extends NoneDatabaseMeta {}

  /** A type that cannot build a URL for the connection. */
  @DatabaseMetaPlugin(type = "LINT_NO_URL", typeDescription = "Lint test: no URL")
  public static class NoUrlDatabaseMeta extends NoneDatabaseMeta {
    @Override
    public String getURL(String hostname, String port, String databaseName)
        throws HopDatabaseException {
      throw new HopDatabaseException("no host given");
    }
  }

  @BeforeAll
  static void initHop() throws Exception {
    // The connection types, to append extra options to a URL.
    HopEnvironment.init();
    for (Class<?> type : List.of(ChildOfNoneDatabaseMeta.class, NoUrlDatabaseMeta.class)) {
      PluginRegistry.getInstance()
          .registerPluginClass(type.getName(), DatabasePluginType.class, DatabaseMetaPlugin.class);
    }
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  @AfterEach
  void clearSubject() {
    CustomRuleExecutor.setSubject(null);
  }

  private static CustomLintRule rule(
      RuleTarget target, String field, RuleCondition condition, String value) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("TEST-001");
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(target);
    rule.setTargetField(field);
    rule.setCondition(condition);
    rule.setConditionValue(value);
    rule.setName("Test rule");
    return rule;
  }

  private static List<LintResult> lint(CustomLintRule rule, Object hopObject) {
    return CustomRuleExecutor.executeRule(rule, hopObject, "/tmp/test");
  }

  private static DatabaseMeta connection(String... options) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("CRM");
    databaseMeta.getAttributes().clear();
    for (int i = 0; i < options.length; i += 2) {
      databaseMeta.addExtraOption("NONE", options[i], options[i + 1]);
    }
    return databaseMeta;
  }

  // ------------------------------------------------------------------ option conditions

  @Test
  void hasOptionFindsAnExtraOptionByItsOwnName() {
    CustomLintRule rule =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.HAS_OPTION, "sslmode");

    assertTrue(lint(rule, connection("sslmode", "require")).isEmpty());
    assertEquals(1, lint(rule, connection("connectTimeout", "10")).size());
    assertEquals(1, lint(rule, connection()).size(), "no options at all");
  }

  @Test
  void hasOptionAlsoTakesTheStoredKey() {
    CustomLintRule rule =
        rule(
            RuleTarget.DATABASE_CONNECTION,
            "attributes",
            RuleCondition.HAS_OPTION,
            "EXTRA_OPTION_NONE.sslmode");

    assertTrue(lint(rule, connection("sslmode", "require")).isEmpty());
  }

  @Test
  void optionMatchesReadsTheOptionsValue() {
    CustomLintRule rule =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.OPTION_MATCHES, TLS);

    assertTrue(lint(rule, connection("sslmode", "verify-full")).isEmpty());
    assertEquals(1, lint(rule, connection("sslmode", "disable")).size());
    assertEquals(1, lint(rule, connection("sslmode", "requireX")).size(), "the whole value");
    assertEquals(1, lint(rule, connection()).size(), "not set");
  }

  /** A pattern over the map's text broke on another option's comma or brace. */
  @Test
  void optionMatchesIsNotConfusedByOtherOptions() {
    CustomLintRule rule =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.OPTION_MATCHES, TLS);

    assertTrue(
        lint(rule, connection("options", "-c search_path=a,b}", "sslmode", "require")).isEmpty());
    assertEquals(
        1, lint(rule, connection("options", "x, sslmode=require}", "sslmode", "allow")).size());
  }

  @Test
  void anOptionConditionOnTextIsAConfigurationError() {
    CustomLintRule rule =
        rule(RuleTarget.DATABASE_CONNECTION, "hostname", RuleCondition.HAS_OPTION, "sslmode");
    DatabaseMeta databaseMeta = connection();
    databaseMeta.setHostname("db.example.org");

    List<LintResult> results = lint(rule, databaseMeta);

    assertEquals(1, results.size());
    assertEquals("ERROR", results.get(0).getSeverity());
    assertTrue(results.get(0).getMessage().contains("holds no options"), results.toString());
  }

  @Test
  void anOptionMatchesValueWithoutANameIsAConfigurationError() {
    CustomLintRule rule =
        rule(
            RuleTarget.DATABASE_CONNECTION,
            "attributes",
            RuleCondition.OPTION_MATCHES,
            "(require|verify-full)");

    List<LintResult> results = lint(rule, connection("sslmode", "require"));

    assertEquals(1, results.size());
    assertTrue(results.get(0).getMessage().contains("name=regex"), results.toString());
  }

  /** The rule editor and the pack loader refuse what the executor cannot run. */
  @Test
  void theRuleEditorChecksTheOptionValues() {
    RuleTarget target = RuleTarget.DATABASE_CONNECTION;
    assertNull(
        new RuleClause("attributes", RuleCondition.OPTION_MATCHES, TLS)
            .valueProblem(target, false));
    assertNotNull(
        new RuleClause("attributes", RuleCondition.OPTION_MATCHES, "sslmode")
            .valueProblem(target, false));
    assertNotNull(
        new RuleClause("attributes", RuleCondition.OPTION_MATCHES, "sslmode=(require")
            .valueProblem(target, false));
    assertNotNull(
        new RuleClause("hostname", RuleCondition.HAS_OPTION, "sslmode").valueProblem(target, false),
        "hostname holds no options");
    assertTrue(
        RuleTargetFields.getCompatibleConditions("attributes")
            .containsAll(List.of(RuleCondition.HAS_OPTION, RuleCondition.OPTION_MATCHES)));
  }

  /** Hop appends neither an option without a value nor one stored for another database type. */
  @Test
  void optionsHopDoesNotUseDoNotCount() {
    CustomLintRule matches =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.OPTION_MATCHES, TLS);
    DatabaseMeta otherType = connection();
    otherType.addExtraOption("MYSQL", "sslmode", "require");

    assertEquals(1, lint(matches, otherType).size(), "stored for MySQL, not used for this type");
    CustomLintRule has =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.HAS_OPTION, "sslmode");
    assertEquals(1, lint(has, connection("sslmode", "")).size(), "no value");
  }

  /**
   * Hop also puts an option stored for the type a connection's type extends in its URL: a
   * PostgreSQL sslmode on a Redshift connection. The rule must see what Hop uses.
   */
  @Test
  void anOptionOfTheTypeAConnectionExtendsCounts() throws Exception {
    DatabaseMeta child = connection();
    child.setDatabaseType("LINT_CHILD");
    child.addExtraOption("NONE", "sslmode", "require");
    CustomLintRule has =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.HAS_OPTION, "sslmode");
    CustomLintRule matches =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.OPTION_MATCHES, TLS);

    assertTrue(child.getURL(new Variables()).contains("sslmode=require"), "Hop uses it");
    assertTrue(lint(has, child).isEmpty(), lint(has, child).toString());
    assertTrue(lint(matches, child).isEmpty(), lint(matches, child).toString());

    DatabaseMeta parent = connection();
    parent.addExtraOption("LINT_CHILD", "sslmode", "require");
    assertFalse(parent.getURL(new Variables()).contains("sslmode"), "Hop does not use it");
    assertEquals(1, lint(has, parent).size(), "the child's option on the parent type");
  }

  /** The executor reads the value as the rule editor checked it. */
  @Test
  void optionMatchesTrimsTheNameAndThePattern() {
    CustomLintRule rule =
        rule(
            RuleTarget.DATABASE_CONNECTION,
            "attributes",
            RuleCondition.OPTION_MATCHES,
            " sslmode = require ");

    assertTrue(lint(rule, connection("sslmode", "require")).isEmpty());
  }

  @Test
  void hasOptionWithoutANameOrWithAValueIsAConfigurationError() {
    for (String value : List.of("", "sslmode=require")) {
      CustomLintRule rule =
          rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.HAS_OPTION, value);

      List<LintResult> results = lint(rule, connection("sslmode", "require"));

      assertEquals(1, results.size(), "'" + value + "'");
      assertEquals("ERROR", results.get(0).getSeverity());
      assertTrue(
          results.get(0).getMessage().contains("name of the option only"), results.toString());
    }
    assertNotNull(
        new RuleClause("attributes", RuleCondition.HAS_OPTION, "sslmode=require")
            .valueProblem(RuleTarget.DATABASE_CONNECTION, false));
  }

  // ------------------------------------------------------------------ url

  /** The NONE type separates its options with ";", where PostgreSQL uses "?" and "&". */
  @Test
  void urlIsTheUrlHopBuildsWithTheExtraOptions() {
    CustomLintRule rule =
        rule(
            RuleTarget.DATABASE_CONNECTION,
            "url",
            RuleCondition.MATCHES_PATTERN,
            "jdbc://none;sslmode=require");

    assertTrue(lint(rule, connection("sslmode", "require")).isEmpty());
    assertEquals(1, lint(rule, connection()).size());
  }

  /** The URL carries the option values, so a finding never shows it, as for attributes. */
  @Test
  void aFindingDoesNotShowTheUrl() {
    CustomLintRule rule =
        rule(
            RuleTarget.DATABASE_CONNECTION,
            "url",
            RuleCondition.MATCHES_PATTERN,
            ".*sslmode=require.*");

    List<LintResult> results = lint(rule, connection("sslmode", "disable", "password", "hunter2"));

    assertEquals(1, results.size());
    assertFalse(results.get(0).getMessage().contains("hunter2"), results.get(0).getMessage());
    assertFalse(
        results.get(0).getMessage().contains("sslmode=disable"), results.get(0).getMessage());
  }

  /**
   * A URL that cannot be built read as no URL, and a pattern rule passes no value: the TLS rule
   * reported nothing for the connection. It is an error on the rule for that connection instead.
   */
  @Test
  void aUrlThatCannotBeBuiltIsAnErrorNotAPass() {
    DatabaseMeta databaseMeta = connection();
    databaseMeta.setDatabaseType("LINT_NO_URL");
    CustomLintRule url =
        rule(
            RuleTarget.DATABASE_CONNECTION,
            "url",
            RuleCondition.MATCHES_PATTERN,
            ".*sslmode=require.*");

    List<LintResult> results = lint(url, databaseMeta);

    assertEquals(1, results.size());
    assertEquals("ERROR", results.get(0).getSeverity());
    assertTrue(results.get(0).getMessage().contains("cannot be built"), results.toString());

    // The same as the second clause of an allOf rule, as OPT-002 in the integration tests.
    CustomLintRule composed =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.HAS_OPTION, "sslmode");
    composed.setAdditionalClauses(
        new ArrayList<>(
            List.of(new RuleClause("url", RuleCondition.MATCHES_PATTERN, ".*sslmode=require.*"))));
    results = lint(composed, databaseMeta);
    assertEquals(1, results.size());
    assertEquals("ERROR", results.get(0).getSeverity());
  }

  /** Variables and variable resolvers stay as written: linting must not reach a vault. */
  @Test
  void urlLeavesVariablesUnresolved() {
    DatabaseMeta databaseMeta = connection();
    databaseMeta.setManualUrl("jdbc:postgresql://${PG_HOST}:5432/#{vault:db:name}");
    CustomLintRule rule =
        rule(
            RuleTarget.DATABASE_CONNECTION,
            "url",
            RuleCondition.MATCHES_PATTERN,
            "^\\Qjdbc:postgresql://${PG_HOST}:5432/#{vault:db:name}\\E$");

    assertTrue(lint(rule, databaseMeta).isEmpty(), lint(rule, databaseMeta).toString());
  }

  // ------------------------------------------------------------------ appliesTo on connections

  /** Compared with the metadata key alone, appliesTo: [POSTGRESQL] matched no connection. */
  @Test
  void aConnectionRuleIsNarrowedByDatabaseType() {
    DatabaseMeta databaseMeta = connection();
    CustomLintRule rule =
        rule(RuleTarget.DATABASE_CONNECTION, "databaseType", RuleCondition.IS_EMPTY, null);

    rule.setAppliesTo(List.of("NONE"));
    assertEquals(1, lint(rule, databaseMeta).size(), "the connection's type");
    rule.setAppliesTo(List.of("POSTGRESQL"));
    assertTrue(lint(rule, databaseMeta).isEmpty(), "another type");
    rule.setAppliesTo(List.of("rdbms"));
    assertEquals(1, lint(rule, databaseMeta).size(), "the metadata key still covers them all");
  }

  // ------------------------------------------------------------------ category

  /** Hop Gui shows the category in its own language; a rule reads it in English. */
  @Test
  void categoryIsInEnglishWhateverTheLanguage() {
    LanguageChoice languageChoice = LanguageChoice.getInstance();
    Locale before = languageChoice.getDefaultLocale();
    try {
      languageChoice.setDefaultLocale(Locale.JAPAN);
      TransformMeta transformMeta = new TransformMeta("Select", "select", new SelectValuesMeta());
      CustomLintRule rule =
          rule(RuleTarget.TRANSFORM, "category", RuleCondition.NOT_MATCHES_PATTERN, "Transform");

      assertEquals("Transform", CustomRuleExecutor.transformCategory(new SelectValuesMeta()));
      assertEquals(1, lint(rule, transformMeta).size());
    } finally {
      languageChoice.setDefaultLocale(before);
    }
  }

  @Test
  void aTransformWithoutAPluginAnnotationHasNoCategory() {
    assertNull(CustomRuleExecutor.transformCategory(new BaseTransformMeta() {}));
    assertNull(CustomRuleExecutor.transformCategory(null));
  }

  // ------------------------------------------------------------------ actionsWithoutFailurePath

  /**
   * An action that evaluates its result, as most do: its hops are followed on success or failure.
   */
  private static ActionMeta evaluating(String name) {
    return new ActionMeta(
        new ActionDummy(name) {
          @Override
          public boolean isEvaluation() {
            return true;
          }
        });
  }

  private static WorkflowHopMeta hop(ActionMeta from, ActionMeta to, String kind) {
    WorkflowHopMeta hop = new WorkflowHopMeta(from, to);
    switch (kind) {
      case "unconditional" -> hop.setUnconditional(true);
      case "failure" -> hop.setEvaluation(false);
      case "disabled failure" -> {
        hop.setEvaluation(false);
        hop.setEnabled(false);
      }
      default -> hop.setEvaluation(true);
    }
    return hop;
  }

  /**
   * Check goes on to load on success only. Load goes on to notify on success, and to alert on
   * failure. Notify goes on to archive unconditionally, which the engine follows on failure too.
   * Archive goes on to cleanup on success and has only a disabled failure hop. Wait is a Dummy: it
   * does not evaluate its result, so it follows only unconditional hops and has no failure to
   * handle.
   */
  private static WorkflowMeta workflow() {
    WorkflowMeta workflow = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    ActionMeta check = evaluating("check");
    ActionMeta load = evaluating("load");
    ActionMeta notify = evaluating("notify");
    ActionMeta alert = evaluating("alert");
    ActionMeta archive = evaluating("archive");
    ActionMeta cleanup = evaluating("cleanup");
    ActionMeta wait = new ActionMeta(new ActionDummy("wait"));
    for (ActionMeta action : List.of(start, check, load, notify, alert, archive, cleanup, wait)) {
      workflow.addAction(action);
    }
    workflow.addWorkflowHop(hop(start, check, "unconditional"));
    workflow.addWorkflowHop(hop(check, load, "success"));
    workflow.addWorkflowHop(hop(load, notify, "success"));
    workflow.addWorkflowHop(hop(load, alert, "failure"));
    workflow.addWorkflowHop(hop(notify, archive, "unconditional"));
    workflow.addWorkflowHop(hop(archive, cleanup, "success"));
    workflow.addWorkflowHop(hop(archive, alert, "disabled failure"));
    workflow.addWorkflowHop(hop(cleanup, wait, "unconditional"));
    workflow.addWorkflowHop(hop(wait, alert, "success"));
    return workflow;
  }

  @Test
  void actionsWithoutFailurePathListsTheActionsThatStopUnhandled() {
    CustomLintRule rule =
        rule(
            RuleTarget.WORKFLOW,
            "actionsWithoutFailurePath",
            RuleCondition.MAX_COLLECTION_SIZE,
            "0");

    List<LintResult> results = lint(rule, workflow());

    assertEquals(1, results.size());
    String message = results.get(0).getMessage();
    assertTrue(message.contains("[check, archive]"), message);
  }

  @Test
  void aWorkflowThatHandlesEveryFailureHasNone() {
    WorkflowMeta workflow = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    ActionMeta load = evaluating("load");
    ActionMeta alert = evaluating("alert");
    workflow.addAction(start);
    workflow.addAction(load);
    workflow.addAction(alert);
    workflow.addWorkflowHop(hop(start, load, "unconditional"));
    workflow.addWorkflowHop(hop(load, alert, "failure"));
    CustomLintRule rule =
        rule(
            RuleTarget.WORKFLOW,
            "actionsWithoutFailurePath",
            RuleCondition.MAX_COLLECTION_SIZE,
            "0");

    assertTrue(lint(rule, workflow).isEmpty(), "Start is not counted, and alert leads nowhere");
  }
}
