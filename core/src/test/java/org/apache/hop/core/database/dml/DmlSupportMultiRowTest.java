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

package org.apache.hop.core.database.dml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.ConcreteBaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The rows-per-INSERT rules of {@link DmlSupport}, and the switches that turn them off. */
class DmlSupportMultiRowTest {

  private static final DmlSyntax VALUES_LIST =
      DmlSyntax.builder().insertForm(DmlSyntax.InsertForm.VALUES_LIST).build();

  @BeforeAll
  static void setUpOnce() throws HopException {
    DatabasePluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  /** A connection whose dialect declares the syntax, and refuses as given on an open session. */
  private static DatabaseMeta connection(DmlSyntax syntax, String sessionRefusal) {
    return connection(syntax, sessionRefusal, 0);
  }

  /**
   * A connection whose dialect declares the syntax, refuses as given and has a statement size limit
   * on an open session; the refusal names the table it was asked about.
   */
  private static DatabaseMeta connection(
      DmlSyntax syntax, String sessionRefusal, long maxStatementBytes) {
    ConcreteBaseDatabaseMeta dialect =
        new ConcreteBaseDatabaseMeta() {
          @Override
          public DmlSyntax getDmlSyntax() {
            return syntax;
          }

          @Override
          public String getMultiRowDmlRefusal(Connection connection, DmlSpec spec) {
            return sessionRefusal == null ? null : sessionRefusal + " on " + spec.getQuotedTable();
          }

          @Override
          public long getMaxDmlStatementBytes(Connection connection) {
            return maxStatementBytes;
          }
        };
    dialect.setPluginName("TestDB");
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(dialect);
    return databaseMeta;
  }

  private static DatabaseMeta connection(DmlSyntax syntax) {
    return connection(syntax, null);
  }

  private static DmlSpec insert() throws HopDatabaseException {
    return DmlSpec.insert("T", List.of(DmlColumn.of("ID"), DmlColumn.of("NAME")));
  }

  private static DmlPlan plan(DmlSyntax syntax, DmlSpec spec, int rows) throws Exception {
    return DmlSupport.planStatements(connection(syntax), null, spec, rows);
  }

  private static Variables variable(String value) {
    Variables variables = new Variables();
    variables.setVariable(Const.HOP_DATABASE_MULTI_ROW_DML, value);
    return variables;
  }

  @Test
  void noConnection() throws Exception {
    DmlPlan plan = DmlSupport.planStatements(null, null, insert(), 10);
    assertFalse(plan.isSupported());
    assertNotNull(plan.getUnsupportedReason());
  }

  @Test
  void onlyAnInsertIsPlanned() throws Exception {
    DmlSpec upsert =
        DmlSpec.upsert(
            "T", List.of(DmlColumn.of("ID")), List.of(DmlColumn.of("NAME")), List.of(), false);
    assertThrows(
        HopDatabaseException.class,
        () -> DmlSupport.planStatements(connection(VALUES_LIST), null, upsert, 10));
  }

  @Test
  void oneRowAskedForIsOneRowWithoutANote() throws Exception {
    for (int rows : new int[] {1, 0, -5}) {
      DmlPlan plan = plan(VALUES_LIST, insert(), rows);
      assertTrue(plan.isSupported());
      assertEquals(DmlOperation.INSERT, plan.getOperation());
      assertEquals(1, plan.getRowsPerStatement());
      assertNull(plan.getNote());
    }
    // Without a declaration too: a plain one-row INSERT works everywhere.
    assertEquals(1, plan(null, insert(), 1).getRowsPerStatement());
  }

  @Test
  void severalRowsNeedADeclaredInsertForm() throws Exception {
    for (DmlSyntax syntax : new DmlSyntax[] {null, DmlSyntax.STANDARD_MERGE}) {
      DmlPlan plan = plan(syntax, insert(), 100);
      assertTrue(plan.isSupported());
      assertEquals(1, plan.getRowsPerStatement());
      assertTrue(plan.getNote().contains("TestDB"), plan.getNote());
    }
    DmlPlan plan = plan(VALUES_LIST, insert(), 100);
    assertEquals(100, plan.getRowsPerStatement());
    assertNull(plan.getNote());
  }

  @Test
  void limitsCapTheRows() throws Exception {
    DmlSpec insert =
        DmlSpec.insert("T", List.of(DmlColumn.of("A"), DmlColumn.of("B"), DmlColumn.of("C")));
    DmlSyntax syntax = VALUES_LIST.withMaxRowsPerStatement(40).withMaxParameters(100);
    DmlPlan plan = plan(syntax, insert, 1000);
    // 40 rows of 3 parameters is 120: 33 rows fit in 100 parameters.
    assertEquals(33, plan.getRowsPerStatement());
    assertTrue(plan.getNote().contains("100"), plan.getNote());
    assertEquals(20, plan(syntax, insert, 20).getRowsPerStatement());
    assertNull(plan(syntax, insert, 20).getNote());
    plan = plan(syntax.withMaxParameters(0), insert, 1000);
    assertEquals(40, plan.getRowsPerStatement());
    assertTrue(plan.getNote().contains("40"), plan.getNote());
    // A row wider than the limit still goes, one per statement.
    plan = plan(syntax.withMaxParameters(2), insert, 10);
    assertEquals(1, plan.getRowsPerStatement());
    assertNotNull(plan.getNote());
  }

  @Test
  void theSessionCanRuleOutSeveralRows() throws Exception {
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(metaData.getConnection()).thenReturn(mock(Connection.class));
    DmlPlan plan =
        DmlSupport.planStatements(
            connection(VALUES_LIST, "strict mode is off"), metaData, insert(), 100);
    assertEquals(1, plan.getRowsPerStatement());
    assertTrue(plan.getNote().contains("strict mode is off on T"), plan.getNote());

    // Without an open connection only the dialect is checked.
    assertEquals(
        100,
        DmlSupport.planStatements(
                connection(VALUES_LIST, "strict mode is off"), null, insert(), 100)
            .getRowsPerStatement());
    // A session that doesn't refuse.
    assertEquals(
        100,
        DmlSupport.planStatements(connection(VALUES_LIST), metaData, insert(), 100)
            .getRowsPerStatement());
  }

  @Test
  void theConnectionLimitsTheBytesOfAStatement() throws Exception {
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(metaData.getConnection()).thenReturn(mock(Connection.class));

    DmlPlan plan =
        DmlSupport.planStatements(
            connection(VALUES_LIST, null, 4_194_304), metaData, insert(), 100);
    assertEquals(100, plan.getRowsPerStatement());
    assertEquals(4_194_304, plan.getMaxStatementBytes());
    assertNull(plan.getNote());

    // Without an open connection there is no limit to read, and without one the plan has none.
    assertEquals(
        0,
        DmlSupport.planStatements(connection(VALUES_LIST, null, 4_194_304), null, insert(), 100)
            .getMaxStatementBytes());
    assertEquals(
        0,
        DmlSupport.planStatements(connection(VALUES_LIST), metaData, insert(), 100)
            .getMaxStatementBytes());
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"DISABLED", "disabled", " Disabled "})
  void theVariableWritesOneRowPerStatement(String value) throws Exception {
    assertEquals(
        100,
        DmlSupport.planStatements(connection(VALUES_LIST), null, insert(), 100, new Variables())
            .getRowsPerStatement());

    DmlPlan plan =
        DmlSupport.planStatements(connection(VALUES_LIST), null, insert(), 100, variable(value));
    assertTrue(plan.isSupported());
    assertEquals(1, plan.getRowsPerStatement());
    assertTrue(plan.getNote().contains(Const.HOP_DATABASE_MULTI_ROW_DML), plan.getNote());
  }

  @ParameterizedTest(name = "[{0}]")
  @ValueSource(strings = {"", "ENABLED", "N", "LEGACY"})
  void anyOtherValueOfTheVariableChangesNothing(String value) throws Exception {
    DmlPlan plan =
        DmlSupport.planStatements(connection(VALUES_LIST), null, insert(), 100, variable(value));
    assertEquals(100, plan.getRowsPerStatement());
    assertNull(plan.getNote());
  }

  @Test
  void theConnectionOptionWritesOneRowPerStatement() throws Exception {
    DatabaseMeta databaseMeta = connection(VALUES_LIST);
    databaseMeta.setName("warehouse");
    assertFalse(databaseMeta.isMultiRowDmlDisabled());
    databaseMeta.setMultiRowDmlDisabled(true);
    assertTrue(databaseMeta.isMultiRowDmlDisabled());
    assertEquals(
        "Y", databaseMeta.getAttributes().get(DmlSupport.ATTRIBUTE_MULTI_ROW_DML_DISABLED));

    // Without variables too: the option is the connection's own.
    DmlPlan plan = DmlSupport.planStatements(databaseMeta, null, insert(), 100);
    assertEquals(1, plan.getRowsPerStatement());
    assertTrue(plan.getNote().contains("warehouse"), plan.getNote());

    databaseMeta.setMultiRowDmlDisabled(false);
    assertFalse(
        databaseMeta.getAttributes().containsKey(DmlSupport.ATTRIBUTE_MULTI_ROW_DML_DISABLED));
    assertEquals(
        100, DmlSupport.planStatements(databaseMeta, null, insert(), 100).getRowsPerStatement());
  }

  @Test
  void oneRowAskedForHasNoNoteWhateverTheSwitch() throws Exception {
    DatabaseMeta databaseMeta = connection(VALUES_LIST);
    databaseMeta.setMultiRowDmlDisabled(true);
    DmlPlan plan = DmlSupport.planStatements(databaseMeta, null, insert(), 1, variable("DISABLED"));
    assertEquals(1, plan.getRowsPerStatement());
    assertNull(plan.getNote());
  }

  @Test
  void theConnectionOptionSurvivesSerialization() throws Exception {
    DatabaseMeta databaseMeta =
        new DatabaseMeta("warehouse", "NONE", "Native", "", "x", "", "", "");
    databaseMeta.setMultiRowDmlDisabled(true);
    JsonMetadataParser<DatabaseMeta> parser =
        new JsonMetadataParser<>(DatabaseMeta.class, new MemoryMetadataProvider());
    String json = parser.getJsonObject(databaseMeta).toJSONString();
    assertTrue(json.contains(DmlSupport.ATTRIBUTE_MULTI_ROW_DML_DISABLED), json);

    try (JsonParser jsonParser = new JsonFactory().createParser(json)) {
      jsonParser.nextToken();
      DatabaseMeta loaded = parser.loadJsonObject(DatabaseMeta.class, jsonParser);
      assertTrue(loaded.isMultiRowDmlDisabled(), json);
    }
  }
}
