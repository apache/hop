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
package org.apache.hop.core.database.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.IPluginMock;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Covers the type rule SPI: contribution, dialect matching, precedence and fallback. */
class DatabaseTypeRulesTest {

  private IVariables variables;
  private final List<IPluginMock> registeredPlugins = new ArrayList<>();

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @AfterEach
  void tearDown() {
    // The plugin registry is process-global, so a plugin registered here would otherwise be
    // visible to every later test in this JVM.
    registeredPlugins.forEach(
        plugin ->
            PluginRegistry.getInstance().removePlugin(DatabaseTypeRulesPluginType.class, plugin));
    registeredPlugins.clear();
    DatabaseTypeRuleRegistry.clearCache();
  }

  @BeforeEach
  void setUp() {
    variables = new Variables();
    // Rules are cached per dialect class, and these tests change what is registered.
    DatabaseTypeRuleRegistry.clearCache();
  }

  // ------------------------------------------------------------------ dialect identity

  @DatabaseMetaPlugin(type = "TEST_BASE", typeDescription = "Test base dialect")
  static class BaseTestDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules()
          .read(Types.OTHER)
          .as(IValueMeta.TYPE_STRING, 42, 0)
          .write(IValueMeta.TYPE_JSON)
          .as("BASE_JSON")
          .build();
    }
  }

  @DatabaseMetaPlugin(type = "TEST_DERIVED", typeDescription = "Test derived dialect")
  static class DerivedTestDialect extends BaseTestDialect {}

  @Test
  void dialectTypeChainFollowsTheClassHierarchy() {
    // This is what replaces isPostgresVariant(): a derived dialect answers to its parent's name
    // without anyone maintaining a list of Postgres-like databases.
    assertEquals(
        List.of("TEST_BASE", "NONE"),
        DatabaseTypeRuleRegistry.getDialectTypes(new BaseTestDialect()));
    assertEquals(
        List.of("TEST_DERIVED", "TEST_BASE", "NONE"),
        DatabaseTypeRuleRegistry.getDialectTypes(new DerivedTestDialect()));
  }

  @Test
  void derivedDialectInheritsItsParentsRules() {
    assertEquals(
        "BASE_JSON",
        DatabaseTypeMapper.getColumnType(
            variables, metaFor(new DerivedTestDialect()), json(), createContext()));
  }

  // ------------------------------------------------------------------ read path

  @Test
  void dialectRuleClaimsTheColumnBeforeTheStandardMapping() throws Exception {
    // Types.OTHER would fall through the standard mapping and return null.
    DatabaseMeta meta = metaFor(new BaseTestDialect());
    IValueMeta claimed =
        DatabaseTypeMapper.getValueMeta(
            variables, meta, column(Types.OTHER, "SOMETHING"), false, false);
    assertNotNull(claimed);
    assertTrue(claimed.isString());
    assertEquals(42, claimed.getLength());
  }

  @Test
  void standardMappingAppliesWhenNoRuleClaimsTheColumn() throws Exception {
    DatabaseMeta meta = metaFor(new BaseTestDialect());
    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(
            variables, meta, column(Types.INTEGER, "INT4"), false, false);
    assertNotNull(valueMeta);
    assertTrue(valueMeta.isInteger());
    assertEquals(9, valueMeta.getLength());
  }

  @Test
  void unclaimedNonStandardTypeStillReturnsNullForTheValueMetaPlugins() throws Exception {
    DatabaseMeta meta = metaFor(new PlainDialect());
    assertNull(
        DatabaseTypeMapper.getValueMeta(
            variables, meta, column(Types.STRUCT, "SDO_GEOMETRY"), false, false));
  }

  @DatabaseMetaPlugin(type = "TEST_PLAIN", typeDescription = "Dialect with no rules")
  static class PlainDialect extends NoneDatabaseMeta {}

  // ------------------------------------------------------------------ unsized integer, #4174

  /**
   * Two dialects that spell a Long wide integer differently, to show the rule asks rather than
   * decides.
   */
  @DatabaseMetaPlugin(type = "TEST_BIGINT", typeDescription = "Spells a wide integer BIGINT")
  static class BigintDialect extends NoneDatabaseMeta {
    @Override
    public String getFieldDefinition(
        IValueMeta v,
        String tk,
        String pk,
        boolean useAutoIncrement,
        boolean addFieldName,
        boolean addCr) {
      return (addFieldName ? v.getName() + " " : "") + "BIGINT(" + v.getLength() + ")";
    }
  }

  @DatabaseMetaPlugin(type = "TEST_INT64", typeDescription = "Spells a wide integer INT64")
  static class Int64Dialect extends NoneDatabaseMeta {
    @Override
    public String getFieldDefinition(
        IValueMeta v,
        String tk,
        String pk,
        boolean useAutoIncrement,
        boolean addFieldName,
        boolean addCr) {
      return (addFieldName ? v.getName() + " " : "") + "INT64";
    }
  }

  private String unsizedInteger(DatabaseMeta meta, IValueMeta valueMeta, ColumnContext context) {
    return ColumnTypeRules.UNSIZED_INTEGER_AS_LONG.getColumnType(
        variables, meta.getIDatabase(), valueMeta, context);
  }

  @Test
  void anUnsizedIntegerIsWidenedToTheLongBoundaryAndSpelledByTheDialect() {
    ValueMetaInteger unsized = new ValueMetaInteger("id");
    unsized.setLength(-1);

    // The width handed to the dialect is 18, the most a Long is guaranteed to hold.
    assertEquals(
        "BIGINT(18)", unsizedInteger(metaFor(new BigintDialect()), unsized, createContext()));
    // Same rule, different database, no type name of its own.
    assertEquals("INT64", unsizedInteger(metaFor(new Int64Dialect()), unsized, createContext()));
  }

  @Test
  void aZeroLengthIntegerCountsAsUnsizedToo() {
    ValueMetaInteger zeroLength = new ValueMetaInteger("id");
    zeroLength.setLength(0);
    assertEquals(
        "BIGINT(18)", unsizedInteger(metaFor(new BigintDialect()), zeroLength, createContext()));
  }

  @Test
  void anIntegerThatStatedItsLengthIsLeftToTheDialect() {
    ValueMetaInteger sized = new ValueMetaInteger("id");
    sized.setLength(4);
    assertNull(unsizedInteger(metaFor(new BigintDialect()), sized, createContext()));
  }

  @Test
  void anUnsizedNonIntegerIsLeftAlone() {
    assertNull(
        unsizedInteger(metaFor(new BigintDialect()), new ValueMetaString("s"), createContext()));
  }

  /** A key column already has its own spelling in every dialect that has one. */
  @Test
  void aKeyColumnIsLeftToTheDialect() {
    ValueMetaInteger unsized = new ValueMetaInteger("id");
    unsized.setLength(-1);
    DatabaseMeta meta = metaFor(new BigintDialect());

    assertNull(
        unsizedInteger(
            meta,
            unsized,
            new ColumnContext(ColumnContext.Purpose.CREATE, "id", null, false, false, false)));
    assertNull(
        unsizedInteger(
            meta,
            unsized,
            new ColumnContext(ColumnContext.Purpose.CREATE, null, "ID", false, false, false)));
  }

  /** The dialect is handed a copy: several of them modify the value they are given. */
  @Test
  void theValueTheCallerOwnsIsNotModified() {
    ValueMetaInteger unsized = new ValueMetaInteger("id");
    unsized.setLength(-1);
    unsizedInteger(metaFor(new BigintDialect()), unsized, createContext());
    assertEquals(-1, unsized.getLength());
  }

  // ------------------------------------------------------------------ builder semantics

  @Test
  void rulesMatchInDeclarationOrder() {
    List<IDatabaseTypeRule> rules =
        DatabaseTypes.rules()
            .write(IValueMeta.TYPE_STRING)
            .where(v -> v.getLength() <= 0)
            .as("TEXT")
            .write(IValueMeta.TYPE_STRING)
            .as("VARCHAR")
            .build();
    DatabaseMeta meta = metaFor(new PlainDialect());

    assertEquals("TEXT", first(rules, meta, new ValueMetaString("c")));
    ValueMetaString sized = new ValueMetaString("c");
    sized.setLength(10);
    assertEquals("VARCHAR", first(rules, meta, sized));
  }

  @Test
  void nativeTypeNameMatchingIsCaseInsensitiveAndSupportsPatterns() throws Exception {
    List<IDatabaseTypeRule> byName =
        DatabaseTypes.rules().readNative("jsonb").as(IValueMeta.TYPE_STRING, 1, 0).build();
    List<IDatabaseTypeRule> byPattern =
        DatabaseTypes.rules().readNativeMatching(".*INT.*").as(IValueMeta.TYPE_INTEGER).build();
    DatabaseMeta meta = metaFor(new PlainDialect());

    assertNotNull(byName.get(0).getValueMeta(variables, meta, column(Types.OTHER, "JSONB")));
    assertNull(byName.get(0).getValueMeta(variables, meta, column(Types.OTHER, "JSON")));
    assertNotNull(byPattern.get(0).getValueMeta(variables, meta, column(Types.INTEGER, "BIGINT")));
  }

  @Test
  void aRuleCanSupplyABindingForTheColumnsItClaims() throws Exception {
    IValueBinding binding = mock(IValueBinding.class);
    IDatabase dialect =
        new PlainDialect() {
          @Override
          public List<IDatabaseTypeRule> getTypeRules() {
            return DatabaseTypes.rules()
                .readNative("SDO_GEOMETRY")
                .bind(binding)
                .as(IValueMeta.TYPE_BINARY)
                .build();
          }
        };
    DatabaseTypeRuleRegistry.clearCache();
    DatabaseMeta meta = metaFor(dialect);

    // A binding is chosen from the dialect and the value, because by the time rows are moving
    // there is no column metadata left.
    assertEquals(
        binding, DatabaseTypeMapper.getBinding(meta.getIDatabase(), new ValueMetaBinary("geom")));
    assertNull(DatabaseTypeMapper.getBinding(meta.getIDatabase(), new ValueMetaString("name")));
  }

  // ------------------------------------------------------------------ external contribution

  @DatabaseTypeRulesPlugin(
      id = "test-external-rules",
      dialects = {"TEST_BASE"})
  public static class ExternalRules implements IDatabaseTypeRuleProvider {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().write(IValueMeta.TYPE_JSON).as("EXTERNAL_JSON").build();
    }
  }

  @DatabaseTypeRulesPlugin(
      id = "test-other-dialect-rules",
      dialects = {"SOMETHING_ELSE"})
  public static class RulesForAnotherDialect implements IDatabaseTypeRuleProvider {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().write(IValueMeta.TYPE_JSON).as("WRONG_DIALECT").build();
    }
  }

  @DatabaseTypeRulesPlugin(
      id = "test-missing-value-type-rules",
      dialects = {"TEST_BASE"},
      valueTypes = {"NoSuchValueType"})
  public static class RulesNeedingAMissingValueType implements IDatabaseTypeRuleProvider {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().write(IValueMeta.TYPE_JSON).as("SHOULD_BE_SKIPPED").build();
    }
  }

  @Test
  void anExternalPluginCanAddRulesToADialectItDoesNotOwn() throws Exception {
    register(new ExternalRules());
    DatabaseTypeRuleRegistry.clearCache();

    // The dialect's own rule says BASE_JSON; the contributed one is consulted first.
    assertEquals(
        "EXTERNAL_JSON",
        DatabaseTypeMapper.getColumnType(
            variables, metaFor(new BaseTestDialect()), json(), createContext()));
  }

  @Test
  void contributedRulesOnlyApplyToTheDialectsTheyName() throws Exception {
    register(new RulesForAnotherDialect());
    DatabaseTypeRuleRegistry.clearCache();

    assertEquals(
        "BASE_JSON",
        DatabaseTypeMapper.getColumnType(
            variables, metaFor(new BaseTestDialect()), json(), createContext()));
  }

  @Test
  void contributedRulesAreSkippedWhenTheirValueTypeIsNotInstalled() throws Exception {
    register(new RulesNeedingAMissingValueType());
    DatabaseTypeRuleRegistry.clearCache();

    assertEquals(
        "BASE_JSON",
        DatabaseTypeMapper.getColumnType(
            variables, metaFor(new BaseTestDialect()), json(), createContext()));
  }

  // ------------------------------------------------------------------ helpers

  private void register(IDatabaseTypeRuleProvider provider) throws Exception {
    IPluginMock plugin = mock(IPluginMock.class);
    when(plugin.getIds())
        .thenReturn(
            new String[] {provider.getClass().getAnnotation(DatabaseTypeRulesPlugin.class).id()});
    when(plugin.getName()).thenReturn("test rules");
    when(plugin.getMainType()).thenReturn((Class) IDatabaseTypeRuleProvider.class);
    when(plugin.loadClass(IDatabaseTypeRuleProvider.class)).thenReturn(provider);
    PluginRegistry.getInstance().registerPlugin(DatabaseTypeRulesPluginType.class, plugin);
    registeredPlugins.add(plugin);
  }

  private String first(List<IDatabaseTypeRule> rules, DatabaseMeta meta, IValueMeta valueMeta) {
    for (IDatabaseTypeRule rule : rules) {
      String type = rule.getColumnType(variables, meta.getIDatabase(), valueMeta, createContext());
      if (type != null) {
        return type;
      }
    }
    return null;
  }

  private static ColumnContext createContext() {
    return new ColumnContext(ColumnContext.Purpose.CREATE, null, null, false, false, false);
  }

  private static IValueMeta json() {
    IValueMeta valueMeta = mock(IValueMeta.class);
    when(valueMeta.getType()).thenReturn(IValueMeta.TYPE_JSON);
    return valueMeta;
  }

  private static DatabaseMeta metaFor(IDatabase dialect) {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    when(meta.getIDatabase()).thenReturn(dialect);
    return meta;
  }

  private static DatabaseColumn column(int sqlType, String nativeTypeName) throws SQLException {
    ResultSetMetaData rm = mock(ResultSetMetaData.class);
    when(rm.getColumnName(1)).thenReturn("COL");
    when(rm.getColumnLabel(1)).thenReturn("COL");
    when(rm.getColumnType(1)).thenReturn(sqlType);
    when(rm.getColumnTypeName(1)).thenReturn(nativeTypeName);
    when(rm.getPrecision(1)).thenReturn(0);
    when(rm.getScale(1)).thenReturn(0);
    when(rm.getColumnDisplaySize(1)).thenReturn(0);
    when(rm.isSigned(1)).thenReturn(true);
    return DatabaseColumn.of(rm, 1);
  }

  // ------------------------------------------------------------------ legacy variant bridge

  /** A dialect from outside Hop that has not migrated: it only answers the deprecated flags. */
  static class UnmigratedExternalDialect extends NoneDatabaseMeta {
    @Override
    public boolean isMySqlVariant() {
      return true;
    }
  }

  @Test
  void anExternalDialectThatOnlyAnswersTheVariantFlagStillGetsItsRules() throws Exception {
    // The MySQL YEAR handling moved out of core's switch into a rule. A dialect that never heard
    // of getTypeRules() has to keep behaving exactly as it did.
    DatabaseMeta meta = mySqlStyleMeta(new UnmigratedExternalDialect(), "false");

    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(variables, meta, column(Types.DATE, "YEAR"), false, false);

    assertTrue(valueMeta.isInteger());
    assertEquals(4, valueMeta.getLength());
    assertEquals(0, valueMeta.getPrecision());
  }

  @Test
  void theYearRuleDefersWhenTheDriverTreatsYearAsADate() throws Exception {
    // yearIsDateType unset, so the column really is a date and the rule must not claim it.
    DatabaseMeta meta = mySqlStyleMeta(new UnmigratedExternalDialect(), null);

    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(variables, meta, column(Types.DATE, "YEAR"), false, false);

    assertTrue(valueMeta.isDate());
  }

  @Test
  void theYearRuleOnlyClaimsColumnsActuallyNamedYear() throws Exception {
    DatabaseMeta meta = mySqlStyleMeta(new UnmigratedExternalDialect(), "false");

    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(variables, meta, column(Types.DATE, "DATE"), false, false);

    assertTrue(valueMeta.isDate());
  }

  /** A dialect that has migrated but still answers the flag, as it will during the deprecation. */
  static class MigratedDialect extends UnmigratedExternalDialect {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().readNative("YEAR").as(IValueMeta.TYPE_STRING, 99, 0).build();
    }
  }

  @Test
  void aMigratedDialectsOwnRulesWinOverTheLegacyBridge() throws Exception {
    DatabaseMeta meta = mySqlStyleMeta(new MigratedDialect(), "false");

    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(variables, meta, column(Types.DATE, "YEAR"), false, false);

    assertTrue(valueMeta.isString());
    assertEquals(99, valueMeta.getLength());
  }

  private static DatabaseMeta mySqlStyleMeta(IDatabase dialect, String yearIsDateType) {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    when(meta.getIDatabase()).thenReturn(dialect);
    Properties properties = new Properties();
    if (yearIsDateType != null) {
      properties.setProperty("yearIsDateType", yearIsDateType);
    }
    when(meta.getConnectionProperties(org.mockito.ArgumentMatchers.any())).thenReturn(properties);
    return meta;
  }
}
