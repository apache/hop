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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A type no database claimed, or one the server turns out not to have, is written as something it
 * can hold rather than as a name that fails at CREATE TABLE.
 */
class ColumnTypeFallbackTest {

  /** Spells a string as TEXT and has a JSON type from its version 17 on. */
  @DatabaseMetaPlugin(type = "FALLBACK_TEST", typeDescription = "Fallback test dialect")
  static class TestDialect extends NoneDatabaseMeta {
    @Override
    public boolean isColumnTypeAvailable(String columnType) {
      return !"JSON".equals(columnType) || serverIsAtLeast(17);
    }

    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().write(IValueMeta.TYPE_JSON).as("JSON").build();
    }

    @Override
    public String getFieldDefinition(
        IValueMeta v,
        String tk,
        String pk,
        boolean useAutoIncrement,
        boolean addFieldName,
        boolean addCr) {
      String column = addFieldName ? v.getName() + " " : "";
      return switch (v.getType()) {
        case IValueMeta.TYPE_STRING -> column + "TEXT(" + v.getLength() + ")";
        case IValueMeta.TYPE_DATE -> column + "DATE";
        default -> column + " UNKNOWN";
      };
    }
  }

  /** Claims nothing at all, the way most dialects do. */
  @DatabaseMetaPlugin(type = "SILENT_TEST", typeDescription = "Silent test dialect")
  static class SilentDialect extends TestDialect {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return List.of();
    }
  }

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    DatabaseTypeRuleRegistry.clearCache();
  }

  private static String definition(IDatabase database, IValueMeta valueMeta) {
    return DatabaseTypeMapper.getColumnDefinition(
        null, database, valueMeta, new ColumnContext(null, null, null, false, false, false));
  }

  @Test
  void aTypeNoDialectClaimedIsWrittenAsText() {
    assertEquals("TEXT(9999999)", definition(new SilentDialect(), new ValueMetaJson("payload")));
  }

  @Test
  void aDeclaredTypeStandsWhenNothingContradictsIt() {
    assertEquals("JSON", definition(new TestDialect(), new ValueMetaJson("payload")));
  }

  @Test
  void aDeclaredTypeTheServerIsTooOldForBecomesTextInstead() {
    TestDialect database = new TestDialect();
    database.setServerInfo(new ServerInfo(16, 0, Set.of()));

    assertEquals("TEXT(9999999)", definition(database, new ValueMetaJson("payload")));
  }

  @Test
  void aDeclaredTypeTheServerIsNewEnoughForStands() {
    TestDialect database = new TestDialect();
    database.setServerInfo(new ServerInfo(17, 0, Set.of()));

    assertEquals("JSON", definition(database, new ValueMetaJson("payload")));
  }

  /** Not knowing which version answered is not a reason to downgrade. */
  @Test
  void anUnknownServerVersionLeavesTheDeclaredTypeStanding() {
    TestDialect database = new TestDialect();
    database.setServerInfo(ServerInfo.UNKNOWN);

    assertEquals("JSON", definition(database, new ValueMetaJson("payload")));
  }

  @Test
  void aTypeTheDialectWritesItselfIsLeftAlone() {
    TestDialect database = new TestDialect();
    database.setServerInfo(new ServerInfo(16, 0, Set.of()));

    assertEquals("TEXT(20)", definition(database, new ValueMetaString("name", 20, 0)));
  }

  /**
   * The substitutes the deprecated SUPPORTS_TIMESTAMP_DATA_TYPE and SUPPORTS_BOOLEAN_DATA_TYPE
   * attributes pick by hand, so that the two can be answered by the driver instead.
   */
  @Test
  void aTimestampFallsBackToADateAndABooleanToASingleCharacter() {
    IValueMeta timestamp = ColumnTypeFallback.substituteFor(new ValueMetaTimestamp("when"));
    assertTrue(timestamp.isDate());

    IValueMeta bool = ColumnTypeFallback.substituteFor(new ValueMetaBoolean("flag"));
    assertTrue(bool.isString());
    assertEquals(1, bool.getLength());
  }
}
