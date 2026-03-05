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
package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class BaseDatabaseMetaTest {
  BaseDatabaseMeta nativeMeta;

  @BeforeEach
  void setupOnce() throws Exception {
    nativeMeta = new ConcreteBaseDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    HopClientEnvironment.init();
  }

  @Test
  void testDefaultSettings() throws Exception {
    // Note - this method should only use native.
    // (each test run in its own thread).
    assertEquals("", nativeMeta.getLimitClause(5));
    assertEquals(0, nativeMeta.getNotFoundTK(true));
    assertFalse(nativeMeta.isNeedsPlaceHolder());
    assertEquals(DatabaseMeta.CLOB_LENGTH, nativeMeta.getMaxTextFieldLength());
    assertEquals(DatabaseMeta.CLOB_LENGTH, nativeMeta.getMaxVARCHARLength());
    assertNull(nativeMeta.getSqlListOfProcedures());
    assertNull(nativeMeta.getSqlListOfSequences());
    assertTrue(nativeMeta.isSupportsFloatRoundingOnUpdate());
    assertNull(nativeMeta.getSqlLockTables(new String[] {"FOO"}));
    assertNull(nativeMeta.getSqlUnlockTables(new String[] {"FOO"}));

    assertFalse(nativeMeta.isForcingIdentifiersToLowerCase());
    assertFalse(nativeMeta.isForcingIdentifiersToUpperCase());
    assertFalse(nativeMeta.isUsingDoubleDecimalAsSchemaTableSeparator());
    assertTrue(nativeMeta.isRequiringTransactionsOnQueries());
    assertEquals(
        "org.apache.hop.core.database.DatabaseFactory", nativeMeta.getDatabaseFactoryName());
    assertNull(nativeMeta.getPreferredSchemaName());
    assertFalse(nativeMeta.isRequiresCreateTablePrimaryKeyAppend());
    assertFalse(nativeMeta.isRequiresCastToVariousForIsNull());
    assertFalse(nativeMeta.isDisplaySizeTwiceThePrecision());
    Variables v = new Variables();
    v.setVariable("FOOVARIABLE", "FOOVALUE");
    DatabaseMeta dm = new DatabaseMeta();
    dm.setIDatabase(nativeMeta);
    assertEquals("'DATA'", nativeMeta.getSqlValue(new ValueMetaString("FOO"), "DATA", null));
    assertEquals("'15'", nativeMeta.getSqlValue(new ValueMetaString("FOO"), "15", null));
    assertEquals("_", nativeMeta.getFieldnameProtector());
    assertEquals("_1ABC_123", nativeMeta.getSafeFieldname("1ABC 123"));
    assertNull(nativeMeta.customizeValueFromSqlType(new ValueMetaString("FOO"), null, 0));
    assertTrue(nativeMeta.isFullExceptionLog(new RuntimeException("xxxx")));
  }

  @Test
  void testConnectionAndTransactionDefaults() {
    assertEquals(-1, nativeMeta.getDefaultDatabasePort());
    assertTrue(nativeMeta.isSupportsSetCharacterStream());
    assertTrue(nativeMeta.isSupportsAutoInc());
    assertTrue(nativeMeta.isSupportsTransactions());
    assertTrue(nativeMeta.isSupportsBatchUpdates());
    assertFalse(nativeMeta.isSupportsSequences());
    assertTrue(nativeMeta.isSupportsBitmapIndex());
    assertTrue(nativeMeta.isSupportsSetLong());
    assertTrue(nativeMeta.isSupportsErrorHandling());
    assertFalse(nativeMeta.isUseSafePoints());
    assertTrue(nativeMeta.isFetchSizeSupported());
    assertTrue(nativeMeta.isSupportsTimeStampToDateConversion());
    assertFalse(nativeMeta.isSupportsBooleanDataType());
    assertFalse(nativeMeta.isSupportsTimestampDataType());
    assertTrue(nativeMeta.isSupportsGetBlob());
    assertNull(nativeMeta.getConnectSql());
    assertTrue(nativeMeta.isSupportsSetMaxRows());
    assertTrue(nativeMeta.isStreamingResults());
    assertTrue(nativeMeta.isSupportsAutoGeneratedKeys());
  }

  @Test
  void testSqlFunctionDefaults() {
    assertEquals("SUM", nativeMeta.getFunctionSum());
    assertEquals("AVG", nativeMeta.getFunctionAverage());
    assertEquals("MIN", nativeMeta.getFunctionMinimum());
    assertEquals("MAX", nativeMeta.getFunctionMaximum());
    assertEquals("COUNT", nativeMeta.getFunctionCount());
  }

  @Test
  void testQuotingAndReservedWordsDefaults() {
    assertEquals("\"", nativeMeta.getStartQuote());
    assertEquals("\"", nativeMeta.getEndQuote());
    assertTrue(nativeMeta.isQuoteReservedWords());
    assertFalse(nativeMeta.isQuoteAllFields());
    assertTrue(nativeMeta.isPreserveReservedCase());
    assertTrue(nativeMeta.isDefaultingToUppercase());
    assertArrayEquals(new String[] {}, nativeMeta.getReservedWords());
  }

  @Test
  void testSchemaTableAndViewDefaults() {
    assertTrue(nativeMeta.isSupportsSchemas());
    assertTrue(nativeMeta.isSupportsCatalogs());
    assertTrue(nativeMeta.isSupportsEmptyTransactions());
    assertEquals("FOO.BAR", nativeMeta.getSchemaTableCombination("FOO", "BAR"));
    assertArrayEquals(new String[] {"TABLE"}, nativeMeta.getTableTypes());
    assertArrayEquals(new String[] {"VIEW"}, nativeMeta.getViewTypes());
    assertArrayEquals(new String[] {"SYNONYM"}, nativeMeta.getSynonymTypes());
    assertFalse(nativeMeta.useSchemaNameForTableList());
    assertTrue(nativeMeta.isSupportsViews());
    assertFalse(nativeMeta.isSupportsSynonyms());
  }

  @Test
  void testSequenceDefaults() {
    assertEquals("", nativeMeta.getSqlNextSequenceValue("FOO"));
    assertEquals("", nativeMeta.getSqlCurrentSequenceValue("FOO"));
    assertEquals("", nativeMeta.getSqlSequenceExists("FOO"));
    assertEquals("NOMAXVALUE", nativeMeta.getSequenceNoMaxValueOption());
    assertFalse(nativeMeta.isSupportsSequenceNoMaxValueOption());
  }

  @Test
  void testExtraOptionsDefaults() {
    Map<String, String> emptyMap = new HashMap<>();
    assertEquals(emptyMap, nativeMeta.getExtraOptions());
    assertEquals(";", nativeMeta.getExtraOptionSeparator());
    assertEquals("=", nativeMeta.getExtraOptionValueSeparator());
    assertEquals(";", nativeMeta.getExtraOptionIndicator());
    assertTrue(nativeMeta.isSupportsOptionsInURL());
    assertNull(nativeMeta.getExtraOptionsHelpText());
  }

  @Test
  void testMiscDefaults() {
    assertTrue(nativeMeta.isSupportsPreparedStatementMetadataRetrieval());
    assertFalse(nativeMeta.isSupportsResultSetMetadataRetrievalOnly());
    assertFalse(nativeMeta.isSystemTable("FOO"));
    assertTrue(nativeMeta.isSupportsNewLinesInSql());
    assertNull(nativeMeta.getSqlListOfSchemas());
    assertEquals(0, nativeMeta.getMaxColumnsInIndex());
    assertTrue(nativeMeta.IsSupportsErrorHandlingOnBatchUpdates());
    assertTrue(nativeMeta.isExplorable());
    assertTrue(nativeMeta.onlySpaces("   \t   \n  \r   "));
    assertFalse(nativeMeta.isMySqlVariant());
    assertTrue(nativeMeta.canTest());
    assertTrue(nativeMeta.isRequiresName());
    assertTrue(nativeMeta.isReleaseSavepoint());
  }

  @Test
  void testTablespaceAndSafeNameDefaults() {
    Variables v = new Variables();
    v.setVariable("FOOVARIABLE", "FOOVALUE");
    DatabaseMeta dm = new DatabaseMeta();
    dm.setIDatabase(nativeMeta);
    assertEquals("", nativeMeta.getDataTablespaceDDL(v, dm));
    assertEquals("", nativeMeta.getIndexTablespaceDDL(v, dm));

    BaseDatabaseMeta tmpSC =
        new ConcreteBaseDatabaseMeta() {
          @Override
          public String[] getReservedWords() {
            return new String[] {"SELECT"};
          }
        };
    assertEquals("SELECT_", tmpSC.getSafeFieldname("SELECT"));
  }

  @Test
  void testDefaultSqlStatements() {
    // Note - this method should use only native metas.
    assertEquals(
        "insert into \"FOO\".\"BAR\"(KEYFIELD, VERSIONFIELD) values (0, 1)",
        nativeMeta.getSqlInsertAutoIncUnknownDimensionRow(
            "\"FOO\".\"BAR\"", "KEYFIELD", "VERSIONFIELD"));
    assertEquals("select count(*) FROM FOO", nativeMeta.getSelectCountStatement("FOO"));
    assertEquals("COL9", nativeMeta.generateColumnAlias(9, "FOO"));
    assertEquals(
        "[SELECT 1, INSERT INTO FOO VALUES(BAR), DELETE FROM BAR]",
        nativeMeta
            .parseStatements("SELECT 1;INSERT INTO FOO VALUES(BAR);DELETE FROM BAR")
            .toString());
    assertEquals("CREATE TABLE ", nativeMeta.getCreateTableStatement());
    assertEquals("DROP TABLE IF EXISTS FOO", nativeMeta.getDropTableIfExistsStatement("FOO"));
  }

  @Test
  void testGettersSetters() {
    nativeMeta.setUsername("FOO");
    assertEquals("FOO", nativeMeta.getUsername());
    nativeMeta.setPassword("BAR");
    assertEquals("BAR", nativeMeta.getPassword());
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    assertEquals("FOO", nativeMeta.getUsername());
    assertEquals("BAR", nativeMeta.getPassword());
    assertFalse(nativeMeta.isChanged());
    nativeMeta.setChanged(true);
    assertTrue(nativeMeta.isChanged());
    nativeMeta.setDatabaseName("FOO");
    assertEquals("FOO", nativeMeta.getDatabaseName());
    nativeMeta.setHostname("FOO");
    assertEquals("FOO", nativeMeta.getHostname());
    nativeMeta.setServername("FOO");
    assertEquals("FOO", nativeMeta.getServername());
    nativeMeta.setDataTablespace("FOO");
    assertEquals("FOO", nativeMeta.getDataTablespace());
    nativeMeta.setIndexTablespace("FOO");
    assertEquals("FOO", nativeMeta.getIndexTablespace());
    Map<String, String> attrs = nativeMeta.getAttributes();
    Map<String, String> testAttrs = new HashMap<>();
    testAttrs.put("FOO", "BAR");
    nativeMeta.setAttributes(testAttrs);
    assertEquals(testAttrs, nativeMeta.getAttributes());
    nativeMeta.setAttributes(attrs); // reset attributes back to what they were...
    nativeMeta.setSupportsBooleanDataType(true);
    assertTrue(nativeMeta.isSupportsBooleanDataType());
    nativeMeta.setSupportsTimestampDataType(true);
    assertTrue(nativeMeta.isSupportsTimestampDataType());
    nativeMeta.setPreserveReservedCase(false);
    assertFalse(nativeMeta.isPreserveReservedCase());
    nativeMeta.addExtraOption("JNDI", "FOO", "BAR");
    Map<String, String> expectedOptionsMap = new HashMap<>();
    expectedOptionsMap.put("JNDI.FOO", "BAR");
    assertEquals(expectedOptionsMap, nativeMeta.getExtraOptions());
    nativeMeta.setConnectSql("SELECT COUNT(*) FROM FOO");
    assertEquals("SELECT COUNT(*) FROM FOO", nativeMeta.getConnectSql());
    // MB: Can't use arrayEquals because the PartitionDatabaseMeta doesn't have a toString. :(

    nativeMeta.setStreamingResults(false);
    assertFalse(nativeMeta.isStreamingResults());
    nativeMeta.setQuoteAllFields(true);
    nativeMeta.setForcingIdentifiersToLowerCase(true);
    nativeMeta.setForcingIdentifiersToUpperCase(true);
    assertTrue(nativeMeta.isQuoteAllFields());
    assertTrue(nativeMeta.isForcingIdentifiersToLowerCase());
    assertTrue(nativeMeta.isForcingIdentifiersToUpperCase());
    nativeMeta.setUsingDoubleDecimalAsSchemaTableSeparator(true);
    assertTrue(nativeMeta.isUsingDoubleDecimalAsSchemaTableSeparator());
    nativeMeta.setPreferredSchemaName("FOO");
    assertEquals("FOO", nativeMeta.getPreferredSchemaName());
  }

  private int rowCnt = 0;

  @Test
  void testCheckIndexExists() throws Exception {
    Database db = Mockito.mock(Database.class);
    ResultSet rs = Mockito.mock(ResultSet.class);
    DatabaseMetaData dmd = Mockito.mock(DatabaseMetaData.class);
    DatabaseMeta dm = Mockito.mock(DatabaseMeta.class);
    when(dm.getQuotedSchemaTableCombination(db, "", "FOO")).thenReturn("FOO");
    when(rs.next())
        .thenAnswer(
            (Answer<Boolean>)
                invocation -> {
                  rowCnt++;
                  return rowCnt < 3;
                });
    when(db.getDatabaseMetaData()).thenReturn(dmd);
    when(dmd.getIndexInfo(null, null, "FOO", false, true)).thenReturn(rs);
    when(rs.getString("COLUMN_NAME"))
        .thenAnswer(
            (Answer<String>)
                invocation -> {
                  if (rowCnt == 1) {
                    return "ROW1COL2";
                  } else if (rowCnt == 2) {
                    return "ROW2COL2";
                  } else {
                    return null;
                  }
                });
    when(db.getDatabaseMeta()).thenReturn(dm);

    dmd.getIndexInfo(null, null, "FOO", false, true);
    verify(dmd).getIndexInfo(null, null, "FOO", false, true);
  }

  /**
   * Build a String meta that mimics what getValueFromSqlType might have produced before
   * customization: name set, BINARY_STRING storage with String storage metadata, etc.
   */
  private IValueMeta buildPreMeta() {
    ValueMetaString v = new ValueMetaString("id");
    v.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    ValueMetaString storage = new ValueMetaString("id");
    storage.setStringEncoding("UTF-8");
    v.setStorageMetadata(storage);
    v.setLength(36);
    v.setPrecision(0);
    v.setOriginalColumnType(java.sql.Types.OTHER);
    v.setOriginalColumnTypeName("uuid");
    return v;
  }

  @Test
  void testCustomizeValueFromSqlTypeUuid() throws Exception {
    int uuidTypeId;
    try {
      uuidTypeId = ValueMetaFactory.getIdForValueMeta("UUID");
    } catch (Exception ignore) {
      // UUID plugin not present:, skip the rest
      return;
    }

    ResultSetMetaData rm = Mockito.mock(ResultSetMetaData.class);
    when(rm.getColumnTypeName(1)).thenReturn("UUID");

    IValueMeta pre = buildPreMeta();
    IValueMeta out = nativeMeta.customizeValueFromSqlType(pre, rm, 1);

    assertEquals(uuidTypeId, out.getType());
    assertEquals("id", out.getName());
    // length/precision reset
    assertEquals(-1, out.getLength());
    assertEquals(-1, out.getPrecision());
    // storage type preserved
    assertEquals(pre.getStorageType(), out.getStorageType());
    assertNotNull(out.getStorageMetadata());
    assertTrue(out.getStorageMetadata().isString());
    // original JDBC metadata preserved by clone
    assertEquals(pre.getOriginalColumnType(), out.getOriginalColumnType());
    assertEquals(pre.getOriginalColumnTypeName(), out.getOriginalColumnTypeName());
  }
}
