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

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Where the binding learns which column it is writing to.
 *
 * <p>Rows arriving at a table say nothing about the columns they are bound to, so the target table
 * is read while the insert is being prepared. Which table answers that question is what these tests
 * are about: asked without a schema, every schema the user can see may answer, and the columns come
 * back with nothing but their name to tell them apart.
 */
class OracleInsertRowMetaEnrichmentTest {

  private static final String TABLE = "CUSTOMER";

  private Database database;
  private Connection connection;
  private DatabaseMetaData dbmd;

  @BeforeEach
  void setUp() throws Exception {
    database = mock(Database.class);
    connection = mock(Connection.class);
    dbmd = mock(DatabaseMetaData.class);
    when(database.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(dbmd);
    when(connection.getSchema()).thenReturn("SALES");
    // The SELECT based half of the enrichment is not what these tests are about.
    when(database.getTableFieldsMeta(any(), any())).thenReturn(null);
  }

  /** A getColumns() result holding a single column. Built before it is handed to a stub. */
  private static ResultSet oneColumn(String name, int sqlType, String typeName, int size)
      throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("COLUMN_NAME")).thenReturn(name);
    when(rs.getInt("DATA_TYPE")).thenReturn(sqlType);
    when(rs.getString("TYPE_NAME")).thenReturn(typeName);
    when(rs.getInt("COLUMN_SIZE")).thenReturn(size);
    return rs;
  }

  private static ResultSet noColumns() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(false);
    return rs;
  }

  private static IRowMeta oneStringField(String name) {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(name, 20, 0));
    return rowMeta;
  }

  @Test
  void theSchemaTheInsertResolvesAgainstIsAskedFirst() throws Exception {
    ResultSet columns = oneColumn("NAME", Types.NVARCHAR, "NVARCHAR2", 50);
    when(dbmd.getColumns(isNull(), eq("SALES"), eq(TABLE), isNull())).thenReturn(columns);

    IRowMeta rowMeta = oneStringField("NAME");
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, null, TABLE, rowMeta);

    assertEquals(Types.NVARCHAR, rowMeta.getValueMeta(0).getOriginalColumnType());
    assertEquals("NVARCHAR2", rowMeta.getValueMeta(0).getOriginalColumnTypeName());
    // The question that would have let another schema answer is never asked.
    verify(dbmd, never()).getColumns(isNull(), isNull(), eq(TABLE), isNull());
  }

  /** A synonym, or a grant from elsewhere: the narrow question finds nothing. */
  @Test
  void theWiderQuestionIsAskedOnlyWhenTheSchemaHasNoSuchTable() throws Exception {
    ResultSet empty = noColumns();
    ResultSet columns = oneColumn("NAME", Types.NCLOB, "NCLOB", 4000);
    when(dbmd.getColumns(isNull(), eq("SALES"), eq(TABLE), isNull())).thenReturn(empty);
    when(dbmd.getColumns(isNull(), isNull(), eq(TABLE), isNull())).thenReturn(columns);

    IRowMeta rowMeta = oneStringField("NAME");
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, null, TABLE, rowMeta);

    assertEquals(Types.NCLOB, rowMeta.getValueMeta(0).getOriginalColumnType());
  }

  @Test
  void anExplicitSchemaIsUsedAsGiven() throws Exception {
    ResultSet columns = oneColumn("NAME", Types.NCHAR, "NCHAR", 10);
    when(dbmd.getColumns(isNull(), eq("WAREHOUSE"), eq(TABLE), isNull())).thenReturn(columns);

    IRowMeta rowMeta = oneStringField("NAME");
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, "warehouse", TABLE, rowMeta);

    assertEquals(Types.NCHAR, rowMeta.getValueMeta(0).getOriginalColumnType());
    verify(dbmd, never()).getColumns(isNull(), eq("SALES"), eq(TABLE), isNull());
  }

  /** An older driver without getSchema() still gets the lookup it had before. */
  @Test
  void aDriverThatWillNotSayFallsBackToTheWiderQuestion() throws Exception {
    ResultSet columns = oneColumn("NAME", Types.NVARCHAR, "NVARCHAR2", 50);
    when(connection.getSchema()).thenThrow(new SQLException("not implemented"));
    when(dbmd.getColumns(isNull(), isNull(), eq(TABLE), isNull())).thenReturn(columns);

    IRowMeta rowMeta = oneStringField("NAME");
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, null, TABLE, rowMeta);

    assertEquals(Types.NVARCHAR, rowMeta.getValueMeta(0).getOriginalColumnType());
  }

  /** A field the table has no column for is left exactly as the stream described it. */
  @Test
  void aFieldWithNoMatchingColumnIsLeftAlone() throws Exception {
    ResultSet columns = oneColumn("NAME", Types.NVARCHAR, "NVARCHAR2", 50);
    when(dbmd.getColumns(isNull(), eq("SALES"), eq(TABLE), isNull())).thenReturn(columns);

    IRowMeta rowMeta = oneStringField("SOMETHING_ELSE");
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, null, TABLE, rowMeta);

    assertEquals(20, rowMeta.getValueMeta(0).getLength());
    assertNull(rowMeta.getValueMeta(0).getOriginalColumnTypeName());
  }

  /**
   * The SELECT based half. It resolves the table through the session the way the insert itself
   * does, and is what normally supplies the column types; the JDBC lookup only refines them.
   */
  @Test
  void theColumnTypesAreTakenFromTheTableItself() throws Exception {
    IRowMeta tableFields = new RowMeta();
    ValueMetaString column = new ValueMetaString("NAME", 50, 0);
    column.setOriginalColumnType(Types.NVARCHAR);
    column.setOriginalColumnTypeName("NVARCHAR2");
    tableFields.addValueMeta(column);
    ResultSet emptyNarrow = noColumns();
    ResultSet emptyWide = noColumns();
    when(database.getTableFieldsMeta(isNull(), eq(TABLE))).thenReturn(tableFields);
    when(dbmd.getColumns(isNull(), eq("SALES"), eq(TABLE), isNull())).thenReturn(emptyNarrow);
    when(dbmd.getColumns(isNull(), isNull(), eq(TABLE), isNull())).thenReturn(emptyWide);

    IRowMeta rowMeta = oneStringField("NAME");
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, null, TABLE, rowMeta);

    assertEquals(Types.NVARCHAR, rowMeta.getValueMeta(0).getOriginalColumnType());
    assertEquals("NVARCHAR2", rowMeta.getValueMeta(0).getOriginalColumnTypeName());
    assertEquals(50, rowMeta.getValueMeta(0).getLength());
  }

  /** What the table says is kept when the JDBC lookup has nothing to add. */
  @Test
  void aJdbcLookupThatFindsNothingLeavesTheTableAnswerAlone() throws Exception {
    IRowMeta tableFields = new RowMeta();
    ValueMetaString column = new ValueMetaString("NAME", 50, 0);
    column.setOriginalColumnType(Types.NCLOB);
    column.setOriginalColumnTypeName("NCLOB");
    tableFields.addValueMeta(column);
    ResultSet emptyNarrow = noColumns();
    ResultSet emptyWide = noColumns();
    when(database.getTableFieldsMeta(isNull(), eq(TABLE))).thenReturn(tableFields);
    when(dbmd.getColumns(isNull(), eq("SALES"), eq(TABLE), isNull())).thenReturn(emptyNarrow);
    when(dbmd.getColumns(isNull(), isNull(), eq(TABLE), isNull())).thenReturn(emptyWide);

    IRowMeta rowMeta = oneStringField("NAME");
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, null, TABLE, rowMeta);

    assertEquals(Types.NCLOB, rowMeta.getValueMeta(0).getOriginalColumnType());
  }

  @Test
  void anEmptyRowMetaAsksTheDatabaseNothing() throws Exception {
    OraclePreparedStatementBinding.enrichInsertRowMeta(database, null, TABLE, new RowMeta());
    verify(dbmd, never()).getColumns(any(), any(), any(), any());
  }
}
