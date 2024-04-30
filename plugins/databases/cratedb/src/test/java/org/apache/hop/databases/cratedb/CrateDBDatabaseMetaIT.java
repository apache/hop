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

package org.apache.hop.databases.cratedb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.testcontainers.cratedb.CrateDBContainer;

public class CrateDBDatabaseMetaIT {

  @ClassRule public static CrateDBContainer crateDBContainer = new CrateDBContainer("crate");

  private static Connection connection;

  private CrateDbDatabaseMeta nativeMeta = new CrateDbDatabaseMeta();

  @BeforeClass
  public static void setup() throws Exception {
    connection = crateDBContainer.createConnection("");
  }

  @Before
  public void setUp() throws Exception {
    executeUpdate("DROP TABLE IF EXISTS foo;");
    executeUpdate("CREATE TABLE foo (id INT PRIMARY KEY, name VARCHAR(100), description TEXT);");
  }

  @Test
  public void testSimpleSelect() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT 1");
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getInt(1));
  }

  @Test
  public void doNotSupportSequences() {
    assertFalse(nativeMeta.isSupportsSequences());
    assertThrows(
        UnsupportedOperationException.class,
        () -> executeUpdate(nativeMeta.getSqlListOfSequences()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> executeUpdate(nativeMeta.getSqlSequenceExists("FOO")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> executeUpdate(nativeMeta.getSqlCurrentSequenceValue("FOO")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> executeUpdate(nativeMeta.getSqlNextSequenceValue("FOO")));
  }

  @Test
  public void sqlStatements() throws Exception {
    executeUpdate(
        "INSERT INTO foo (id, name, description) VALUES (1, 'Alice', 'test_description');");
    Thread.sleep(1500); // need a break to make sure the data is there: unfortunately,
    // CrateDB does not support transactions, rather it promote eventual consistency.
    // Using an async lib for assertions like awaitility would be a better approach
    int counter = 0;
    ResultSet rs = executeQuery(nativeMeta.getSqlQueryFields("foo"));
    while (rs.next()) {
      counter++;
      assertTrue("Alice".equals(rs.getString("name")));
    }
    assertTrue(counter > 0);

    counter = 0;
    rs = executeQuery(nativeMeta.getSqlTableExists("foo"));
    while (rs.next()) {
      counter++;
      assertTrue("Alice".equals(rs.getString("name")));
    }
    assertTrue(counter > 0);

    counter = 0;
    rs = executeQuery(nativeMeta.getSqlQueryColumnFields("name", "foo"));
    while (rs.next()) {
      counter++;
      assertTrue("Alice".equals(rs.getString("name")));
    }
    assertTrue(counter > 0);

    counter = 0;
    rs = executeQuery(nativeMeta.getSqlColumnExists("name", "foo"));
    while (rs.next()) {
      counter++;
      assertTrue("Alice".equals(rs.getString("name")));
    }
    assertTrue(counter > 0);
  }

  @Test
  public void addTimestampColumn() throws Exception {
    executeUpdate(
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaDate("BAR"), "", false, "", false));
    executeUpdate(
        "INSERT INTO foo (id, name, description, bar) VALUES (1, 'test_name', 'test_description', CURRENT_TIMESTAMP);");
    ResultSet resultSet =
        connection.createStatement().executeQuery("SELECT bar FROM foo WHERE id = 1");
    assertTrue(resultSet.next());
  }

  @Test
  public void addNumberColumn() throws Exception {
    executeUpdate(
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, 3), "", false, "", false));
    executeUpdate(
        "INSERT INTO foo (id, name, description, bar) VALUES (1, 'test_name', 'test_description', 1234567890.123);");
    ResultSet resultSet =
        connection.createStatement().executeQuery("SELECT bar FROM foo WHERE id = 1");
    assertTrue(resultSet.next());
    assertEquals(1234567890.123, resultSet.getDouble(1), 0.001);
  }

  @Test
  public void addBigNumber() throws Exception {
    executeUpdate(
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 21, 4), "", false, "", false));
    executeUpdate(
        "INSERT INTO foo (id, name, description, bar) VALUES (1, 'test_name', 'test_description', 123456789012345678901.1234);");
    ResultSet resultSet =
        connection.createStatement().executeQuery("SELECT bar FROM foo WHERE id = 1");
    assertTrue(resultSet.next());
    assertEquals(123456789012345678901.1234, resultSet.getDouble(1), 0.0001);
  }

  @Test
  public void addStringColumnWithLength() throws Exception {
    executeUpdate(
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", false));
    executeUpdate(
        "INSERT INTO foo (id, name, description, bar) VALUES (1, 'test_name', 'test_description', '0123456789ABCDE');");
    assertThrows(
        PSQLException.class,
        () ->
            executeUpdate(
                "INSERT INTO foo (id, name, description, bar) VALUES (2, 'test_name', 'test_description', '0123456789ABCDEF');"));
  }

  @Test
  public void addLongTextColumn() throws Exception {
    executeUpdate(
        nativeMeta.getAddColumnStatement(
            "FOO",
            new ValueMetaString("BAR", nativeMeta.getMaxVARCHARLength() + 2, 0),
            "",
            false,
            "",
            false));
    // here an assertDoesNotThrow would be better, but we are using JUnit 4
  }

  @Test
  public void doesNotSupportLockTables() throws Exception {
    assertThrows(
        UnsupportedOperationException.class,
        () -> executeUpdate(nativeMeta.getSqlLockTables(new String[] {"FOO", "BAR"})));
  }

  private int executeUpdate(String query) throws Exception {
    try (Statement statement = connection.createStatement()) {
      return statement.executeUpdate(query);
    }
  }

  private ResultSet executeQuery(String query) throws Exception {
    Statement statement = connection.createStatement();
    return statement.executeQuery(query);
  }
}
