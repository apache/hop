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
package org.apache.hop.databases.h2;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** What Database.checkTableExists and checkColumnExists answer on a real database. */
class H2ExistenceCheckTest {

  private Database db;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    String name =
        "mem:exists" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta = new DatabaseMeta("h2", "H2", "Native", "", name, "", "sa", "");
    db = new Database(new LoggingObject("exists"), new Variables(), databaseMeta);
    db.connect();
    db.execStatements(
        "CREATE SCHEMA SALES;"
            + "CREATE TABLE SALES.ORDERS (ID INTEGER, \"ORDER\" INTEGER);"
            + "CREATE TABLE CUSTOMERS (ID INTEGER, NAME VARCHAR(20));"
            + "INSERT INTO CUSTOMERS VALUES (1, 'a'), (2, 'b');");
  }

  @AfterEach
  void tearDown() {
    db.disconnect();
  }

  @Test
  void anExistingTableExistsWhetherItIsEmptyOrNot() throws Exception {
    assertTrue(db.checkTableExists(null, "CUSTOMERS"));
    assertTrue(db.checkTableExists("SALES", "ORDERS"));
  }

  @Test
  void aMissingTableDoesNotExist() throws Exception {
    assertFalse(db.checkTableExists(null, "MISSING"));
    assertFalse(db.checkTableExists("SALES", "MISSING"));
    assertFalse(db.checkTableExists("SALES", "CUSTOMERS"));
    assertFalse(db.checkTableExists("NO_SCHEMA", "ORDERS"));
  }

  @Test
  void anExistingColumnExists() throws Exception {
    assertTrue(db.checkColumnExists(null, "CUSTOMERS", "NAME"));
    assertTrue(db.checkColumnExists("SALES", "ORDERS", "ID"));
  }

  @Test
  void aColumnWithAReservedWordAsItsNameExists() throws Exception {
    assertTrue(db.checkColumnExists("SALES", "ORDERS", "ORDER"));
  }

  @Test
  void aMissingColumnOrTableDoesNotExist() throws Exception {
    assertFalse(db.checkColumnExists(null, "CUSTOMERS", "MISSING"));
    assertFalse(db.checkColumnExists(null, "MISSING", "ID"));
    assertFalse(db.checkColumnExists("SALES", "ORDERS", "NAME"));
  }

  @Test
  void manyChecksInARowKeepGivingTheRightAnswer() throws Exception {
    for (int i = 0; i < 200; i++) {
      assertFalse(db.checkTableExists(null, "MISSING_" + (i % 7)));
      assertFalse(db.checkColumnExists(null, "CUSTOMERS", "MISSING_" + (i % 5)));
    }
    assertTrue(db.checkTableExists(null, "CUSTOMERS"));
    assertTrue(db.checkColumnExists(null, "CUSTOMERS", "ID"));
  }
}
