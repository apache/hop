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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
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

/**
 * The existence checks close every statement they open, also when the table or column does not
 * exist and the check query fails.
 */
class H2ExistenceCheckStatementTest {

  private Database db;
  private final AtomicInteger opened = new AtomicInteger();
  private final AtomicInteger closed = new AtomicInteger();

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    String name =
        "mem:statements" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta = new DatabaseMeta("h2", "H2", "Native", "", name, "", "sa", "");
    db = new Database(new LoggingObject("statements"), new Variables(), databaseMeta);
    db.connect();
    db.execStatement("CREATE TABLE CUSTOMERS (ID INTEGER, NAME VARCHAR(20))");
    db.setConnection(counting(db.getConnection()));
  }

  @AfterEach
  void tearDown() {
    db.disconnect();
  }

  /** A connection that counts the statements it hands out and the ones that get closed. */
  private Connection counting(Connection connection) {
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              Object result;
              try {
                result = method.invoke(connection, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (result instanceof Statement statement) {
                opened.incrementAndGet();
                return countingClose(statement);
              }
              return result;
            });
  }

  private Object countingClose(Statement statement) {
    Class<?>[] interfaces = statement.getClass().getInterfaces();
    return Proxy.newProxyInstance(
        getClass().getClassLoader(),
        interfaces.length == 0 ? new Class<?>[] {Statement.class} : interfaces,
        (proxy, method, args) -> {
          if (method.getName().equals("close") && !statement.isClosed()) {
            closed.incrementAndGet();
          }
          try {
            return method.invoke(statement, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  void failingChecksCloseTheirStatements() throws Exception {
    for (int i = 0; i < 100; i++) {
      assertFalse(db.checkTableExists(null, "MISSING"));
      assertFalse(db.checkColumnExists(null, "CUSTOMERS", "MISSING"));
    }

    assertEquals(200, opened.get(), "one statement per check");
    assertEquals(opened.get(), closed.get(), "every statement is closed");
  }

  @Test
  void succeedingChecksCloseTheirStatements() throws Exception {
    for (int i = 0; i < 100; i++) {
      assertTrue(db.checkTableExists(null, "CUSTOMERS"));
      assertTrue(db.checkColumnExists(null, "CUSTOMERS", "NAME"));
    }

    assertEquals(200, opened.get(), "one statement per check");
    assertEquals(opened.get(), closed.get(), "every statement is closed");
  }
}
