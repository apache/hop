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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Executor;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies the login/connection ({@link Const#HOP_DATABASE_CONNECTION_TIMEOUT}) and network/socket
 * ({@link Const#HOP_DATABASE_SOCKET_TIMEOUT}) timeout defaults are resolved and applied correctly.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class DatabaseTimeoutTest {

  private int originalLoginTimeout;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void rememberLoginTimeout() {
    // DriverManager.setLoginTimeout is a JVM-wide setting; snapshot it so tests don't leak state.
    originalLoginTimeout = DriverManager.getLoginTimeout();
  }

  @AfterEach
  void restoreLoginTimeout() {
    DriverManager.setLoginTimeout(originalLoginTimeout);
  }

  private Database databaseWith(String variableName, String value) {
    Variables variables = new Variables();
    if (value != null) {
      variables.setVariable(variableName, value);
    }
    DatabaseMeta meta = new DatabaseMeta("test", "None", "", "", "", "", "", "");
    return new Database(null, variables, meta);
  }

  // ---------------------------------------------------------------------------
  // Login / connection timeout -> DriverManager.setLoginTimeout(seconds)
  // ---------------------------------------------------------------------------

  @Test
  void loginTimeout_defaultsTo30SecondsWhenUnset() {
    DriverManager.setLoginTimeout(0);
    Database db = databaseWith(Const.HOP_DATABASE_CONNECTION_TIMEOUT, null);

    int resolved = db.applyLoginTimeout();

    assertEquals(30, resolved);
    assertEquals(30, DriverManager.getLoginTimeout());
  }

  @Test
  void loginTimeout_appliesConfiguredValue() {
    DriverManager.setLoginTimeout(0);
    Database db = databaseWith(Const.HOP_DATABASE_CONNECTION_TIMEOUT, "45");

    int resolved = db.applyLoginTimeout();

    assertEquals(45, resolved);
    assertEquals(45, DriverManager.getLoginTimeout());
  }

  @Test
  void loginTimeout_zeroLeavesDriverManagerUntouched() {
    DriverManager.setLoginTimeout(7);
    Database db = databaseWith(Const.HOP_DATABASE_CONNECTION_TIMEOUT, "0");

    int resolved = db.applyLoginTimeout();

    // 0 means "impose no timeout" - the existing JVM default must be left in place.
    assertEquals(0, resolved);
    assertEquals(7, DriverManager.getLoginTimeout());
  }

  // ---------------------------------------------------------------------------
  // Network / socket timeout -> Connection.setNetworkTimeout(executor, millis)
  // ---------------------------------------------------------------------------

  @Test
  void socketTimeout_isDisabledByDefault() throws Exception {
    Connection connection = mock(Connection.class);
    Database db = databaseWith(Const.HOP_DATABASE_SOCKET_TIMEOUT, null);
    db.setConnection(connection);

    db.applyNetworkTimeout();

    verify(connection, never()).setNetworkTimeout(any(), anyInt());
  }

  @Test
  void socketTimeout_zeroIsDisabled() throws Exception {
    Connection connection = mock(Connection.class);
    Database db = databaseWith(Const.HOP_DATABASE_SOCKET_TIMEOUT, "0");
    db.setConnection(connection);

    db.applyNetworkTimeout();

    verify(connection, never()).setNetworkTimeout(any(), anyInt());
  }

  @Test
  void socketTimeout_appliesConfiguredValueConvertedToMillis() throws Exception {
    Connection connection = mock(Connection.class);
    Database db = databaseWith(Const.HOP_DATABASE_SOCKET_TIMEOUT, "30");
    db.setConnection(connection);

    db.applyNetworkTimeout();

    // seconds -> milliseconds
    verify(connection, times(1)).setNetworkTimeout(any(Executor.class), eq(30000));
  }

  @Test
  void socketTimeout_nullConnectionIsNoop() {
    Database db = databaseWith(Const.HOP_DATABASE_SOCKET_TIMEOUT, "30");
    // No setConnection() call - the connection stays null.

    assertDoesNotThrow(db::applyNetworkTimeout);
  }

  @Test
  void socketTimeout_unsupportedDriverIsIgnored() throws Exception {
    Connection connection = mock(Connection.class);
    doThrow(new SQLFeatureNotSupportedException("not supported"))
        .when(connection)
        .setNetworkTimeout(any(), anyInt());
    Database db = databaseWith(Const.HOP_DATABASE_SOCKET_TIMEOUT, "30");
    db.setConnection(connection);

    // An optional timeout must never fail the connection.
    assertDoesNotThrow(db::applyNetworkTimeout);
  }
}
