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

package org.apache.hop.databases.derby;

import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.batch.DialectContractCheck;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Derby through its network client, the driver Hop uses when a hostname is set: the server runs in
 * process, the connection uses the URL the dialect builds, and the driver must behave as the
 * dialect declares for it.
 */
class DerbyNetworkBatchContractTest {

  private static NetworkServerControl server;
  private static int port;

  @BeforeAll
  static void startServer() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DerbyDatabaseMeta.class);
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    server = new NetworkServerControl(InetAddress.getLoopbackAddress(), port);
    server.start(new PrintWriter(Writer.nullWriter()));
    for (int i = 0; i < 50; i++) {
      try {
        server.ping();
        return;
      } catch (Exception e) {
        Thread.sleep(100);
      }
    }
    server.ping();
  }

  @AfterAll
  static void stopServer() throws Exception {
    if (server != null) {
      server.shutdown();
    }
  }

  @Test
  void theNetworkClientBehavesAsTheDialectDeclares() throws Exception {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            "derby",
            "DERBY",
            "Native",
            "localhost",
            "memory:netcontract;create=true",
            Integer.toString(port),
            "app",
            "app");
    String url = databaseMeta.getURL(new Variables());
    try (Connection connection = DriverManager.getConnection(url, "app", "app")) {
      DialectContractCheck.declaredContractHolds(
          connection, databaseMeta.getIDatabase().getBatchFailureContract(), "NET");
    }
  }
}
