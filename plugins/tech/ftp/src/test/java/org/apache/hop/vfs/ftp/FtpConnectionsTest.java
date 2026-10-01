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

package org.apache.hop.vfs.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Looking connections up, and turning them into the URIs the rest of Hop resolves. */
class FtpConnectionsTest {

  @Test
  @DisplayName("A connection in the metadata is found by name")
  void loadsByName() throws Exception {
    IHopMetadataProvider provider = new MemoryMetadataProvider();
    FtpConnection stored = new FtpConnection();
    stored.setName("prod");
    stored.setServerName("ftp.example.com");
    provider.getSerializer(FtpConnection.class).save(stored);

    assertEquals("ftp.example.com", FtpConnections.load(provider, "prod").getServerName());
  }

  @Test
  @DisplayName("A name with whitespace around it still finds the connection")
  void trimsTheName() throws Exception {
    IHopMetadataProvider provider = new MemoryMetadataProvider();
    FtpConnection stored = new FtpConnection();
    stored.setName("prod");
    provider.getSerializer(FtpConnection.class).save(stored);

    assertEquals("prod", FtpConnections.load(provider, "  prod  ").getName());
  }

  @Test
  @DisplayName("An empty name is an error, not a null connection handed to the caller")
  void emptyNameThrows() {
    IHopMetadataProvider provider = new MemoryMetadataProvider();

    assertThrows(HopException.class, () -> FtpConnections.load(provider, ""));
    assertThrows(HopException.class, () -> FtpConnections.load(provider, null));
  }

  @Test
  @DisplayName("A name which isn't in the metadata is an error naming it")
  void unknownNameThrows() {
    IHopMetadataProvider provider = new MemoryMetadataProvider();

    HopException e =
        assertThrows(HopException.class, () -> FtpConnections.load(provider, "nowhere"));
    assertTrue(e.getMessage().contains("nowhere"));
  }

  @Test
  @DisplayName("The URI of a file behind a connection is the name of the connection plus a path")
  void buildsUris() {
    FtpConnection connection = new FtpConnection();
    connection.setName("prod");

    assertEquals("prod://", FtpConnections.getBaseUri(connection));
    assertEquals("prod://inbox/x.csv", FtpConnections.buildUri(connection, "inbox", "x.csv"));
    assertEquals("prod://inbox/x.csv", FtpConnections.buildUri(connection, "/inbox/", "x.csv"));
    assertEquals("prod://inbox/x.csv", FtpConnections.buildUri(connection, "\\inbox", "x.csv"));
    assertEquals("prod://x.csv", FtpConnections.buildUri(connection, null, "x.csv"));
    assertEquals("prod://inbox", FtpConnections.buildUri(connection, "inbox", null));
    assertEquals("prod://", FtpConnections.buildUri(connection, null, null));
  }

  @Test
  @DisplayName("An unset security mode reads as plain FTP rather than as null")
  void securityModeIsNeverNull() {
    FtpConnection connection = new FtpConnection();
    connection.setSecurityMode(null);

    assertSame(FtpSecurityMode.FTP, connection.getSecurityMode());
    assertEquals("ftp", connection.getSecurityMode().getScheme());
    assertEquals("ftps", FtpSecurityMode.FTPS_EXPLICIT.getScheme());
  }
}
