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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A named connection registered as a VFS scheme: {@code prod:///greeting.txt} has to reach the
 * server the connection points at, with the credentials of that connection, without any of that
 * showing up in the URI.
 */
class FtpConnectionFileProviderTest {

  private static final String CONNECTION_NAME = "prod";
  private static final String PAYLOAD = "behind-a-named-connection";

  @TempDir private static Path serverRoot;

  private static FtpTestServer server;

  private final IVariables variables = new Variables();

  @BeforeAll
  static void startServer() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");

    server = FtpTestServer.start(serverRoot, FtpSecurityMode.FTP);
    server.writeFile("greeting.txt", PAYLOAD);
  }

  @AfterAll
  static void stopServer() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  @Test
  @DisplayName("A file behind the connection is read over FTP")
  void readsAFileBehindTheConnection() throws Exception {
    try (DefaultFileSystemManager manager = managerWithConnection()) {
      FileObject file = manager.resolveFile(CONNECTION_NAME + ":///greeting.txt");

      assertTrue(file.exists());
      try (InputStream in = file.getContent().getInputStream()) {
        assertEquals(PAYLOAD, new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  @DisplayName("The URI stays the one that was typed: no server, no credentials")
  void theUriKeepsTheConnectionName() throws Exception {
    try (DefaultFileSystemManager manager = managerWithConnection()) {
      FileObject file = manager.resolveFile(CONNECTION_NAME + ":///greeting.txt");

      String uri = file.getName().getURI();
      assertEquals(CONNECTION_NAME + ":///greeting.txt", uri);
      assertFalse(uri.contains(FtpTestServer.PASSWORD), "the password must not be in the URI");
      assertFalse(uri.contains(FtpTestServer.HOST), "the server must not be in the URI");
    }
  }

  @Test
  @DisplayName("Writing through the connection puts the bytes on the server")
  void writesAFileBehindTheConnection() throws Exception {
    try (DefaultFileSystemManager manager = managerWithConnection()) {
      FileObject file = manager.resolveFile(CONNECTION_NAME + ":///written.txt");
      try (OutputStream out = file.getContent().getOutputStream()) {
        out.write("written-over-ftp".getBytes(StandardCharsets.UTF_8));
      }
      file.close();

      assertEquals("written-over-ftp", server.readFile("written.txt"));
    }
  }

  @Test
  @DisplayName("A file which isn't there does not exist, rather than failing to resolve")
  void aMissingFileSimplyDoesNotExist() throws Exception {
    try (DefaultFileSystemManager manager = managerWithConnection()) {
      assertFalse(manager.resolveFile(CONNECTION_NAME + ":///nope.txt").exists());
    }
  }

  private DefaultFileSystemManager managerWithConnection() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setName(CONNECTION_NAME);
    connection.setServerName(FtpTestServer.HOST);
    connection.setServerPort(Integer.toString(server.getPort()));
    connection.setUserName(FtpTestServer.USER);
    connection.setPassword(FtpTestServer.PASSWORD);

    DefaultFileSystemManager manager = new DefaultFileSystemManager();
    manager.addProvider(CONNECTION_NAME, new FtpConnectionFileProvider(variables, connection));
    manager.init();
    return manager;
  }
}
