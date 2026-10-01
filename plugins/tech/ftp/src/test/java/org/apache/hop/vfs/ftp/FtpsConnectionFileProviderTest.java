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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A named FTPS connection registered as a VFS scheme. The twin of {@link
 * FtpConnectionFileProviderTest}, over TLS: the same URI has to work, and it has to go through the
 * FTPS provider rather than the plain one.
 */
class FtpsConnectionFileProviderTest {

  private static final String CONNECTION_NAME = "secure";
  private static final String PAYLOAD = "behind-a-named-ftps-connection";

  private final IVariables variables = new Variables();

  @BeforeAll
  static void initEncryption() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  @DisplayName("Reading and writing over an explicit FTPS connection")
  void readsAndWritesOverExplicitFtps(@TempDir Path root) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, FtpSecurityMode.FTPS_EXPLICIT)) {
      server.writeFile("greeting.txt", PAYLOAD);

      try (DefaultFileSystemManager manager =
          managerFor(server, FtpSecurityMode.FTPS_EXPLICIT, false)) {
        FileObject file = manager.resolveFile(CONNECTION_NAME + ":///greeting.txt");
        assertTrue(file.exists());
        try (InputStream in = file.getContent().getInputStream()) {
          assertEquals(PAYLOAD, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }

        FileObject written = manager.resolveFile(CONNECTION_NAME + ":///written.txt");
        try (OutputStream out = written.getContent().getOutputStream()) {
          out.write("over-tls".getBytes(StandardCharsets.UTF_8));
        }
        written.close();
        assertEquals("over-tls", server.readFile("written.txt"));
      }
    }
  }

  @Test
  @DisplayName("An implicit FTPS connection reaches its server too")
  void readsOverImplicitFtps(@TempDir Path root) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, FtpSecurityMode.FTPS_IMPLICIT)) {
      server.writeFile("greeting.txt", PAYLOAD);

      try (DefaultFileSystemManager manager =
          managerFor(server, FtpSecurityMode.FTPS_IMPLICIT, false)) {
        try (InputStream in =
            manager
                .resolveFile(CONNECTION_NAME + ":///greeting.txt")
                .getContent()
                .getInputStream()) {
          assertEquals(PAYLOAD, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
      }
    }
  }

  @Test
  @DisplayName("The URI of a secured connection hides the server just the same")
  void theUriKeepsTheConnectionName(@TempDir Path root) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, FtpSecurityMode.FTPS_EXPLICIT);
        DefaultFileSystemManager manager =
            managerFor(server, FtpSecurityMode.FTPS_EXPLICIT, false)) {
      String uri = manager.resolveFile(CONNECTION_NAME + ":///a/b.txt").getName().getURI();

      assertEquals(CONNECTION_NAME + ":///a/b.txt", uri);
      assertFalse(uri.contains(FtpTestServer.PASSWORD));
    }
  }

  @Test
  @DisplayName("Certificate verification applies to the VFS path as well, not only to the actions")
  void verificationAppliesToTheProviderToo(@TempDir Path root) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, FtpSecurityMode.FTPS_EXPLICIT);
        DefaultFileSystemManager manager =
            managerFor(server, FtpSecurityMode.FTPS_EXPLICIT, true)) {
      server.writeFile("greeting.txt", PAYLOAD);

      assertThrows(
          FileSystemException.class,
          () -> manager.resolveFile(CONNECTION_NAME + ":///greeting.txt").exists(),
          "the self signed certificate of the test server must be refused");
    }
  }

  private DefaultFileSystemManager managerFor(
      FtpTestServer server, FtpSecurityMode mode, boolean verifyCertificate) throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setName(CONNECTION_NAME);
    connection.setSecurityMode(mode);
    connection.setServerName(FtpTestServer.HOST);
    connection.setServerPort(Integer.toString(server.getPort()));
    connection.setUserName(FtpTestServer.USER);
    connection.setPassword(FtpTestServer.PASSWORD);
    connection.setVerifyServerCertificate(verifyCertificate);

    DefaultFileSystemManager manager = new DefaultFileSystemManager();
    manager.addProvider(CONNECTION_NAME, new FtpsConnectionFileProvider(variables, connection));
    manager.init();
    return manager;
  }
}
