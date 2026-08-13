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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code ftp://} scheme is served by this plugin (see {@link FtpVfsPlugin}), not by Hop core,
 * so this is where it gets proved end to end: a URL resolved through {@link HopVfs} has to reach a
 * real FTP server. It moved here from {@code HopVfsNetworkProvidersTest} in core when the provider
 * did.
 *
 * <p>Losing the registration is not a loud failure - VFS falls back to reading an unknown scheme as
 * a relative local path - so the test also asserts the resolved file is genuinely remote.
 */
class HopVfsFtpSchemeTest {

  // Registers the plugins, which is what puts the ftp provider on the file system manager.
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String PAYLOAD = "ftp-payload";

  @TempDir private static Path serverRoot;

  private static FtpTestServer server;

  @BeforeAll
  static void startServer() throws Exception {
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
  @DisplayName("ftp:// is registered and fetches a payload from an embedded Apache FtpServer")
  void ftpSchemeReadsFromEmbeddedServer() throws Exception {
    String url =
        "ftp://"
            + FtpTestServer.USER
            + ":"
            + FtpTestServer.PASSWORD
            + "@"
            + FtpTestServer.HOST
            + ":"
            + server.getPort()
            + "/greeting.txt";

    // Passive mode: an embedded server on the loopback address can't open a data connection back.
    FileSystemOptions options = new FileSystemOptions();
    FtpFileSystemConfigBuilder.getInstance().setPassiveMode(options, true);

    FileObject fileObject = HopVfs.getFileSystemManager().resolveFile(url, options);

    // A missing ftp provider doesn't throw: VFS would read "ftp:/..." as a local relative path.
    assertEquals(
        "ftp",
        fileObject.getName().getScheme(),
        "ftp:// resolved to something else, so the provider is not registered");
    assertTrue(fileObject.exists());

    try (InputStream inputStream = fileObject.getContent().getInputStream()) {
      assertEquals(PAYLOAD, new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("ftps:// is registered by the FTPS VFS plugin")
  void ftpsSchemeIsRegistered() {
    assertTrue(
        HopVfs.getFileSystemManager().hasProvider("ftps"),
        "the ftps provider must be registered by FtpsVfsPlugin");
  }
}
