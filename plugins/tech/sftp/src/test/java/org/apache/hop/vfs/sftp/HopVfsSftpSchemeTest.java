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

package org.apache.hop.vfs.sftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.core.CoreModuleProperties;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.AcceptAllPasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code sftp://} scheme is served by this plugin (see {@link SftpVfsPlugin}), not by Hop core,
 * so this is where it gets proved end to end: a URL resolved through {@link HopVfs} has to reach a
 * real SSH server. It moved here from {@code HopVfsNetworkProvidersTest} in core when the provider
 * did.
 *
 * <p>Losing the registration is not a loud failure - VFS falls back to reading an unknown scheme as
 * a relative local path - so the test also asserts the resolved file is genuinely remote.
 */
class HopVfsSftpSchemeTest {

  // Registers the plugins, which is what puts the sftp provider on the file system manager.
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String LOCALHOST = "127.0.0.1";
  private static final String USER = "alice";
  private static final String PASSWORD = "secret";
  private static final String PAYLOAD = "sftp-payload";

  @TempDir private static Path serverRoot;

  private static SshServer sshServer;

  @BeforeAll
  static void startServer() throws IOException {
    Files.writeString(serverRoot.resolve("greeting.txt"), PAYLOAD);

    sshServer = SshServer.setUpDefaultServer();
    sshServer.setHost(LOCALHOST);
    sshServer.setPort(0);
    sshServer.setKeyPairProvider(
        new SimpleGeneratorHostKeyProvider(serverRoot.resolve("hostkey.ser")));
    sshServer.setPasswordAuthenticator(AcceptAllPasswordAuthenticator.INSTANCE);
    sshServer.setFileSystemFactory(new VirtualFileSystemFactory(serverRoot));
    sshServer.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
    // The SFTP client doesn't close the session when the FileObject is closed, and the server's
    // 10 minute default idle timeout would hold the test open that long.
    CoreModuleProperties.IDLE_TIMEOUT.set(sshServer, Duration.ofSeconds(5));
    sshServer.start();
  }

  @AfterAll
  static void stopServer() throws IOException {
    if (sshServer != null) {
      sshServer.stop();
    }
  }

  @Test
  @DisplayName("sftp:// fetches a payload from an embedded Apache MINA SSHD server")
  void sftpSchemeReadsFromEmbeddedServer() throws Exception {
    FileObject fileObject = HopVfs.getFileObject(url("greeting.txt"));

    // A missing sftp provider doesn't throw: VFS would read "sftp:/..." as a local relative path.
    assertEquals(
        "sftp",
        fileObject.getName().getScheme(),
        "sftp:// resolved to something else, so the provider is not registered");
    assertTrue(fileObject.exists());

    try (InputStream inputStream = fileObject.getContent().getInputStream()) {
      assertEquals(PAYLOAD, new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private static String url(String path) {
    return "sftp://"
        + USER
        + ":"
        + PASSWORD
        + "@"
        + LOCALHOST
        + ":"
        + sshServer.getPort()
        + "/"
        + path;
  }
}
