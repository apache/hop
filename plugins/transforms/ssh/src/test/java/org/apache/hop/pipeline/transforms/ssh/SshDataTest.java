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

package org.apache.hop.pipeline.transforms.ssh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jcraft.jsch.Session;
import java.io.IOException;
import java.nio.file.Path;
import java.security.PublicKey;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.sshd.server.SshServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link SshData} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class SshDataTest extends SshTestSupport {
  @TempDir static java.nio.file.Path tempDir;
  private static SshServer server;
  private static int sshPort;
  private static java.nio.file.Path privateKeyPath;

  @BeforeAll
  static void beforeAll() throws Exception {
    PublicKey authorizedPublicKey = null;
    if (isSshKeygenAvailable()) {
      String keyName = "ssh_data";
      privateKeyPath = generateRsaKeyPair(tempDir, keyName);
      authorizedPublicKey = loadPublicKeyFromPubFile(Path.of(privateKeyPath + ".pub"));
    }
    server = startPasswordSshServer(authorizedPublicKey);
    sshPort = server.getPort();
  }

  @Test
  void openConnectionWithPasswordAuthenticates() throws Exception {
    SshMeta meta = SshTestSupport.passwordMeta(sshPort);

    Session session = SshData.openConnection(SshTestSupport.localVariables(), meta);

    assertNotNull(session);
    assertTrue(session.isConnected());
    session.disconnect();
  }

  @Test
  void openConnectionWithPrivateKeyAuthenticates() throws Exception {
    Assumptions.assumeTrue(
        privateKeyPath != null, "ssh-keygen is required for private key authentication tests");
    SshMeta meta =
        SshTestSupport.privateKeyMeta(sshPort, privateKeyPath.toAbsolutePath().toString(), null);
    Session session = SshData.openConnection(SshTestSupport.localVariables(), meta);

    assertNotNull(session);
    assertTrue(session.isConnected());
    session.disconnect();
  }

  @Test
  void openConnectionMissingPrivateKeyFileThrows() {
    SshMeta meta = SshTestSupport.passwordMeta(sshPort);
    meta.setUsePrivateKey(true);
    meta.setKeyFileName(null);

    HopException ex =
        assertThrows(
            HopException.class,
            () -> SshData.openConnection(SshTestSupport.localVariables(), meta));

    assertTrue(ex.getMessage().contains("PrivateKeyFileMissing") || !ex.getMessage().isEmpty());
  }

  @Test
  void openConnectionNonExistentPrivateKeyThrows() {
    SshMeta meta = SshTestSupport.passwordMeta(sshPort);
    meta.setUsePrivateKey(true);
    meta.setKeyFileName(tempDir.resolve("missing-key.pem").toUri().toString());

    assertThrows(
        HopException.class, () -> SshData.openConnection(SshTestSupport.localVariables(), meta));
  }

  @Test
  void openConnectionWrongPasswordThrows() {
    SshMeta meta = SshTestSupport.passwordMeta(sshPort);
    meta.setPassword("definitely-wrong-password");
    meta.setTimeOut("3");

    assertThrows(
        HopException.class, () -> SshData.openConnection(SshTestSupport.localVariables(), meta));
  }

  @Test
  void executeCommandAgainstEmbeddedServerReturnsOutput() throws Exception {
    SshMeta meta = SshTestSupport.passwordMeta(sshPort);
    String token = "hop-data-test";
    Session session = SshData.openConnection(SshTestSupport.localVariables(), meta);
    try {
      SessionResult result =
          SessionResult.executeCommand(session, SshTestSupport.echoCommand(token));
      assertTrue(result.getStd().contains(token), () -> "output was: " + result.getStd());
    } finally {
      session.disconnect();
    }
  }

  @Test
  void newDataInitialState() {
    SshData data = new SshData();
    assertEquals(-1, data.indexOfCommand);
    assertFalse(data.wroteOneRow);
  }

  @AfterAll
  static void stop() throws IOException {
    if (server != null) {
      stopSharedServer(server);
    }
  }
}
