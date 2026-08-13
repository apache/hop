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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.sshd.common.config.keys.AuthorizedKeyEntry;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;

/** Shared fixtures for SSH transform unit tests. */
@Slf4j
class SshTestSupport {
  static final String SSH_USER = "root";
  static final String SSH_PASS = "pwd123456";

  /** Passphrase used when generating test keys; must match {@link #privateKeyMeta}. */
  static final String TEST_KEY_PASSPHRASE = "hello";

  /**
   * Lightweight pool for embedded command handlers. SSHD expects {@link Command#start} not to block
   * the I/O thread for the whole command lifetime.
   */
  private static final ExecutorService COMMAND_EXECUTOR =
      Executors.newCachedThreadPool(
          new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, "ssh-test-command-" + seq.incrementAndGet());
              t.setDaemon(true);
              return t;
            }
          });

  static SshServer startPasswordSshServer(PublicKey authorizedPublicKey) throws IOException {
    SshServer sshd = SshServer.setUpDefaultServer();
    sshd.setPort(0);
    // Keep the embedded server lean for constrained CI runners.
    sshd.setNioWorkers(2);
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
    sshd.setPasswordAuthenticator(
        (username, password, session) -> SSH_USER.equals(username) && SSH_PASS.equals(password));
    sshd.setPublickeyAuthenticator(
        (username, key, session) ->
            SSH_USER.equals(username)
                && authorizedPublicKey != null
                && KeyUtils.compareKeys(authorizedPublicKey, key));
    sshd.setCommandFactory((channel, command) -> new TestEchoCommand(command));
    sshd.start();
    return sshd;
  }

  static void stopSharedServer(SshServer server) throws IOException {
    if (server != null && server.isOpen()) {
      server.stop();
    }
  }

  static SshMeta passwordMeta(int port) {
    SshMeta meta = new SshMeta();
    meta.setUsePrivateKey(false);
    meta.setServerName("localhost");
    meta.setPort(String.valueOf(port));
    meta.setUserName(SSH_USER);
    meta.setPassword(SSH_PASS);
    meta.setCommand("echo hop-ssh-test");
    meta.setStdOutFieldName("stdOut");
    meta.setStdErrFieldName("stdErr");
    return meta;
  }

  static SshMeta privateKeyMeta(int port, String privateKeyPath) {
    return privateKeyMeta(port, privateKeyPath, TEST_KEY_PASSPHRASE);
  }

  static SshMeta privateKeyMeta(int port, String privateKeyPath, String keyPassphrase) {
    SshMeta meta = passwordMeta(port);
    meta.setUsePrivateKey(true);
    meta.setPassword(null);
    meta.setKeyFileName(privateKeyPath);
    meta.setPassPhrase(keyPassphrase);
    return meta;
  }

  static PublicKey loadPublicKeyFromPubFile(Path pubFile) throws Exception {
    AuthorizedKeyEntry entry =
        AuthorizedKeyEntry.parseAuthorizedKeyEntry(
            Files.readString(pubFile, StandardCharsets.UTF_8));
    return entry.resolvePublicKey(null, null);
  }

  static boolean isSshKeygenAvailable() {
    try {
      boolean windows = System.getProperty("os.name", "").toLowerCase().contains("win");
      ProcessBuilder builder = new ProcessBuilder(windows ? "where" : "which", "ssh-keygen");
      Process process = builder.redirectErrorStream(true).start();
      return process.waitFor() == 0;
    } catch (Exception e) {
      return false;
    }
  }

  static Path generateRsaKeyPair(Path directory, String keyName)
      throws IOException, InterruptedException {
    Files.createDirectories(directory);
    Path privateKey = directory.resolve(keyName);
    Process process =
        new ProcessBuilder(
                "ssh-keygen",
                "-t",
                "rsa",
                "-b",
                "2048",
                "-m",
                "PEM",
                "-N",
                "",
                "-f",
                privateKey.toAbsolutePath().toString(),
                "-q")
            .redirectErrorStream(true)
            .start();
    int exit = process.waitFor();
    if (exit != 0 || !Files.exists(privateKey)) {
      throw new IOException("ssh-keygen failed with exit code " + exit);
    }
    return privateKey;
  }

  static IVariables localVariables() {
    return new Variables();
  }

  /** Command understood by the embedded test SSH server. */
  static String echoCommand(String token) {
    return "echo " + token;
  }

  /** Minimal exec handler so tests do not depend on OS shell behavior. */
  private static final class TestEchoCommand implements Command {
    private final String command;
    private OutputStream stdout;
    private OutputStream stderr;
    private ExitCallback exitCallback;

    private TestEchoCommand(String command) {
      this.command = command;
    }

    @Override
    public void setInputStream(InputStream in) {
      // no stdin required
    }

    @Override
    public void setOutputStream(OutputStream out) {
      this.stdout = out;
    }

    @Override
    public void setErrorStream(OutputStream err) {
      this.stderr = err;
    }

    @Override
    public void setExitCallback(ExitCallback callback) {
      this.exitCallback = callback;
    }

    @Override
    public void start(ChannelSession channel, Environment env) {
      // Run off the SSHD I/O thread. Synchronous start+onExit can close the channel before the
      // client has attached readers, which shows up as empty stdout under CI load.
      COMMAND_EXECUTOR.execute(this::runCommand);
    }

    private void runCommand() {
      try {
        if (command != null && command.startsWith("echo ")) {
          writeAndFlush(stdout, command.substring(5) + "\n");
          exitCallback.onExit(0);
        } else if (command != null && command.startsWith("stderr ")) {
          writeAndFlush(stderr, command.substring(7) + "\n");
          exitCallback.onExit(0);
        } else {
          writeAndFlush(stderr, "unsupported command: " + command + "\n");
          exitCallback.onExit(1);
        }
      } catch (IOException e) {
        log.warn("Embedded SSH test command failed: {}", command, e);
        if (exitCallback != null) {
          exitCallback.onExit(1, e.getMessage());
        }
      }
      // Do not close stdout/stderr here: SSHD owns channel stream lifecycle after onExit.
    }

    private static void writeAndFlush(OutputStream stream, String payload) throws IOException {
      if (stream == null) {
        return;
      }
      stream.write(payload.getBytes(StandardCharsets.UTF_8));
      stream.flush();
    }

    @Override
    public void destroy(ChannelSession channel) {
      // nothing to clean up
    }
  }
}
