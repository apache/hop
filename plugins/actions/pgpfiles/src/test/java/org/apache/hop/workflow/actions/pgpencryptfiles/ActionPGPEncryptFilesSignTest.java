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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.Result;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.pgpdecryptfiles.ActionPGPDecryptFiles;
import org.apache.hop.workflow.actions.pgpverify.ActionPGPVerify;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Drives the PGP actions against a real gpg binary and a throwaway keyring, so "signing works" is
 * asserted end to end rather than inferred from the command string we build. Raised by
 * https://github.com/apache/hop/issues/8206, where a user reported signing had been dropped.
 *
 * <p>Skipped when gpg is not installed. gpg is pointed at its own keyring through a wrapper script
 * rather than GNUPGHOME, because {@link GPG} runs the binary with the JVM's environment and a Java
 * process cannot change its own.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class ActionPGPEncryptFilesSignTest {

  private static final String KEY_USER_ID = "hop-pgp-test@example.org";
  private static final String PLAIN_TEXT = "Apache Hop signs this file.\n";

  private static Path gpgBinary;

  private Path sandbox;
  private Path gnupgHome;
  private Path work;
  private Path gpgWrapper;

  @BeforeAll
  static void findGpg() {
    HopLogStore.init();
    gpgBinary = locateGpg();
    assumeTrue(gpgBinary != null, "gpg is not on the PATH; skipping the PGP end-to-end tests");
  }

  @BeforeEach
  void createThrowawayKeyring() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");

    sandbox = Files.createTempDirectory(shortTempRoot(), "hop-pgp");
    gnupgHome = Files.createDirectory(sandbox.resolve("gnupg"));
    Files.setPosixFilePermissions(gnupgHome, PosixFilePermissions.fromString("rwx------"));
    work = Files.createDirectory(sandbox.resolve("work"));

    gpgWrapper = sandbox.resolve("gpg-in-sandbox.sh");
    Files.writeString(
        gpgWrapper,
        "#!/bin/sh\nexec '" + gpgBinary + "' --homedir '" + gnupgHome + "' \"$@\"\n",
        StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(gpgWrapper, PosixFilePermissions.fromString("rwx------"));

    // A passphrase-less key: the encrypt action has no passphrase field, so signing can only ever
    // use a key gpg can unlock on its own.
    run(
        gpgBinary.toString(),
        "--homedir",
        gnupgHome.toString(),
        "--batch",
        "--yes",
        "--pinentry-mode",
        "loopback",
        "--passphrase",
        "",
        "--quick-generate-key",
        "Hop PGP Test <" + KEY_USER_ID + ">",
        "default",
        "default",
        "never");
  }

  @AfterEach
  void removeThrowawayKeyring() throws Exception {
    if (gnupgHome != null && Files.exists(gnupgHome)) {
      // gpg 2 starts an agent per home directory; leave none behind holding the folder open.
      run("gpgconf", "--homedir", gnupgHome.toString(), "--kill", "all");
    }
    deleteRecursively(sandbox);
  }

  @Test
  void signedFileIsAcceptedByTheVerifyAction() throws Exception {
    Path source = Files.writeString(work.resolve("invoices.csv"), PLAIN_TEXT);
    Path signed = work.resolve("invoices.csv.asc");

    ActionPGPEncryptFiles sign = encryptAction();
    sign.setAsciiMode(true);
    sign.getPgpFiles()
        .add(
            pgpFile(
                ActionPGPEncryptFiles.ActionType.SIGN,
                source,
                signed,
                // The key to sign with cannot be chosen: the User ID goes to gpg as -r, which
                // --clearsign ignores. See https://github.com/apache/hop/issues/8206.
                ""));

    Result result = sign.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors(), "signing must not report errors");
    assertTrue(result.getResult(), "signing must succeed");
    assertTrue(Files.exists(signed), "the signed file must be written");
    assertTrue(
        Files.readString(signed).startsWith("-----BEGIN PGP SIGNED MESSAGE-----"),
        "the output must be a clear-signed message");

    ActionPGPVerify verify = new ActionPGPVerify();
    attachToWorkflow(verify);
    verify.setGpgLocation(gpgWrapper.toString());
    verify.setFilename(signed.toString());

    Result verified = verify.execute(new Result(), 0);

    assertEquals(0, verified.getNrErrors(), "verifying the signature must not report errors");
    assertTrue(verified.getResult(), "the signature must verify against the signing key");
  }

  @Test
  void signedAndEncryptedFileSurvivesTheDecryptAction() throws Exception {
    Path source = Files.writeString(work.resolve("payments.xml"), PLAIN_TEXT);
    Path sealed = work.resolve("payments.xml.asc");
    Path opened = work.resolve("payments-opened.xml");

    ActionPGPEncryptFiles seal = encryptAction();
    seal.setAsciiMode(true);
    seal.getPgpFiles()
        .add(
            pgpFile(
                ActionPGPEncryptFiles.ActionType.SIGN_AND_ENCRYPT, source, sealed, KEY_USER_ID));

    Result sealedResult = seal.execute(new Result(), 0);

    assertEquals(0, sealedResult.getNrErrors(), "sign and encrypt must not report errors");
    assertTrue(sealedResult.getResult(), "sign and encrypt must succeed");
    assertTrue(
        Files.readString(sealed).startsWith("-----BEGIN PGP MESSAGE-----"),
        "the output must be an encrypted message");

    ActionPGPDecryptFiles decrypt = new ActionPGPDecryptFiles();
    attachToWorkflow(decrypt);
    decrypt.setGpgLocation(gpgWrapper.toString());
    decrypt.setDestinationIsAFile(true);
    ActionPGPDecryptFiles.FileToDecrypt toDecrypt = new ActionPGPDecryptFiles.FileToDecrypt();
    toDecrypt.setSourceFileFolder(sealed.toString());
    toDecrypt.setDestinationFileFolder(opened.toString());
    toDecrypt.setPassphrase("");
    decrypt.setFilesToDecrypt(List.of(toDecrypt));

    Result openedResult = decrypt.execute(new Result(), 0);

    assertEquals(0, openedResult.getNrErrors(), "decrypting must not report errors");
    assertTrue(openedResult.getResult(), "decrypting must succeed");
    assertEquals(PLAIN_TEXT, Files.readString(opened), "the round trip must preserve the content");
  }

  private ActionPGPEncryptFiles encryptAction() {
    ActionPGPEncryptFiles action = new ActionPGPEncryptFiles("PGP encrypt files");
    attachToWorkflow(action);
    action.setGpgLocation(gpgWrapper.toString());
    action.setDestinationIsAFile(true);
    return action;
  }

  private static ActionPGPEncryptFiles.PgpFile pgpFile(
      ActionPGPEncryptFiles.ActionType actionType, Path source, Path destination, String userId) {
    ActionPGPEncryptFiles.PgpFile file = new ActionPGPEncryptFiles.PgpFile();
    file.setActionType(actionType);
    file.setSourceFileFolder(source.toString());
    file.setDestinationFileFolder(destination.toString());
    file.setUserId(userId);
    return file;
  }

  private static void attachToWorkflow(ActionBase action) {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    workflow.setStopped(false);
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
  }

  /**
   * gpg-agent listens on a unix socket inside its home directory, and those paths are capped near
   * 100 characters. macOS hands out temp folders long enough to blow that on their own, so keep the
   * keyring under /tmp where one is available.
   */
  private static Path shortTempRoot() {
    Path tmp = Path.of("/tmp");
    if (Files.isDirectory(tmp) && Files.isWritable(tmp)) {
      return tmp;
    }
    return Path.of(System.getProperty("java.io.tmpdir"));
  }

  private static Path locateGpg() {
    String path = System.getenv("PATH");
    if (path == null) {
      return null;
    }
    for (String folder : path.split(java.io.File.pathSeparator)) {
      Path candidate = Path.of(folder, "gpg");
      if (Files.isExecutable(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  private static void run(String... command) throws Exception {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertTrue(process.waitFor(60, TimeUnit.SECONDS), "timed out: " + String.join(" ", command));
    assertEquals(0, process.exitValue(), String.join(" ", command) + " failed:\n" + output);
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (root == null || !Files.exists(root)) {
      return;
    }
    try (var paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }
}
