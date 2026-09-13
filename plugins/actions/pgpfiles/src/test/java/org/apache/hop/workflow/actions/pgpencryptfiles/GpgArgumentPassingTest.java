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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Asserts what {@link GPG} actually hands to the GnuPG process, by standing a recorder script in
 * for the binary and reading back the argument vector it was given.
 *
 * <p>Observing the arguments rather than whether an operation succeeded is what makes these tests
 * meaningful in both directions: they describe a property ("the filename arrives as one literal
 * argument", "the passphrase is never on the command line") that can be checked against any
 * implementation, and they cover the methods whose real GnuPG operation cannot easily be made to
 * succeed in a unit test. Run them against the implementation that built a shell command string and
 * they fail; see https://github.com/apache/hop/issues/8311.
 *
 * <p>POSIX only: the recorder is a shell script. Argument passing on Windows is not covered here.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class GpgArgumentPassingTest {

  /**
   * Names that a shell would rewrite. Every one is a legal POSIX filename, and every one is what an
   * attacker who can write into a scanned folder would choose.
   */
  private static final List<String> HOSTILE_NAMES =
      List.of(
          "report$(echo pwned).csv",
          "report`echo pwned`.csv",
          "report$HOME.csv",
          "report;echo pwned;.csv",
          "report\";echo pwned;\".csv",
          "report'; echo pwned; '.csv",
          "report file with spaces.csv",
          "report&&echo pwned.csv",
          "report|echo pwned.csv",
          "report*.csv");

  private static final String PASSPHRASE = "s3cr3t-do-not-leak";

  /** Created by the payloads below if a shell ever evaluates them, and by nothing else. */
  private static final String EXPLOIT_MARKER = "hop-pgp-exploit-marker";

  private Path sandbox;
  private Path recorder;
  private Path record;
  private ILogChannel log;
  private IVariables variables;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @BeforeEach
  void createRecorder() throws Exception {
    sandbox = Files.createTempDirectory("hop-gpg-argv");
    record = sandbox.resolve("argv.txt");
    recorder = sandbox.resolve("gpg-recorder.sh");

    // Writes one argument per line and succeeds, so the caller carries on as if GnuPG had run.
    Files.writeString(
        recorder,
        "#!/bin/sh\n"
            + "{ for a in \"$@\"; do printf '%s\\n' \"$a\"; done; } > '"
            + record
            + "'\n"
            + "exit 0\n",
        StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(recorder, PosixFilePermissions.fromString("rwx------"));

    log = new LogChannel("GpgArgumentPassingTest");
    variables = new Variables();
  }

  @AfterEach
  void removeSandbox() throws Exception {
    // If a payload ever did run, do not leave its marker behind for the next test to trip over.
    Files.deleteIfExists(Path.of(System.getProperty("user.dir")).resolve(EXPLOIT_MARKER));
    deleteRecursively(sandbox);
  }

  @Test
  void signFilePassesFilenamesLiterally() throws Exception {
    for (String name : HOSTILE_NAMES) {
      gpg().signFile(name, "", "signed-" + name, true);
      assertPassedLiterally(name, "signFile source");
      assertPassedLiterally("signed-" + name, "signFile destination");
    }
  }

  @Test
  void encryptFilePassesFilenamesLiterally() throws Exception {
    for (String name : HOSTILE_NAMES) {
      gpg().encryptFile(name, "user@example.org", "encrypted-" + name, false);
      assertPassedLiterally(name, "encryptFile source");
      assertPassedLiterally("encrypted-" + name, "encryptFile destination");
    }
  }

  @Test
  void signAndEncryptFilePassesFilenamesLiterally() throws Exception {
    for (String name : HOSTILE_NAMES) {
      gpg().signAndEncryptFile(name, "user@example.org", "sealed-" + name, true);
      assertPassedLiterally(name, "signAndEncryptFile source");
      assertPassedLiterally("sealed-" + name, "signAndEncryptFile destination");
    }
  }

  @Test
  void decryptFilePassesFilenamesLiterally() throws Exception {
    for (String name : HOSTILE_NAMES) {
      gpg().decryptFile(name, "", "opened-" + name);
      assertPassedLiterally(name, "decryptFile source");
      assertPassedLiterally("opened-" + name, "decryptFile destination");
    }
  }

  @Test
  void verifySignaturePassesFilenamesLiterally() throws Exception {
    for (String name : HOSTILE_NAMES) {
      gpg().verifySignature(name);
      assertPassedLiterally(name, "verifySignature filename");
    }
  }

  @Test
  void verifyDetachedSignaturePassesFilenamesLiterally() throws Exception {
    for (String name : HOSTILE_NAMES) {
      gpg().verifyDetachedSignature(name, "original-" + name);
      assertPassedLiterally(name, "verifyDetachedSignature signature");
      assertPassedLiterally("original-" + name, "verifyDetachedSignature original");
    }
  }

  /** Used by the PGP encrypt stream transform in plugins/transforms/pgp. */
  @Test
  void encryptStringPassesTheKeyIdLiterally() throws Exception {
    for (String keyId : HOSTILE_NAMES) {
      gpg().encrypt("some data", keyId);
      assertPassedLiterally(keyId, "encrypt key id");
    }
  }

  /** Used by the PGP decrypt stream transform in plugins/transforms/pgp. */
  @Test
  void decryptStringKeepsThePassphraseOffTheCommandLine() throws Exception {
    gpg().decrypt("some data", PASSPHRASE);
    assertPassphraseAbsent("decrypt");
  }

  @Test
  void signStringKeepsThePassphraseOffTheCommandLine() throws Exception {
    gpg().sign("some data", PASSPHRASE);
    assertPassphraseAbsent("sign");
  }

  @Test
  void signAndEncryptStringKeepsThePassphraseOffTheCommandLine() throws Exception {
    gpg().signAndEncrypt("some data", "user@example.org", PASSPHRASE);
    assertPassphraseAbsent("signAndEncrypt");
  }

  /**
   * The passphrase must travel over stdin. On the command line it is readable by every other user
   * on the machine for as long as GnuPG runs.
   */
  @Test
  void decryptFileKeepsThePassphraseOffTheCommandLine() throws Exception {
    gpg().decryptFile("sealed.asc", PASSPHRASE, "opened.txt");
    assertPassphraseAbsent("decryptFile");
    assertTrue(
        recordedArguments().contains("--passphrase-fd"),
        "decryptFile must ask GnuPG to read the passphrase from a file descriptor");
  }

  @Test
  void anEmptyUserIdOmitsTheRecipientFlag() throws Exception {
    gpg().signFile("plain.txt", "", "plain.txt.asc", true);
    assertFalse(
        recordedArguments().contains("-r"),
        "an empty user id must not be passed to GnuPG as an empty recipient");

    gpg().signFile("plain.txt", "user@example.org", "plain.txt.asc", true);
    List<String> args = recordedArguments();
    assertTrue(args.contains("-r"), "a user id must be passed as a recipient");
    assertEquals(
        "user@example.org",
        args.get(args.indexOf("-r") + 1),
        "the recipient must follow -r as its own argument");
  }

  /**
   * The one that matters: proves the defect was executable, not merely untidy.
   *
   * <p>A file name cannot contain a path separator, so the payload cannot name an absolute path. It
   * does not need to: a command substitution runs with the working directory the Hop process
   * happens to have, and creating a file there is evidence enough that the shell ran it. Against
   * the implementation that built a command string this marker appears; the file being signed does
   * not even have to exist, because the substitution happens before GnuPG is reached at all.
   */
  @Test
  void aCommandSubstitutionInAFilenameIsNeverExecuted() throws Exception {
    Path marker = Path.of(System.getProperty("user.dir")).resolve(EXPLOIT_MARKER);
    Files.deleteIfExists(marker);

    // Reads as one file name, and every method below is handed it as such.
    String payload = "report$(touch " + EXPLOIT_MARKER + ").csv";

    gpg().verifySignature(payload);
    assertFalse(Files.exists(marker), "verifySignature executed a command from a filename");

    gpg().signFile(payload, "", "signed.asc", true);
    assertFalse(Files.exists(marker), "signFile executed a command from a filename");

    gpg().encryptFile(payload, "user@example.org", "sealed.asc", false);
    assertFalse(Files.exists(marker), "encryptFile executed a command from a filename");

    gpg().decryptFile(payload, "", "opened.csv");
    assertFalse(Files.exists(marker), "decryptFile executed a command from a filename");

    gpg().verifyDetachedSignature(payload, payload + ".sig");
    assertFalse(Files.exists(marker), "verifyDetachedSignature executed a command from a filename");

    // And the name still arrived intact rather than being dropped on the floor.
    assertPassedLiterally(payload, "the payload");
  }

  /**
   * The same proof for the key id, which reaches GnuPG through the string based methods that the
   * PGP stream transforms call.
   */
  @Test
  void aCommandSubstitutionInAKeyIdIsNeverExecuted() throws Exception {
    Path marker = Path.of(System.getProperty("user.dir")).resolve(EXPLOIT_MARKER);
    Files.deleteIfExists(marker);

    String payload = "user$(touch " + EXPLOIT_MARKER + ")@example.org";

    gpg().encrypt("some data", payload);
    assertFalse(Files.exists(marker), "encrypt executed a command from a key id");

    gpg().signAndEncrypt("some data", payload, PASSPHRASE);
    assertFalse(Files.exists(marker), "signAndEncrypt executed a command from a key id");

    assertPassedLiterally(payload, "the payload");
  }

  private GPG gpg() throws Exception {
    return new GPG(recorder.toString(), log, variables);
  }

  /**
   * The value has to appear in the recorded vector exactly once and as a whole element. A shell
   * would have rewritten it, split it across elements, or dropped it.
   */
  private void assertPassedLiterally(String value, String what) throws IOException {
    List<String> args = recordedArguments();
    assertTrue(
        args.contains(value),
        what
            + " must reach GnuPG as one literal argument.\nexpected to find: "
            + value
            + "\nactual arguments: "
            + args);
  }

  private void assertPassphraseAbsent(String what) throws IOException {
    List<String> args = recordedArguments();
    assertTrue(
        args.stream().noneMatch(a -> a.contains(PASSPHRASE)),
        what + " must not put the passphrase on the command line.\nactual arguments: " + args);
  }

  private List<String> recordedArguments() throws IOException {
    assertTrue(Files.exists(record), "GnuPG was never invoked");
    return Files.readAllLines(record, StandardCharsets.UTF_8);
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
