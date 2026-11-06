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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.actions.pgpdecryptfiles.ActionPGPDecryptFiles;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Conflict options that have to run GnuPG, with a stub that copies the source onto {@code
 * --output}. See <a href="https://github.com/apache/hop/issues/9207">issue-9207</a>.
 */
class ActionPGPFilesConflictTest {

  private static Path root;
  private static Path stub;

  @BeforeAll
  static void initRuntime() throws Exception {
    HopLogStore.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
    root = Files.createTempDirectory("hop-pgp-conflict");
    stub = GpgCopyStub.install(root.resolve("stub"));
  }

  @AfterAll
  static void removeRoot() throws Exception {
    if (root == null || !Files.exists(root)) {
      return;
    }
    try (var paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
    }
  }

  @Test
  void uniqueNameKeepsTheExistingFileAndWritesANewOne() throws Exception {
    Path source = file("unique/plain.txt", "PLAIN");
    Path destination = file("unique/out/plain.txt", "OLD");
    ActionPGPEncryptFiles action = encrypt("unique_name", source, destination);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten());
    assertEquals("OLD", Files.readString(destination));
    Path created;
    try (Stream<Path> listed = Files.list(destination.getParent())) {
      created =
          listed
              .filter(path -> !path.getFileName().toString().equals("plain.txt"))
              .findFirst()
              .orElseThrow();
    }
    assertEquals("PLAIN", Files.readString(created));
    assertEquals(1, result.getResultFiles().size());
    assertTrue(
        result
            .getResultFiles()
            .keySet()
            .iterator()
            .next()
            .contains(created.getFileName().toString()));
  }

  @Test
  void deleteSourceEncryptsIntoTheDestinationThenRemovesTheSource() throws Exception {
    Path source = file("delete/plain.txt", "PLAIN");
    Path destination = file("delete/out/plain.txt", "OLD");
    ActionPGPEncryptFiles action = encrypt("delete_file", source, destination);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten());
    assertFalse(Files.exists(source));
    assertEquals("PLAIN", Files.readString(destination));
    assertEquals(1, result.getResultFiles().size());
    assertTrue(result.getResultFiles().keySet().iterator().next().contains("plain.txt"));
  }

  @Test
  void moveSourceEncryptsFirstAndThenMovesThePlaintext() throws Exception {
    Path source = file("move/plain.txt", "PLAIN");
    Path destination = file("move/out/plain.txt", "OLD");
    Path moved = root.resolve("move/moved");
    Files.createDirectories(moved);
    ActionPGPEncryptFiles action = encrypt("move_file", source, destination);
    action.setDestinationFolder(moved.toString());
    action.setCreateMoveToFolder(true);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten());
    assertFalse(Files.exists(source));
    assertEquals("PLAIN", Files.readString(destination));
    assertEquals("PLAIN", Files.readString(moved.resolve("plain.txt")));
    assertEquals(1, result.getResultFiles().size());
    String registered = result.getResultFiles().keySet().iterator().next();
    assertTrue(registered.contains("out"), registered);
    assertFalse(registered.contains("moved"), registered);
  }

  @Test
  void doNothingOnANewFileAddsThatFileToTheResult() throws Exception {
    Path source = file("nothing/plain.txt", "PLAIN");
    Path destination = root.resolve("nothing/out/plain.txt");
    Files.createDirectories(destination.getParent());
    ActionPGPEncryptFiles action = encrypt("do_nothing", source, destination);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten());
    assertEquals("PLAIN", Files.readString(destination));
    assertEquals(1, result.getResultFiles().size());
  }

  @Test
  void decryptDeleteSourceWritesTheDestinationThenRemovesTheSource() throws Exception {
    Path source = file("d-delete/sealed.gpg", "SEALED");
    Path destination = file("d-delete/out/sealed.gpg", "OLD");
    Result result = decrypt(source, destination);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten());
    assertFalse(Files.exists(source));
    assertEquals("SEALED", Files.readString(destination));
    assertEquals(1, result.getResultFiles().size());
  }

  @Test
  void decryptMoveSourceWritesTheDestinationThenMovesTheSource() throws Exception {
    Path source = file("d-move/sealed.gpg", "SEALED");
    Path destination = file("d-move/out/sealed.gpg", "OLD");
    Path moved = root.resolve("d-move/moved");
    Files.createDirectories(moved);
    ActionPGPDecryptFiles action =
        decryptAction(ActionPGPDecryptFiles.CONST_MOVE_FILE, source, destination);
    action.setDestinationFolder(moved.toString());
    action.setCreateMoveToFolder(true);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten());
    assertEquals("SEALED", Files.readString(destination));
    assertEquals("SEALED", Files.readString(moved.resolve("sealed.gpg")));
    String registered = result.getResultFiles().keySet().iterator().next();
    assertTrue(registered.contains("out"), registered);
    assertFalse(registered.contains("moved"), registered);
  }

  @Test
  void signSendsThePassphraseOnStdin() throws Exception {
    Path source = file("sign/plain.txt", "PLAIN");
    Path destination = root.resolve("sign/plain.txt.asc");
    Files.createDirectories(destination.getParent());
    ActionPGPEncryptFiles action = new ActionPGPEncryptFiles("sign");
    action.setGpgLocation(stub.toString());
    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(true);
    action.setIfFileExists("overwrite_file");
    attach(action);
    ActionPGPEncryptFiles.PgpFile file = new ActionPGPEncryptFiles.PgpFile();
    file.setActionType(ActionPGPEncryptFiles.ActionType.SIGN);
    file.setLocalUser("signer@example.org");
    file.setPassphrase("secret");
    file.setSourceFileFolder(source.toString());
    file.setDestinationFileFolder(destination.toString());
    action.getPgpFiles().add(file);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("PLAIN", Files.readString(destination));
    assertEquals("secret", GpgCopyStub.standardInput(stub.getParent()));
    List<String> args = GpgCopyStub.arguments(stub.getParent());
    assertTrue(args.contains("--passphrase-fd"), args.toString());
    assertTrue(args.contains("loopback"), args.toString());
    assertFalse(args.contains("secret"), args.toString());
  }

  @Test
  void aPreviousResultRowCarriesThePassphraseInTheSeventhField() throws Exception {
    Path source = file("prev/plain.txt", "PLAIN");
    Path destination = root.resolve("prev/sealed.asc");
    Files.createDirectories(destination.getParent());
    ActionPGPEncryptFiles action = new ActionPGPEncryptFiles("previous");
    action.setGpgLocation(stub.toString());
    action.setArgFromPrevious(true);
    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(true);
    action.setIfFileExists("overwrite_file");
    attach(action);

    RowMeta rowMeta = new RowMeta();
    for (int i = 0; i < 7; i++) {
      rowMeta.addValueMeta(new ValueMetaString("c" + i));
    }
    Result incoming = new Result();
    incoming
        .getRows()
        .add(
            new RowMetaAndData(
                rowMeta,
                new Object[] {
                  "signandencrypt",
                  source.toString(),
                  "",
                  "recipient@example.org",
                  destination.toString(),
                  "signer@example.org",
                  "secret"
                }));

    Result result = action.execute(incoming, 0);

    assertTrue(result.isResult(), "errors=" + result.getNrErrors());
    assertEquals("secret", GpgCopyStub.standardInput(stub.getParent()));
    List<String> args = GpgCopyStub.arguments(stub.getParent());
    assertTrue(args.contains("--passphrase-fd"), args.toString());
    assertTrue(args.contains("recipient@example.org"), args.toString());
    assertTrue(args.contains("signer@example.org"), args.toString());
  }

  private ActionPGPEncryptFiles encrypt(String ifFileExists, Path source, Path destination) {
    ActionPGPEncryptFiles action = new ActionPGPEncryptFiles("encrypt");
    action.setGpgLocation(stub.toString());
    action.setAddResultFileNames(true);
    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(true);
    action.setIfFileExists(ifFileExists);
    attach(action);
    ActionPGPEncryptFiles.PgpFile file = new ActionPGPEncryptFiles.PgpFile();
    file.setActionType(ActionPGPEncryptFiles.ActionType.ENCRYPT);
    file.setUserId("user@example.org");
    file.setSourceFileFolder(source.toString());
    file.setDestinationFileFolder(destination.toString());
    action.getPgpFiles().add(file);
    return action;
  }

  private Result decrypt(Path source, Path destination) throws Exception {
    return decryptAction(ActionPGPDecryptFiles.CONST_DELETE_FILE, source, destination)
        .execute(new Result(), 0);
  }

  private ActionPGPDecryptFiles decryptAction(String ifFileExists, Path source, Path destination) {
    ActionPGPDecryptFiles action = new ActionPGPDecryptFiles("decrypt");
    action.setGpgLocation(stub.toString());
    action.setAddResultFilenames(true);
    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(true);
    action.setIfFileExists(ifFileExists);
    attach(action);
    ActionPGPDecryptFiles.FileToDecrypt file = new ActionPGPDecryptFiles.FileToDecrypt();
    file.setSourceFileFolder(source.toString());
    file.setDestinationFileFolder(destination.toString());
    action.getFilesToDecrypt().add(file);
    return action;
  }

  private static Path file(String relative, String content) throws Exception {
    Path path = root.resolve(relative);
    Files.createDirectories(path.getParent());
    Files.writeString(path, content);
    return path;
  }

  private static void attach(ActionBase action) {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("pgp-test");
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(meta);
    action.setParentWorkflow(workflow);
  }
}
