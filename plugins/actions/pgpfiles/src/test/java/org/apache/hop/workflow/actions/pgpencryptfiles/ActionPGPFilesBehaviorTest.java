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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.hop.core.Result;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.actions.pgpdecryptfiles.ActionPGPDecryptFiles;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The conflict options and a missing GnuPG program, exercised without invoking GnuPG. See <a
 * href="https://github.com/apache/hop/issues/9207">issue-9207</a>.
 */
class ActionPGPFilesBehaviorTest {

  private Path sandbox;

  @BeforeAll
  static void initRuntime() throws Exception {
    HopLogStore.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @BeforeEach
  void createSandbox() throws Exception {
    sandbox = Files.createTempDirectory("hop-pgp-behavior");
  }

  @AfterEach
  void removeSandbox() throws Exception {
    if (sandbox == null || !Files.exists(sandbox)) {
      return;
    }
    try (var paths = Files.walk(sandbox)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
    }
  }

  @Test
  void missingGpgMakesEncryptFalseWithoutThrowing() {
    ActionPGPEncryptFiles action = encryptAction(sandbox.resolve("missing-gpg").toString());
    Result result = assertDoesNotThrow(() -> action.execute(new Result(), 0));

    assert result != null;
    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void missingGpgMakesDecryptFalseWithoutThrowing() {
    ActionPGPDecryptFiles action = decryptAction(sandbox.resolve("missing-gpg").toString());
    Result result = assertDoesNotThrow(() -> action.execute(new Result(), 0));

    assert result != null;
    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void oneFolderFailureIsNotCountedTwiceForEncrypt() throws Exception {
    Path source = sandbox.resolve("in");
    Path destination = sandbox.resolve("out");
    Files.createDirectories(source);
    Files.createDirectories(destination);
    Files.writeString(source.resolve("a.txt"), "A");
    Files.writeString(source.resolve("b.txt"), "B");
    Files.writeString(destination.resolve("a.txt"), "KEEP-A");
    Files.writeString(destination.resolve("b.txt"), "KEEP-B");

    ActionPGPEncryptFiles action = encryptAction(sandbox.resolve("gpg-unused").toString());
    Files.writeString(sandbox.resolve("gpg-unused"), "not gpg");
    action.setIfFileExists("fail");
    action.setSuccessCondition(ActionPGPEncryptFiles.SUCCESS_IF_ERRORS_LESS);
    action.setNrErrorsLessThan("1");
    action.getPgpFiles().add(encryptFile(source, destination));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "one error still satisfies nrErrors <= 1");
    assertEquals(1, result.getNrErrors());
    assertEquals("KEEP-A", Files.readString(destination.resolve("a.txt")));
    assertEquals("KEEP-B", Files.readString(destination.resolve("b.txt")));
  }

  @Test
  void oneFolderFailureIsNotCountedTwiceForDecrypt() throws Exception {
    Path source = sandbox.resolve("in");
    Path destination = sandbox.resolve("out");
    Files.createDirectories(source);
    Files.createDirectories(destination);
    Files.writeString(source.resolve("a.txt"), "A");
    Files.writeString(source.resolve("b.txt"), "B");
    Files.writeString(destination.resolve("a.txt"), "KEEP-A");
    Files.writeString(destination.resolve("b.txt"), "KEEP-B");

    ActionPGPDecryptFiles action = decryptAction(sandbox.resolve("gpg-unused").toString());
    Files.writeString(sandbox.resolve("gpg-unused"), "not gpg");
    action.setIfFileExists(ActionPGPDecryptFiles.CONST_FAIL);
    action.setSuccessCondition(ActionPGPDecryptFiles.SUCCESS_IF_ERRORS_LESS);
    action.setNrErrorsLessThan("1");
    ActionPGPDecryptFiles.FileToDecrypt file = new ActionPGPDecryptFiles.FileToDecrypt();
    file.setSourceFileFolder(source.toString());
    file.setDestinationFileFolder(destination.toString());
    action.getFilesToDecrypt().add(file);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "one error still satisfies nrErrors <= 1");
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void doNothingLeavesAnExistingDestinationAndAddsNoResultFile() throws Exception {
    Path source = sandbox.resolve("plain.txt");
    Path destination = sandbox.resolve("out").resolve("plain.txt");
    Files.writeString(source, "PLAIN");
    Files.createDirectories(destination.getParent());
    Files.writeString(destination, "OLD");
    Path gpg = sandbox.resolve("gpg-unused");
    Files.writeString(gpg, "not gpg");

    ActionPGPEncryptFiles action = encryptAction(gpg.toString());
    action.setAddResultFileNames(true);
    action.setDestinationIsAFile(true);
    action.setIfFileExists("do_nothing");
    action.getPgpFiles().add(encryptFile(source, destination));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(0, result.getNrLinesWritten());
    assertTrue(result.getResultFiles().isEmpty());
    assertEquals("OLD", Files.readString(destination));
    assertEquals("PLAIN", Files.readString(source));
  }

  private ActionPGPEncryptFiles encryptAction(String gpgLocation) {
    ActionPGPEncryptFiles action = new ActionPGPEncryptFiles("encrypt");
    action.setGpgLocation(gpgLocation);
    attach(action);
    return action;
  }

  private ActionPGPDecryptFiles decryptAction(String gpgLocation) {
    ActionPGPDecryptFiles action = new ActionPGPDecryptFiles("decrypt");
    action.setGpgLocation(gpgLocation);
    attach(action);
    return action;
  }

  private static ActionPGPEncryptFiles.PgpFile encryptFile(Path source, Path destination) {
    ActionPGPEncryptFiles.PgpFile file = new ActionPGPEncryptFiles.PgpFile();
    file.setActionType(ActionPGPEncryptFiles.ActionType.ENCRYPT);
    file.setUserId("user@example.org");
    file.setSourceFileFolder(source.toString());
    file.setDestinationFileFolder(destination.toString());
    return file;
  }

  private static void attach(ActionBase action) {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("pgp-test");
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(meta);
    action.setParentWorkflow(workflow);
  }
}
