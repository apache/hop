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
package org.apache.hop.workflow.actions.sftpput;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.junit.vfs.CrossDeviceFileProvider;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.sftpput.ActionSftpPut.AfterFtpAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.core.CoreModuleProperties;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.AcceptAllPasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Issue <a href="https://github.com/apache/hop/issues/5936">#5936</a>: "move the file after the
 * upload" only worked when the source folder and the destination folder happened to sit on the same
 * file system of the operating system. On a machine where the working folder and the archive folder
 * are two different mounts the action ended with
 *
 * <pre>Could not rename "file:///files/work/x.csv" to "file:///files/arc/x.csv"</pre>
 *
 * <p>{@link FileObject#moveTo(FileObject)} only falls back to a copy followed by a delete when the
 * two files belong to two different VFS file systems, and two local folders are one and the same
 * VFS file system whichever disk they're on - so it always picks a rename, which is exactly what
 * the operating system refuses across a mount point. Hence {@link HopVfs#moveFile(FileObject,
 * FileObject)}, which the action goes through.
 *
 * <p>The tests upload to an embedded Apache MINA SSHD server, so they exercise the real action from
 * end to end.
 */
class ActionSftpPutMoveTest {

  /** Registers the plugins and initializes the engine, which the action needs to run. */
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String LOCALHOST = "127.0.0.1";
  private static final String USER = "alice";
  private static final String PASSWORD = "secret";
  private static final String FILE_NAME = "sales.csv";
  private static final String PAYLOAD = "id;amount\n1;42\n";

  /**
   * A scheme served by {@link CrossDeviceFileProvider}: one VFS file system, on which a rename
   * never succeeds. That is the situation of two local folders on two different mounts.
   */
  private static final String CROSS_DEVICE = "xdev";

  private static final String WORK_FOLDER = CROSS_DEVICE + ":///work";
  private static final String ARCHIVE_FOLDER = CROSS_DEVICE + ":///archive";

  /** Points at a folder on a different file system than the temporary folder, when there is one. */
  private static final String CROSS_DEVICE_DIR_PROPERTY = "hop.test.cross.device.dir";

  @TempDir private Path serverRoot;
  @TempDir private Path localRoot;

  private SshServer sshServer;

  @BeforeEach
  void startServer() throws IOException {
    sshServer = SshServer.setUpDefaultServer();
    sshServer.setHost(LOCALHOST);
    sshServer.setPort(0);
    sshServer.setKeyPairProvider(
        new SimpleGeneratorHostKeyProvider(serverRoot.resolve("hostkey.ser")));
    sshServer.setPasswordAuthenticator(AcceptAllPasswordAuthenticator.INSTANCE);
    sshServer.setFileSystemFactory(new VirtualFileSystemFactory(serverRoot));
    sshServer.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
    // The client keeps its session open, and the server's 10 minute default idle timeout would
    // hold the test open that long.
    CoreModuleProperties.IDLE_TIMEOUT.set(sshServer, Duration.ofSeconds(5));
    sshServer.start();

    HopVfs.getFileSystemManager().addProvider(CROSS_DEVICE, new CrossDeviceFileProvider());
  }

  @AfterEach
  void stopServer() throws IOException {
    if (sshServer != null) {
      sshServer.stop();
    }
    // The provider above is registered on the one and only file system manager.
    HopVfs.reset();
  }

  /**
   * The reproduction of the report: the file is uploaded and then has to be moved to a folder on
   * another file system, which is where the action used to give up.
   */
  @Test
  @DisplayName("#5936: the uploaded file is moved to a folder on another file system")
  void movesTheUploadedFileToAnotherFileSystem() throws Exception {
    writeFile(WORK_FOLDER + "/" + FILE_NAME);
    HopVfs.getFileObject(ARCHIVE_FOLDER).createFolder();

    ActionSftpPut action = action(WORK_FOLDER, ARCHIVE_FOLDER);
    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors(), () -> "the action reported an error:\n" + log(action));
    assertTrue(Files.exists(serverRoot.resolve(FILE_NAME)), "the file never reached the server");
    assertTrue(
        HopVfs.getFileObject(ARCHIVE_FOLDER + "/" + FILE_NAME).exists(),
        () -> "the uploaded file was not moved to the destination folder:\n" + log(action));
    assertFalse(
        HopVfs.getFileObject(WORK_FOLDER + "/" + FILE_NAME).exists(),
        "the file is still in the source folder");
  }

  /**
   * The same run with both folders on one file system, where the rename does work. It tells apart
   * "the move is broken" from "the move across a mount point is broken".
   */
  @Test
  @DisplayName("the uploaded file is moved to a folder on the same file system")
  void movesTheUploadedFileWithinOneFileSystem() throws Exception {
    Path work = Files.createDirectory(localRoot.resolve("work"));
    Path archive = Files.createDirectory(localRoot.resolve("archive"));
    Files.writeString(work.resolve(FILE_NAME), PAYLOAD);

    ActionSftpPut action = action(work.toString(), archive.toString());
    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors(), () -> "the action reported an error:\n" + log(action));
    assertTrue(Files.exists(serverRoot.resolve(FILE_NAME)), "the file never reached the server");
    assertTrue(Files.exists(archive.resolve(FILE_NAME)), "the uploaded file was not moved");
    assertFalse(Files.exists(work.resolve(FILE_NAME)), "the file is still in the source folder");
  }

  /**
   * The reproduction above once more, but on two genuinely different mounts instead of a file
   * system which refuses to rename. It needs a writable folder on a second file store: {@code
   * /dev/shm} on Linux, or any folder handed in through the {@code hop.test.cross.device.dir}
   * system property (a mounted RAM disk, a network share, a second volume). Without one there is
   * nothing to prove here and the test is skipped.
   */
  @Test
  @DisplayName("#5936: the uploaded file is moved to a folder on a second real mount")
  void movesTheUploadedFileToASecondMount() throws Exception {
    Path secondMount = secondFileStore();
    assumeTrue(
        secondMount != null,
        "no writable folder on a second file store; set -D" + CROSS_DEVICE_DIR_PROPERTY);

    Path work = Files.createTempDirectory(secondMount, "hop-5936-work");
    Path archive = Files.createDirectory(localRoot.resolve("archive"));
    Files.writeString(work.resolve(FILE_NAME), PAYLOAD);

    try {
      ActionSftpPut action = action(work.toString(), archive.toString());
      Result result = action.execute(new Result(), 0);

      assertEquals(0, result.getNrErrors(), () -> "the action reported an error:\n" + log(action));
      assertTrue(Files.exists(serverRoot.resolve(FILE_NAME)), "the file never reached the server");
      assertTrue(Files.exists(archive.resolve(FILE_NAME)), "the uploaded file was not moved");
      assertFalse(Files.exists(work.resolve(FILE_NAME)), "the file is still in the source folder");
    } finally {
      Files.deleteIfExists(work.resolve(FILE_NAME));
      Files.deleteIfExists(work);
    }
  }

  /**
   * Why the action can't leave the move to VFS: two folders of one file system always take the
   * rename branch of {@link FileObject#moveTo(FileObject)}, and a failing rename is the end of it -
   * the copy plus delete branch is only for two different VFS file systems. Two local folders are
   * one VFS file system whichever disks they are on, so the fallback is never reached, however far
   * apart the two folders are.
   */
  @Test
  @DisplayName("moveTo() renames within one file system and does not fall back to a copy")
  void moveToWithinOneFileSystemNeverFallsBackToACopy() throws Exception {
    FileObject source = writeFile(WORK_FOLDER + "/" + FILE_NAME);
    FileObject destination = HopVfs.getFileObject(ARCHIVE_FOLDER + "/" + FILE_NAME);

    assertTrue(
        source.canRenameTo(destination),
        "the two folders are one VFS file system, so moveTo() renames");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> source.moveTo(destination));
    assertTrue(e.getMessage().contains("Could not rename"), e.getMessage());
    assertFalse(destination.exists(), "moveTo() did not fall back to a copy");

    // Which is all HopVfs.moveFile() has to do to get the file across.
    destination.copyFrom(source, Selectors.SELECT_SELF);
    source.delete();

    assertTrue(destination.exists());
    assertFalse(source.exists());
  }

  /** Two local folders are one VFS file system, whichever disk each of them is on. */
  @Test
  @DisplayName("two local folders are one VFS file system")
  void twoLocalFoldersAreOneVfsFileSystem() throws Exception {
    FileObject here = HopVfs.getFileObject(localRoot.toString());
    Path secondMount = secondFileStore();
    Path elsewhere = secondMount != null ? secondMount : Paths.get(System.getProperty("user.dir"));

    assertEquals(
        here.getFileSystem(),
        HopVfs.getFileObject(elsewhere.toString()).getFileSystem(),
        "VFS sees one file system, so moveTo() will rename between these two folders");
  }

  private ActionSftpPut action(String localDirectory, String destinationFolder) {
    ActionSftpPut action = new ActionSftpPut("SFTP Put");
    action.setServerName(LOCALHOST);
    action.setServerPort(String.valueOf(sshServer.getPort()));
    action.setUserName(USER);
    action.setPassword(PASSWORD);
    action.setLocalDirectory(localDirectory);
    action.setAfterSftpAction(AfterFtpAction.MOVE);
    action.setDestinationFolder(destinationFolder);
    action.setCreateDestinationFolder(true);

    IWorkflowEngine<WorkflowMeta> parentWorkflow = mock(Workflow.class);
    doReturn(false).when(parentWorkflow).isStopped();
    doReturn(LogLevel.BASIC).when(parentWorkflow).getLogLevel();
    doReturn("unit test").when(parentWorkflow).getWorkflowName();
    action.setParentWorkflow(parentWorkflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    return action;
  }

  private static FileObject writeFile(String uri) throws Exception {
    FileObject fileObject = HopVfs.getFileObject(uri);
    try (OutputStream outputStream = fileObject.getContent().getOutputStream()) {
      outputStream.write(PAYLOAD.getBytes(UTF_8));
    }
    return fileObject;
  }

  /** What the action logged, so a failing assertion says why the action gave up. */
  private static String log(ActionSftpPut action) {
    return HopLogStore.getAppender().getBuffer(action.getLogChannelId(), false).toString();
  }

  /**
   * A writable folder on a different file store than the temporary folder, or null when this
   * machine has none to offer.
   */
  private Path secondFileStore() throws IOException {
    FileStore local = Files.getFileStore(localRoot);
    for (String candidate :
        new String[] {System.getProperty(CROSS_DEVICE_DIR_PROPERTY), "/dev/shm"}) {
      if (candidate == null) {
        continue;
      }
      Path path = Paths.get(candidate);
      if (Files.isDirectory(path)
          && Files.isWritable(path)
          && !local.equals(Files.getFileStore(path))) {
        return path;
      }
    }
    return null;
  }
}
