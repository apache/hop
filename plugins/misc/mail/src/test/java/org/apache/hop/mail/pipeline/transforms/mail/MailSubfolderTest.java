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

package org.apache.hop.mail.pipeline.transforms.mail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Attachments are collected with a VFS file selector. The "include subfolders" option has to
 * control folder traversal as well, otherwise a sub-folder the user has no access to breaks a mail
 * that was never meant to attach anything from it. See https://github.com/apache/hop/issues/7952
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class MailSubfolderTest {

  @TempDir Path folder;

  private TransformMockHelper<MailMeta, MailData> mockHelper;
  private MailMeta meta;
  private Mail transform;
  private Path unreadableFolder;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper = new TransformMockHelper<>("Mail", MailMeta.class, MailData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    meta = mock(MailMeta.class);
    transform =
        new Mail(
            mockHelper.transformMeta,
            meta,
            new MailData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (unreadableFolder != null) {
      Files.setPosixFilePermissions(unreadableFolder, PosixFilePermissions.fromString("rwx------"));
    }
    mockHelper.cleanUp();
  }

  @Test
  void unreadableSubfolderIsNotTraversedWhenSubfoldersAreExcluded() throws Exception {
    Files.createFile(folder.resolve("report.csv"));
    unreadableFolder = createUnreadableFolder();
    when(meta.isIncludeSubFolders()).thenReturn(false);

    List<String> found = findAttachments(".*\\.csv");

    assertEquals(
        List.of("report.csv"), found, "an excluded sub-folder must neither fail nor contribute");
  }

  @Test
  void subfoldersAreTraversedWhenIncluded() throws Exception {
    Files.createFile(folder.resolve("report.csv"));
    Path subFolder = Files.createDirectory(folder.resolve("archive"));
    Files.createFile(subFolder.resolve("nested.csv"));

    when(meta.isIncludeSubFolders()).thenReturn(false);
    assertEquals(List.of("report.csv"), findAttachments(".*\\.csv"), "the nested file is skipped");

    when(meta.isIncludeSubFolders()).thenReturn(true);
    assertEquals(
        List.of("nested.csv", "report.csv"),
        findAttachments(".*\\.csv"),
        "the nested file is attached too");
  }

  /** Runs the transform's own attachment selector over the temp folder. */
  private List<String> findAttachments(String wildcard) throws Exception {
    try (FileObject sourceFolder = HopVfs.getFileObject(folder.toString())) {
      FileObject[] list =
          sourceFolder.findFiles(transform.new TextFileSelector(sourceFolder.toString(), wildcard));
      return Arrays.stream(list).map(file -> file.getName().getBaseName()).sorted().toList();
    }
  }

  private Path createUnreadableFolder() throws IOException {
    Path unreadable = Files.createDirectory(folder.resolve("no-access"));
    Files.createFile(unreadable.resolve("hidden.csv"));
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));
    assumeTrue(
        unreadable.toFile().list() == null,
        "folder is still readable, the test cannot run as this user (root?)");
    return unreadable;
  }
}
