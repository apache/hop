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

package org.apache.hop.workflow.actions.xml.xmlwellformed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * The "include subfolders" option has to control folder traversal as well, otherwise a sub-folder
 * the user has no access to breaks a check that was never meant to look inside it. See
 * https://github.com/apache/hop/issues/7952
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class XmlWellFormedSubfolderTest {

  private static final String WELL_FORMED = "<?xml version=\"1.0\"?><root><a>1</a></root>";
  private static final String MALFORMED = "<?xml version=\"1.0\"?><root><a>1</root>";

  @TempDir Path folder;

  private XmlWellFormed action;
  private Path unreadableFolder;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new XmlWellFormed();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);

    XmlWellFormedField field = new XmlWellFormedField();
    field.setSourceFilefolder(folder.toString());
    field.setWildcard(".*\\.xml");
    action.setSourceFileFolders(List.of(field));
  }

  @AfterEach
  void restorePermissions() throws IOException {
    if (unreadableFolder != null) {
      Files.setPosixFilePermissions(unreadableFolder, PosixFilePermissions.fromString("rwx------"));
    }
  }

  @Test
  void unreadableSubfolderIsNotTraversedWhenSubfoldersAreExcluded() throws Exception {
    Files.writeString(folder.resolve("good.xml"), WELL_FORMED, StandardCharsets.UTF_8);
    unreadableFolder = createUnreadableFolder();
    action.includeSubfolders = false;

    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors(), "an excluded sub-folder must not cause an error");
    assertTrue(result.isResult());
    assertEquals(1, result.getNrLinesWritten(), "only the base folder file is checked");
  }

  @Test
  void subfoldersAreTraversedWhenIncluded() throws Exception {
    Files.writeString(folder.resolve("good.xml"), WELL_FORMED, StandardCharsets.UTF_8);
    Path subFolder = Files.createDirectory(folder.resolve("archive"));
    Files.writeString(subFolder.resolve("bad.xml"), MALFORMED, StandardCharsets.UTF_8);

    action.includeSubfolders = false;
    Result result = action.execute(new Result(), 0);
    assertEquals(0, result.getNrLinesRejected(), "the nested malformed file must be skipped");

    action.includeSubfolders = true;
    result = action.execute(new Result(), 0);
    assertEquals(1, result.getNrLinesRejected(), "the nested malformed file must be reported");
  }

  private Path createUnreadableFolder() throws IOException {
    Path unreadable = Files.createDirectory(folder.resolve("no-access"));
    Files.writeString(unreadable.resolve("hidden.xml"), WELL_FORMED, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));
    assumeTrue(
        unreadable.toFile().list() == null,
        "folder is still readable, the test cannot run as this user (root?)");
    return unreadable;
  }
}
