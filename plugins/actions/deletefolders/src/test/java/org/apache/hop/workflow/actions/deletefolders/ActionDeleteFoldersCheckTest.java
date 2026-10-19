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

package org.apache.hop.workflow.actions.deletefolders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ActionDeleteFoldersCheckTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void emptyFolderListWarns() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");

    List<ICheckResult> remarks = check(action, new Variables());

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.getFirst().getType());
    assertEquals(
        message("ActionDeleteFolders.CheckResult.NoFolders"), remarks.getFirst().getText());
  }

  @Test
  void previousResultsSkipTheFolderTable() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setArgFromPrevious(true);

    assertTrue(check(action, new Variables()).isEmpty());
  }

  @Test
  void missingFolderIsReported() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    String missing = tempDir.resolve("missing").toString();
    action.setFileItems(List.of(new FileItem(missing)));

    List<ICheckResult> remarks = check(action, new Variables());

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.getFirst().getType());
    assertEquals(
        message("ActionDeleteFolders.CheckResult.FolderDoesNotExist", missing),
        remarks.getFirst().getText());
  }

  @Test
  void existingFolderIsOk() throws Exception {
    Path folder = tempDir.resolve("to-delete");
    Files.createDirectories(folder);
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setFileItems(List.of(new FileItem(folder.toString())));

    List<ICheckResult> remarks = check(action, new Variables());

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.getFirst().getType());
    assertEquals(
        message("ActionDeleteFolders.CheckResult.FolderOk", folder.toString()),
        remarks.getFirst().getText());
  }

  @Test
  void fileIsNotAFolder() throws Exception {
    Path file = tempDir.resolve("a.txt");
    Files.writeString(file, "x", StandardCharsets.UTF_8);
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setFileItems(List.of(new FileItem(file.toString())));

    List<ICheckResult> remarks = check(action, new Variables());

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.getFirst().getType());
    assertEquals(
        message("ActionDeleteFolders.Error.NotFolder", file.toString()),
        remarks.getFirst().getText());
  }

  @Test
  void blankFolderNameIsReported() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setFileItems(List.of(new FileItem("  ")));

    List<ICheckResult> remarks = check(action, new Variables());

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.getFirst().getType());
    assertEquals(
        message("ActionDeleteFolders.CheckResult.FolderNameMissing", "1"),
        remarks.getFirst().getText());
  }

  @Test
  void variablePathResolvesBeforeTheCheck() throws Exception {
    Path folder = tempDir.resolve("from-variable");
    Files.createDirectories(folder);
    Variables variables = new Variables();
    variables.setVariable("FOLDER", folder.toString());
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setFileItems(List.of(new FileItem("${FOLDER}")));

    List<ICheckResult> remarks = check(action, variables);

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.getFirst().getType());
    assertEquals(
        message("ActionDeleteFolders.CheckResult.FolderOk", folder.toString()),
        remarks.getFirst().getText());
  }

  private static String message(String key, String... arguments) {
    return BaseMessages.getString(ActionDeleteFolders.class, key, (Object[]) arguments);
  }

  private static List<ICheckResult> check(ActionDeleteFolders action, Variables variables) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, new WorkflowMeta(), variables, new MemoryMetadataProvider());
    return remarks;
  }
}
