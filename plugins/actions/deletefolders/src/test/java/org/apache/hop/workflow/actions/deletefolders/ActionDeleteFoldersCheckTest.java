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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.validator.ValidatorMessages;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ActionDeleteFoldersCheckTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
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
  void missingFolderIsAWarning() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setFileItems(List.of(new FileItem(tempDir.resolve("missing").toString())));

    List<ICheckResult> remarks = check(action, new Variables());

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.getFirst().getType());
    assertEquals(
        ValidatorMessages.getString("messages.failed.fileExists", "fileItems[0].fileName"),
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
        passedMessage("fileItems[0].fileName", folder.toString()), remarks.getFirst().getText());
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
        message("ActionDeleteFolders.CheckResult.NotAFolder", file.toString()),
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
        ValidatorMessages.getString("messages.failed.notBlank", "fileItems[0].fileName"),
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
    assertEquals(passedMessage("fileItems[0].fileName", "${FOLDER}"), remarks.getFirst().getText());
  }

  @Test
  void remoteFolderIsNotProbed() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setFileItems(List.of(new FileItem("sftp://example.com/data")));

    assertTrue(check(action, new Variables()).isEmpty());
  }

  @Test
  void unknownSchemeIsNotProbed() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setFileItems(List.of(new FileItem("nosuchscheme://x")));

    assertTrue(check(action, new Variables()).isEmpty());
  }

  @Test
  void previousResultNameIsNotResolved() throws Exception {
    Path kept = tempDir.resolve("kept");
    Files.createDirectories(kept);
    Files.writeString(kept.resolve("marker.txt"), "x", StandardCharsets.UTF_8);
    ActionDeleteFolders action = attachedAction();
    action.setArgFromPrevious(true);
    action.setVariable("FOLDER", kept.toString());

    Result result = action.execute(resultWithFolder("${FOLDER}"), 0);

    assertEquals(0, result.getNrErrors());
    assertTrue(Files.exists(kept.resolve("marker.txt")));
  }

  @Test
  void previousResultDeletesTheLiteralFolder() throws Exception {
    Path folder = tempDir.resolve("from-row");
    Files.createDirectories(folder);
    ActionDeleteFolders action = attachedAction();
    action.setArgFromPrevious(true);

    Result result = action.execute(resultWithFolder(folder.toString()), 0);

    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getNrLinesDeleted());
    assertFalse(Files.exists(folder));
  }

  @Test
  void blankNamesAreIgnoredAtRuntime() throws Exception {
    ActionDeleteFolders action = attachedAction();
    action.setFileItems(List.of(new FileItem("   ")));

    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors());
    assertEquals(0, result.getNrLinesDeleted());
    assertTrue(result.getResult());
  }

  @Test
  void resourceDependenciesUseTheGivenVariables() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    action.setVariable("FOLDER", "from-action");
    action.setFileItems(List.of(new FileItem("${FOLDER}/data")));
    Variables caller = new Variables();
    caller.setVariable("FOLDER", "from-caller");

    List<ResourceReference> references = action.getResourceDependencies(caller, new WorkflowMeta());

    assertEquals(1, references.size());
    ResourceEntry entry = references.getFirst().getEntries().getFirst();
    assertEquals(ResourceEntry.ResourceType.FILE, entry.getResourcetype());
    assertEquals("from-caller/data", entry.getResource());
  }

  private static String passedMessage(String propertyName, String value) {
    String shown = value;
    if (value.length() > 20) {
      shown = value.substring(0, 20) + "...";
    }
    return ValidatorMessages.getString("messages.passed", propertyName, shown);
  }

  private static ActionDeleteFolders attachedAction() {
    ActionDeleteFolders action = new ActionDeleteFolders("delete folders");
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.setStopped(false);
    action.setParentWorkflow(workflow);
    return action;
  }

  private static Result resultWithFolder(String folderName) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("folder"));
    Result result = new Result();
    result.setRows(List.of(new RowMetaAndData(rowMeta, folderName)));
    return result;
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
