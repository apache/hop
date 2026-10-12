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

package org.apache.hop.workflow.actions.evalfilesmetrics;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers {@link ActionEvalFilesMetrics#check}: validates {@code sourceFiles} (not legacy {@code
 * arguments}), checks every row (no {@code i++} skip), and fails when a configured file is missing.
 */
class ActionEvalFilesMetricsCheckTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void nullSourceFilesReportsSourceFilesNotArguments() {
    ActionEvalFilesMetrics action = new ActionEvalFilesMetrics("metrics");
    action.setSourceFiles(null);

    List<String> errors = errorTexts(action);

    assertFalse(errors.isEmpty(), errors::toString);
    assertTrue(
        errors.stream().anyMatch(t -> t.contains("sourceFiles")),
        "Expected failure on sourceFiles, got: " + errors);
    assertTrue(
        errors.stream().noneMatch(t -> t.contains("arguments")),
        "Must not validate legacy arguments property, got: " + errors);
  }

  @Test
  void missingSecondRowIsReported() throws Exception {
    Path existing = tempDir.resolve("a.txt");
    Files.writeString(existing, "ok", StandardCharsets.UTF_8);
    Path missing = tempDir.resolve("b-missing.txt");

    ActionEvalFilesMetrics action = actionWithFiles(existing, missing);

    List<String> errors = errorTexts(action);

    assertTrue(
        errors.stream().anyMatch(t -> t.contains("sourceFiles[1].sourceFileFolder")),
        "Second row must be checked (i++ bug would skip it): " + errors);
    assertTrue(
        errors.stream().noneMatch(t -> t.contains("sourceFiles[0].sourceFileFolder")),
        "First existing row should not error: " + errors);
    assertTrue(
        errors.stream().noneMatch(t -> t.contains("arguments")),
        "Must not mention arguments: " + errors);
  }

  @Test
  void bothExistingRowsPass() throws Exception {
    Path a = tempDir.resolve("a.txt");
    Path b = tempDir.resolve("b.txt");
    Files.writeString(a, "1", StandardCharsets.UTF_8);
    Files.writeString(b, "2", StandardCharsets.UTF_8);

    ActionEvalFilesMetrics action = actionWithFiles(a, b);

    List<String> errors = errorTexts(action);
    assertTrue(errors.isEmpty(), errors::toString);
  }

  @Test
  void previousResultSourceSkipsStaticFileExistenceCheck() {
    ActionEvalFilesMetrics action = new ActionEvalFilesMetrics("metrics");
    action.setSourceFilesType(ActionEvalFilesMetrics.SourceFilesType.PREVIOUS_RESULT);
    action.setSourceFiles(
        List.of(sourceFile(tempDir.resolve("does-not-exist-at-check-time.txt").toString())));

    List<String> errors = errorTexts(action);
    assertTrue(
        errors.isEmpty(),
        "Previous-result mode has no static paths to verify at check time: " + errors);
  }

  @Test
  void filenamesResultSourceSkipsStaticFileExistenceCheck() {
    ActionEvalFilesMetrics action = new ActionEvalFilesMetrics("metrics");
    action.setSourceFilesType(ActionEvalFilesMetrics.SourceFilesType.FILENAMES_RESULT);
    action.setSourceFiles(
        List.of(sourceFile(tempDir.resolve("does-not-exist-at-check-time.txt").toString())));

    List<String> errors = errorTexts(action);
    assertTrue(
        errors.isEmpty(),
        "Filenames-result mode has no static paths to verify at check time: " + errors);
  }

  @Test
  void blankSourceFileFolderIsReportedEvenWithWildcard() {
    ActionEvalFilesMetrics action = new ActionEvalFilesMetrics("metrics");
    action.setSourceFilesType(ActionEvalFilesMetrics.SourceFilesType.FILES);
    ActionEvalFilesMetrics.SourceFile row = sourceFile("");
    row.setSourceWildcard(".*\\.txt");
    action.setSourceFiles(List.of(row));

    List<String> errors = errorTexts(action);

    assertTrue(
        errors.stream().anyMatch(t -> t.contains("sourceFiles[0].sourceFileFolder")),
        "Blank folder must fail Check (must not treat \"\" as CWD): " + errors);
  }

  @Test
  void sourceFileFolderResolvingToBlankIsReported() {
    ActionEvalFilesMetrics action = new ActionEvalFilesMetrics("metrics");
    action.setSourceFilesType(ActionEvalFilesMetrics.SourceFilesType.FILES);
    action.setSourceFiles(List.of(sourceFile("${EMPTY_FOLDER}")));

    Variables variables = new Variables();
    variables.setVariable("EMPTY_FOLDER", "");

    List<String> errors = errorTexts(action, variables);

    assertTrue(
        errors.stream().anyMatch(t -> t.contains("sourceFiles[0].sourceFileFolder")),
        "Variable resolving to blank must fail Check: " + errors);
  }

  private static ActionEvalFilesMetrics actionWithFiles(Path first, Path second) {
    ActionEvalFilesMetrics action = new ActionEvalFilesMetrics("metrics");
    action.setSourceFilesType(ActionEvalFilesMetrics.SourceFilesType.FILES);
    action.setSourceFiles(
        List.of(
            sourceFile(first.toAbsolutePath().toString()),
            sourceFile(second.toAbsolutePath().toString())));
    return action;
  }

  private static ActionEvalFilesMetrics.SourceFile sourceFile(String folder) {
    ActionEvalFilesMetrics.SourceFile file = new ActionEvalFilesMetrics.SourceFile();
    file.setSourceFileFolder(folder);
    file.setSourceWildcard("");
    file.setSourceIncludeSubfolders("N");
    return file;
  }

  private static List<String> errorTexts(ActionEvalFilesMetrics action) {
    return errorTexts(action, new Variables());
  }

  private static List<String> errorTexts(ActionEvalFilesMetrics action, Variables variables) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, new WorkflowMeta(), variables, new MemoryMetadataProvider());
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
