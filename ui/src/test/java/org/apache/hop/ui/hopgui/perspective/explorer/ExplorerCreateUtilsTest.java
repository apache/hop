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

package org.apache.hop.ui.hopgui.perspective.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.sql.SqlExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.svg.SvgExplorerFileType;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExplorerCreateUtilsTest {

  @Test
  void applyExtensionAppendsWhenMissing() {
    assertEquals("notes.md", ExplorerCreateUtils.applyExtension("notes", ".md"));
    assertEquals("notes.md", ExplorerCreateUtils.applyExtension("notes", "md"));
    assertEquals("notes.md", ExplorerCreateUtils.applyExtension("  notes  ", ".md"));
  }

  @Test
  void applyExtensionDoesNotDuplicate() {
    assertEquals("notes.md", ExplorerCreateUtils.applyExtension("notes.md", ".md"));
    assertEquals("notes.MD", ExplorerCreateUtils.applyExtension("notes.MD", ".md"));
  }

  @Test
  void applyExtensionKeepsForeignExtension() {
    assertEquals("notes.txt.md", ExplorerCreateUtils.applyExtension("notes.txt", ".md"));
  }

  @Test
  void applyExtensionWithoutExtensionReturnsName() {
    assertEquals("notes", ExplorerCreateUtils.applyExtension("notes", null));
    assertEquals("notes", ExplorerCreateUtils.applyExtension("notes", ""));
  }

  @Test
  void baseNameStripsLastExtension() {
    assertEquals("notes", ExplorerCreateUtils.baseName("notes.md"));
    assertEquals("my.notes", ExplorerCreateUtils.baseName("my.notes.md"));
    assertEquals("notes", ExplorerCreateUtils.baseName("notes"));
    assertEquals(".hidden", ExplorerCreateUtils.baseName(".hidden"));
  }

  @Test
  void childPathJoinsWithSingleSeparator() {
    assertEquals("/project/notes.md", ExplorerCreateUtils.childPath("/project", "notes.md"));
    assertEquals("/project/notes.md", ExplorerCreateUtils.childPath("/project/", "notes.md"));
    assertEquals("C:/project/notes.md", ExplorerCreateUtils.childPath("C:\\project\\", "notes.md"));
  }

  @Test
  void creatableFileTypesKeepsOnlyNewCapableAndExcludesPipelineAndWorkflow() {
    List<IHopFileType> all =
        List.of(
            new HopPipelineFileType<PipelineMeta>(),
            new HopWorkflowFileType<WorkflowMeta>(),
            new SqlExplorerFileType(),
            new SvgExplorerFileType());

    List<IHopFileType> creatable = ExplorerCreateUtils.creatableFileTypes(all);

    // SQL is creatable; SVG is not; pipeline and workflow have their own menu entries.
    assertEquals(1, creatable.size());
    assertEquals(new SqlExplorerFileType().getName(), creatable.get(0).getName());
  }

  @Test
  void isSimpleFileNameAcceptsPlainNames() {
    assertTrue(ExplorerCreateUtils.isSimpleFileName("notes"));
    assertTrue(ExplorerCreateUtils.isSimpleFileName("notes.md"));
    assertTrue(ExplorerCreateUtils.isSimpleFileName("my.notes.md"));
    assertTrue(ExplorerCreateUtils.isSimpleFileName(".hidden"));
  }

  @Test
  void isSimpleFileNameRejectsBlankOrPathLikeNames() {
    assertFalse(ExplorerCreateUtils.isSimpleFileName(null));
    assertFalse(ExplorerCreateUtils.isSimpleFileName(""));
    assertFalse(ExplorerCreateUtils.isSimpleFileName("   "));
    assertFalse(ExplorerCreateUtils.isSimpleFileName("sub/notes"));
    assertFalse(ExplorerCreateUtils.isSimpleFileName("..\\notes"));
    assertFalse(ExplorerCreateUtils.isSimpleFileName("../shared/util"));
    assertFalse(ExplorerCreateUtils.isSimpleFileName("."));
    assertFalse(ExplorerCreateUtils.isSimpleFileName(".."));
  }

  @Test
  void createEmptyFileCreatesTheFile(@TempDir Path tempDir) throws Exception {
    String path = new File(tempDir.toFile(), "notes.md").getAbsolutePath();

    assertFalse(ExplorerCreateUtils.fileExists(path));
    ExplorerCreateUtils.createEmptyFile(path);

    assertTrue(ExplorerCreateUtils.fileExists(path));
    assertEquals(0, new File(path).length());
  }

  @Test
  void createEmptyFileRefusesAnExistingFile(@TempDir Path tempDir) throws Exception {
    String path = new File(tempDir.toFile(), "notes.md").getAbsolutePath();
    ExplorerCreateUtils.createEmptyFile(path);

    assertThrows(HopException.class, () -> ExplorerCreateUtils.createEmptyFile(path));
  }

  @Test
  void resolvesInsideFolderAcceptsAPlainChildName(@TempDir Path tempDir) {
    String folder = tempDir.toFile().getAbsolutePath();
    String candidate = ExplorerCreateUtils.childPath(folder, "notes.md");

    assertTrue(ExplorerCreateUtils.resolvesInsideFolder(folder, candidate));
  }

  @Test
  void resolvesInsideFolderRejectsPercentEncodedTraversal_regressionFor8135(@TempDir Path tempDir) {
    // A typed name of "%2e%2e%2fsecret" passes isSimpleFileName (it contains no literal "/" or
    // "\"), but VFS decodes the %XX escapes before collapsing ".." segments, so the composed path
    // actually resolves to a sibling of the folder, not a child of it. This is the reported
    // bypass: a blacklist on the raw typed name cannot catch it, only resolving the path can.
    String folder = tempDir.toFile().getAbsolutePath();
    String candidate = ExplorerCreateUtils.childPath(folder, "%2e%2e%2fsecret");

    assertFalse(ExplorerCreateUtils.resolvesInsideFolder(folder, candidate));
  }

  @Test
  void resolvesInsideFolderRejectsTheParentDirectoryItself(@TempDir Path tempDir) {
    String folder = tempDir.toFile().getAbsolutePath();
    String candidate = ExplorerCreateUtils.childPath(folder, "..");

    assertFalse(ExplorerCreateUtils.resolvesInsideFolder(folder, candidate));
  }

  @Test
  void isSimpleFileNameRejectsATrailingDotButAcceptsHiddenFiles() {
    assertFalse(ExplorerCreateUtils.isSimpleFileName("notes."));
    assertFalse(ExplorerCreateUtils.isSimpleFileName("notes.md."));
    assertTrue(ExplorerCreateUtils.isSimpleFileName(".hidden"));
  }
}
