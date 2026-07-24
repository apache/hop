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
package org.apache.hop.workflow.actions.unzip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Result;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class WorkflowActionUnZipTest extends WorkflowActionLoadSaveTestSupport<ActionUnZip> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  /**
   * Base-name source wildcard, aligned with the rest of Hop: all folder+wildcard file readers (Text
   * File Input, Get File Names, ... via {@link org.apache.hop.core.fileinput.FileInputList}) match
   * the wildcard against {@code getName().getBaseName()}.
   */
  private static final String SOURCE_WILDCARD_BASENAME = "geoip_.*\\.zip";

  /**
   * Path-style source wildcard (contains a directory separator), exactly as written in the last
   * comment of issue #5943. It only matched before 2.18 (when the source wildcard was tested
   * against the full path); it must keep working for backward compatibility.
   */
  private static final String SOURCE_WILDCARD_PATH = ".*/geoip_.*\\.zip";

  @Override
  protected Class<ActionUnZip> getActionClass() {
    return ActionUnZip.class;
  }

  @Override
  protected List<String> ignoreAttributes() {
    return new ArrayList<>(List.of("ifFileExist"));
  }

  @Test
  void unzipPostProcessingTest() throws Exception {

    ActionUnZip jobEntryUnZip = new ActionUnZip();

    Method unzipPostprocessingMethod =
        jobEntryUnZip
            .getClass()
            .getDeclaredMethod(
                "doUnzipPostProcessing",
                FileObject.class,
                FileObject.class,
                String.class,
                Result.class);
    unzipPostprocessingMethod.setAccessible(true);
    FileObject movetodir = Mockito.mock(FileObject.class);
    Mockito.when(movetodir.toString()).thenReturn("file:///dest");

    // delete
    FileObject sourceForDelete = Mockito.mock(FileObject.class);
    Mockito.doReturn(Mockito.mock(FileName.class)).when(sourceForDelete).getName();
    jobEntryUnZip.afterUnzip = 1;
    unzipPostprocessingMethod.invoke(jobEntryUnZip, sourceForDelete, movetodir, "", new Result());
    Mockito.verify(sourceForDelete, Mockito.times(1)).delete();

    // move (bytes written for moved archive size)
    FileObject sourceForMove = Mockito.mock(FileObject.class);
    FileName moveName = Mockito.mock(FileName.class);
    Mockito.when(moveName.getBaseName()).thenReturn("archive.zip");
    Mockito.when(sourceForMove.getName()).thenReturn(moveName);
    FileType moveType = Mockito.mock(FileType.class);
    Mockito.when(sourceForMove.getType()).thenReturn(moveType);
    Mockito.when(moveType.hasContent()).thenReturn(true);
    FileContent moveContent = Mockito.mock(FileContent.class);
    Mockito.when(sourceForMove.getContent()).thenReturn(moveContent);
    Mockito.when(moveContent.getSize()).thenReturn(77L);

    jobEntryUnZip.afterUnzip = 2;
    Result moveResult = new Result();
    unzipPostprocessingMethod.invoke(jobEntryUnZip, sourceForMove, movetodir, "", moveResult);
    Mockito.verify(sourceForMove, Mockito.times(1)).moveTo(Mockito.any());
    assertEquals(77L, moveResult.getBytesWrittenThisAction());
  }

  /**
   * Issue #5943: unzip a <b>folder</b> of archives selected with a base-name source wildcard. This
   * is the convention shared with every other folder+wildcard reader in Hop (Text File Input, Get
   * File Names, ... via {@link org.apache.hop.core.fileinput.FileInputList}), which matches the
   * wildcard against the file's base name. Both {@code geoip_*.zip} archives should be extracted.
   */
  @Test
  void unzipFolderWithSourceWildcardMatchesBaseName() throws Exception {
    File sourceDir = createGeoipArchiveFolder();
    File targetDir = new File(sourceDir, "extract");

    Result result = runUnzipFolder(sourceDir, targetDir, SOURCE_WILDCARD_BASENAME, null);

    assertEquals(0, result.getNrErrors(), "The action should not report errors");
    assertEquals(
        2,
        result.getNrLinesWritten(),
        "Both geoip_*.zip archives should have been matched and unzipped");
    assertTrue(
        new File(targetDir, "country.txt").exists(),
        "country.txt from geoip_country.zip should have been extracted");
    assertTrue(
        new File(targetDir, "city.txt").exists(),
        "city.txt from geoip_city.zip should have been extracted");
  }

  /**
   * Issue #5943 regression: the same scenario using the path-style source wildcard from the last
   * comment of the issue ({@link #SOURCE_WILDCARD_PATH}, which contains a directory separator).
   * This pattern only matched before 2.18; the action now also matches the full path/URI so it
   * keeps working. Without the fix this extracts nothing ("Nr unzipped files : 0") without an
   * error.
   */
  @Test
  void unzipFolderWithPathStyleSourceWildcard() throws Exception {
    File sourceDir = createGeoipArchiveFolder();
    File targetDir = new File(sourceDir, "extract");

    Result result = runUnzipFolder(sourceDir, targetDir, SOURCE_WILDCARD_PATH, null);

    assertEquals(0, result.getNrErrors(), "The action should not report errors");
    assertEquals(
        2,
        result.getNrLinesWritten(),
        "Both geoip_*.zip archives should have been matched by the path-style wildcard");
    assertTrue(
        new File(targetDir, "country.txt").exists(),
        "country.txt from geoip_country.zip should have been extracted");
    assertTrue(
        new File(targetDir, "city.txt").exists(),
        "city.txt from geoip_city.zip should have been extracted");
  }

  /**
   * The wildcard selecting entries <i>inside</i> a zip ({@code wildcard}) is now matched against
   * both the entry's base name and its full URI, consistent with the source wildcard. This verifies
   * a plain base-name entry pattern extracts only the matching entry.
   */
  @Test
  void unzipEntryWildcardMatchesBaseName() throws Exception {
    File sourceDir = Files.createTempDirectory("unzip5943-entry").toFile();
    sourceDir.deleteOnExit();
    createZipWithEntries(
        new File(sourceDir, "geoip.zip"),
        new String[] {"country.txt", "city.txt"},
        new String[] {"country-data", "city-data"});
    File targetDir = new File(sourceDir, "extract");

    // Select the single zip with a base-name source wildcard, and only the "country.txt" entry
    // inside it with a base-name entry wildcard.
    Result result = runUnzipFolder(sourceDir, targetDir, "geoip\\.zip", "country\\.txt");

    assertEquals(0, result.getNrErrors(), "The action should not report errors");
    assertTrue(
        new File(targetDir, "country.txt").exists(),
        "country.txt should have been extracted (matched the entry wildcard)");
    assertFalse(
        new File(targetDir, "city.txt").exists(),
        "city.txt should have been filtered out by the entry wildcard");
  }

  /**
   * Builds a temp folder with two {@code geoip_*.zip} archives, each holding one distinct entry.
   */
  private static File createGeoipArchiveFolder() throws java.io.IOException {
    File sourceDir = Files.createTempDirectory("unzip5943-src").toFile();
    sourceDir.deleteOnExit();
    createZipWithEntries(
        new File(sourceDir, "geoip_country.zip"),
        new String[] {"country.txt"},
        new String[] {"country-data"});
    createZipWithEntries(
        new File(sourceDir, "geoip_city.zip"),
        new String[] {"city.txt"},
        new String[] {"city-data"});
    return sourceDir;
  }

  /** Configures and runs the unzip action over a folder of archives, returning its result. */
  private static Result runUnzipFolder(
      File sourceDir, File targetDir, String sourceWildcard, String entryWildcard) {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.setStopped(false);

    ActionUnZip action = new ActionUnZip();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(workflow.getWorkflowMeta());

    action.setZipFilename(sourceDir.getPath()); // Zip file name = a folder
    action.setWildcardSource(sourceWildcard); // which archives in the folder
    action.setWildcard(entryWildcard); // which entries inside each archive
    action.setSourceDirectory(targetDir.getPath()); // Target directory (on screen)
    action.setCreateFolder(true);
    action.setIfFileExist(FileExistsEnum.SKIP);

    return action.execute(new Result(), 0);
  }

  /** Creates a zip file containing the given text entries. */
  private static void createZipWithEntries(File zipFile, String[] entryNames, String[] contents)
      throws java.io.IOException {
    try (OutputStream fos = new FileOutputStream(zipFile);
        ZipOutputStream zos = new ZipOutputStream(fos)) {
      for (int i = 0; i < entryNames.length; i++) {
        zos.putNextEntry(new ZipEntry(entryNames[i]));
        zos.write(contents[i].getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
      }
    }
  }
}
