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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

  /** 2024-04-15 10:20:30 local time: the modification date carried by the archived entries. */
  private static final long ARCHIVE_TIME = toEpochMillis(2024, 4, 15, 10, 20, 30);

  /** 2026-01-05 08:00:00 local time: the modification date of the files already in the target. */
  private static final long EXISTING_TIME = toEpochMillis(2026, 1, 5, 8, 0, 0);

  /** A zip stores DOS timestamps, which have a two second resolution. */
  private static final long TIME_TOLERANCE_MS = 2000L;

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
   * Issue #2235: the zip VFS filesystem must be released once after all entries are processed, not
   * once per entry. Closing it per entry re-parses the zip central directory for every file and is
   * catastrophic on archives with hundreds of thousands of small files.
   */
  @Test
  void unzipReleasesZipFileSystemOncePerArchive() throws Exception {
    final int entryCount = 50;

    File workDir = Files.createTempDirectory("unzip2235").toFile();
    workDir.deleteOnExit();
    File zipFile = new File(workDir, "many-entries.zip");
    File targetDir = new File(workDir, "extract");
    createZipWithManySmallEntries(zipFile, entryCount);

    AtomicInteger releaseCount = new AtomicInteger();
    ActionUnZip action =
        new ActionUnZip() {
          @Override
          void releaseZipFileSystem(FileObject zipFileObject) {
            releaseCount.incrementAndGet();
            super.releaseZipFileSystem(zipFileObject);
          }
        };

    Result result = runUnzipSingleFile(action, zipFile, targetDir, FileExistsEnum.OVERWRITE);

    assertEquals(0, result.getNrErrors(), "The action should not report errors");
    assertTrue(result.getResult(), "The action should succeed");
    assertEquals(
        entryCount,
        countFiles(targetDir),
        "All " + entryCount + " entries should have been extracted");
    assertEquals(
        1,
        releaseCount.get(),
        "Zip filesystem must be released exactly once per archive (issue #2235); "
            + "releasing per entry re-parses the central directory for every file");
  }

  /**
   * Issue #4143: the scenario from the report. An archive of reference files dated April 2024 is
   * unzipped over a folder that already holds freshly generated files of the same name, with "If
   * file exists" set to {@code SKIP} and "Set modification date to original" checked. Only the
   * genuinely missing entry may be extracted, and only that entry may receive the archived
   * modification date -- the skipped files keep their own, recent date.
   *
   * <p>Before the fix the archived date was applied to every entry in the archive, because it was
   * stamped from a {@code finally} block that ran whether or not the entry had been extracted.
   */
  @Test
  void skippedFilesKeepTheirOwnModificationDate() throws Exception {
    File workDir = Files.createTempDirectory("unzip4143-skip").toFile();
    workDir.deleteOnExit();

    File zipFile = new File(workDir, "ref_files.zip");
    createZipWithEntries(
        zipFile,
        new String[] {"a.csv", "b.csv", "c.csv"},
        new String[] {"archived-a", "archived-b", "archived-c"},
        ARCHIVE_TIME);

    // The target directory already holds freshly generated a.csv and b.csv, plus an unrelated
    // x.csv that is not in the archive at all.
    File targetDir = new File(workDir, "dst");
    File a = writeExistingFile(targetDir, "a.csv");
    File b = writeExistingFile(targetDir, "b.csv");
    File x = writeExistingFile(targetDir, "x.csv");

    ActionUnZip action = new ActionUnZip();
    action.setSetOriginalModificationDate(true);
    Result result = runUnzipSingleFile(action, zipFile, targetDir, FileExistsEnum.SKIP);

    assertEquals(0, result.getNrErrors(), "The action should not report errors");

    // The one missing entry is extracted and does carry the archived date.
    File c = new File(targetDir, "c.csv");
    assertTrue(c.exists(), "c.csv was missing from the target and should have been extracted");
    assertTimeEquals(
        ARCHIVE_TIME,
        c.lastModified(),
        "extracted c.csv should carry the archived modification date");

    // The skipped files keep their content *and* their own modification date.
    assertEquals("current-a", readFile(a), "a.csv already existed and must not be overwritten");
    assertTimeEquals(
        EXISTING_TIME,
        a.lastModified(),
        "skipped a.csv must keep its own modification date (issue #4143)");
    assertTimeEquals(
        EXISTING_TIME,
        b.lastModified(),
        "skipped b.csv must keep its own modification date (issue #4143)");
    assertTimeEquals(
        EXISTING_TIME,
        x.lastModified(),
        "x.csv is not in the archive and must not be touched at all");
  }

  /**
   * Issue #4143, second path into the same bug: an entry that is filtered out by the entry wildcard
   * is not extracted either, so a file of that name already sitting in the target folder must keep
   * its modification date. {@code OVERWRITE} is used here on purpose, so that the wildcard -- not
   * the "if file exists" rule -- is what spares the existing file.
   */
  @Test
  void entriesFilteredOutByTheWildcardKeepTheirModificationDate() throws Exception {
    File workDir = Files.createTempDirectory("unzip4143-wildcard").toFile();
    workDir.deleteOnExit();

    File zipFile = new File(workDir, "geoip.zip");
    createZipWithEntries(
        zipFile,
        new String[] {"country.txt", "city.txt"},
        new String[] {"archived-country", "archived-city"},
        ARCHIVE_TIME);

    File targetDir = new File(workDir, "dst");
    File city = writeExistingFile(targetDir, "city.txt");

    ActionUnZip action = new ActionUnZip();
    action.setSetOriginalModificationDate(true);
    action.setWildcard("country\\.txt"); // city.txt is filtered out
    Result result = runUnzipSingleFile(action, zipFile, targetDir, FileExistsEnum.OVERWRITE);

    assertEquals(0, result.getNrErrors(), "The action should not report errors");

    File country = new File(targetDir, "country.txt");
    assertTrue(country.exists(), "country.txt matched the wildcard and should have been extracted");
    assertTimeEquals(
        ARCHIVE_TIME,
        country.lastModified(),
        "extracted country.txt should carry the archived modification date");

    assertEquals(
        "current-city", readFile(city), "city.txt was filtered out and must not be overwritten");
    assertTimeEquals(
        EXISTING_TIME,
        city.lastModified(),
        "city.txt was filtered out by the wildcard and must keep its modification date");
  }

  /**
   * Issue #4143, third path into the same bug: with "If file exists" set to {@code FAIL} the
   * existing file is reported as an error and left alone, so its modification date must survive
   * too.
   */
  @Test
  void filesRefusedByFailModeKeepTheirModificationDate() throws Exception {
    File workDir = Files.createTempDirectory("unzip4143-fail").toFile();
    workDir.deleteOnExit();

    File zipFile = new File(workDir, "ref_files.zip");
    createZipWithEntries(
        zipFile, new String[] {"a.csv"}, new String[] {"archived-a"}, ARCHIVE_TIME);

    File targetDir = new File(workDir, "dst");
    File a = writeExistingFile(targetDir, "a.csv");

    ActionUnZip action = new ActionUnZip();
    action.setSetOriginalModificationDate(true);
    Result result = runUnzipSingleFile(action, zipFile, targetDir, FileExistsEnum.FAIL);

    assertTrue(result.getNrErrors() > 0, "An existing file should be reported as an error in FAIL");
    assertEquals("current-a", readFile(a), "a.csv must not be overwritten in FAIL mode");
    assertTimeEquals(
        EXISTING_TIME,
        a.lastModified(),
        "a.csv was refused by FAIL mode and must keep its modification date");
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

  /** Configures and runs the unzip action on a single archive file. */
  private static Result runUnzipSingleFile(
      File zipFile, File targetDir, FileExistsEnum ifFileExist) {
    return runUnzipSingleFile(new ActionUnZip(), zipFile, targetDir, ifFileExist);
  }

  private static Result runUnzipSingleFile(
      ActionUnZip action, File zipFile, File targetDir, FileExistsEnum ifFileExist) {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.setStopped(false);

    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(workflow.getWorkflowMeta());

    action.setZipFilename(zipFile.getPath());
    action.setSourceDirectory(targetDir.getPath());
    action.setCreateFolder(true);
    action.setIfFileExist(ifFileExist);

    return action.execute(new Result(), 0);
  }

  /** Creates a zip file containing the given text entries. */
  private static void createZipWithEntries(File zipFile, String[] entryNames, String[] contents)
      throws java.io.IOException {
    createZipWithEntries(zipFile, entryNames, contents, -1L);
  }

  /**
   * Creates a zip file containing the given text entries, all stamped with the given modification
   * time (epoch millis, or -1 to leave the default "now").
   */
  private static void createZipWithEntries(
      File zipFile, String[] entryNames, String[] contents, long modificationTime)
      throws java.io.IOException {
    try (OutputStream fos = new FileOutputStream(zipFile);
        ZipOutputStream zos = new ZipOutputStream(fos)) {
      for (int i = 0; i < entryNames.length; i++) {
        ZipEntry entry = new ZipEntry(entryNames[i]);
        if (modificationTime >= 0) {
          entry.setTime(modificationTime);
        }
        zos.putNextEntry(entry);
        zos.write(contents[i].getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
      }
    }
  }

  /**
   * Creates a file in the target folder with known content and a known, recent modification date,
   * standing in for a file that a previous workflow step just generated.
   */
  private static File writeExistingFile(File dir, String name) throws java.io.IOException {
    Files.createDirectories(dir.toPath());
    File file = new File(dir, name);
    Files.writeString(
        file.toPath(),
        "current-" + name.substring(0, name.lastIndexOf('.')),
        StandardCharsets.UTF_8);
    assertTrue(
        file.setLastModified(EXISTING_TIME),
        "Test setup: could not set the modification date of " + name);
    return file;
  }

  private static String readFile(File file) throws java.io.IOException {
    return Files.readString(file.toPath(), StandardCharsets.UTF_8);
  }

  private static long toEpochMillis(
      int year, int month, int day, int hour, int minute, int second) {
    return LocalDateTime.of(year, month, day, hour, minute, second)
        .atZone(ZoneId.systemDefault())
        .toInstant()
        .toEpochMilli();
  }

  /** Compares two modification dates within the two second resolution of a zip DOS timestamp. */
  private static void assertTimeEquals(long expected, long actual, String message) {
    assertTrue(
        Math.abs(expected - actual) <= TIME_TOLERANCE_MS,
        message
            + " -- expected "
            + Instant.ofEpochMilli(expected)
            + " but was "
            + Instant.ofEpochMilli(actual));
  }

  /**
   * Creates a zip with many tiny entries (same shape as SEC EDGAR bulk zips: lots of small files).
   * Compression level 0 keeps archive creation cheap in unit tests.
   */
  private static void createZipWithManySmallEntries(File zipFile, int entryCount)
      throws java.io.IOException {
    byte[] payload = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    try (OutputStream fos = new FileOutputStream(zipFile);
        ZipOutputStream zos = new ZipOutputStream(fos)) {
      zos.setLevel(0);
      for (int i = 0; i < entryCount; i++) {
        zos.putNextEntry(new ZipEntry(String.format("f%06d.json", i)));
        zos.write(payload);
        zos.closeEntry();
      }
    }
  }

  private static int countFiles(File dir) {
    File[] children = dir.listFiles(File::isFile);
    return children == null ? 0 : children.length;
  }
}
