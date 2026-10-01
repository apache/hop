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

package org.apache.hop.imports.kettle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class KettleImportSkipReportTest {

  private static final String ORIGINAL_WORKFLOW = "original-workflow";
  private static final String ORIGINAL_NOTES = "original-notes";
  private static final String SOURCE_NOTES = "source-notes";

  static {
    System.setProperty(
        "HOP_CONFIG_FOLDER",
        System.getProperty("java.io.tmpdir") + "/hop-issue-4500-" + UUID.randomUUID());
  }

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @AfterEach
  void tearDown() {
    HopVfs.reset();
  }

  /**
   * Re-importing with "Skip existing target files" counted every source file as imported. The
   * summary has to say how many were written and how many were left unchanged (#4500).
   */
  @Test
  void summaryCountsFilesLeftInPlaceWhenSkippingExistingTargets() throws Exception {
    String id = UUID.randomUUID().toString();
    String source = "ram:///" + id + "/src";
    String target = "ram:///" + id + "/tgt";
    FileObject sourceFolder = HopVfs.getFileObject(source);
    sourceFolder.createFolder();
    writeText(source + "/keep.kjb", "<job></job>");
    writeText(source + "/skip.kjb", "<job></job>");
    writeText(source + "/keep.ktr", "<transformation></transformation>");
    writeText(source + "/skip.txt", SOURCE_NOTES);
    sourceFolder.refresh();

    FileObject targetFolder = HopVfs.getFileObject(target);
    targetFolder.createFolder();
    writeText(target + "/skip.hwf", ORIGINAL_WORKFLOW);
    writeText(target + "/skip.txt", ORIGINAL_NOTES);
    targetFolder.refresh();

    KettleImport kettleImport = importFrom(source, target, true);

    assertEquals(1, kettleImport.getKjbCounter());
    assertEquals(1, kettleImport.getKjbSkippedCounter());
    assertEquals(1, kettleImport.getKtrCounter());
    assertEquals(0, kettleImport.getKtrSkippedCounter());
    assertEquals(0, kettleImport.getOtherCounter());
    assertEquals(1, kettleImport.getOtherSkippedCounter());

    String eol = System.getProperty("line.separator");
    assertEquals(
        "Imported :"
            + eol
            + "1 jobs, 1 skipped"
            + eol
            + "1 transformations"
            + eol
            + "0 other files, 1 skipped"
            + eol,
        kettleImport.getImportReport());

    assertEquals(ORIGINAL_WORKFLOW, readText(target + "/skip.hwf"));
    assertEquals(ORIGINAL_NOTES, readText(target + "/skip.txt"));
    assertTrue(HopVfs.getFileObject(target + "/keep.hwf").exists(), kettleImport.getImportReport());
    assertTrue(HopVfs.getFileObject(target + "/keep.hpl").exists(), kettleImport.getImportReport());
    assertFalse(readText(target + "/keep.hwf").contains(ORIGINAL_WORKFLOW));
  }

  @Test
  void summaryStaysUnchangedWhenExistingTargetsAreOverwritten() throws Exception {
    String id = UUID.randomUUID().toString();
    String source = "ram:///" + id + "/src";
    String target = "ram:///" + id + "/tgt";
    FileObject sourceFolder = HopVfs.getFileObject(source);
    sourceFolder.createFolder();
    writeText(source + "/keep.kjb", "<job></job>");
    writeText(source + "/skip.kjb", "<job></job>");
    writeText(source + "/keep.ktr", "<transformation></transformation>");
    writeText(source + "/skip.txt", SOURCE_NOTES);
    sourceFolder.refresh();

    FileObject targetFolder = HopVfs.getFileObject(target);
    targetFolder.createFolder();
    writeText(target + "/keep.hwf", ORIGINAL_WORKFLOW);
    writeText(target + "/skip.hwf", ORIGINAL_WORKFLOW);
    writeText(target + "/keep.hpl", "original-pipeline");
    writeText(target + "/skip.txt", ORIGINAL_NOTES);
    targetFolder.refresh();

    KettleImport kettleImport = importFrom(source, target, false);

    assertEquals(2, kettleImport.getKjbCounter());
    assertEquals(1, kettleImport.getKtrCounter());
    assertEquals(1, kettleImport.getOtherCounter());
    assertEquals(0, kettleImport.getKjbSkippedCounter());
    assertEquals(0, kettleImport.getKtrSkippedCounter());
    assertEquals(0, kettleImport.getOtherSkippedCounter());

    String eol = System.getProperty("line.separator");
    assertEquals(
        "Imported :" + eol + "2 jobs" + eol + "1 transformations" + eol + "1 other files" + eol,
        kettleImport.getImportReport());
    assertFalse(kettleImport.getImportReport().contains("skipped"));
    assertEquals(SOURCE_NOTES, readText(target + "/skip.txt"));
    assertNotEquals(ORIGINAL_WORKFLOW, readText(target + "/skip.hwf"));
  }

  @Test
  void summaryOmitsKindsThatWereNeitherImportedNorSkipped() {
    KettleImport kettleImport = new KettleImport();
    kettleImport.setKjbCounter(2);
    String eol = System.getProperty("line.separator");
    assertEquals("Imported :" + eol + "2 jobs" + eol, kettleImport.getImportReport());
  }

  private static KettleImport importFrom(String source, String target, boolean skipExisting)
      throws Exception {
    KettleImport kettleImport = new KettleImport();
    kettleImport.setValidateInputFolder(source);
    kettleImport.setValidateOutputFolder(target);
    kettleImport.setSkippingExistingTargetFiles(skipExisting);
    kettleImport.findFilesToImport();
    kettleImport.importFiles();
    return kettleImport;
  }

  private static void writeText(String uri, String text) throws Exception {
    try (OutputStream out = HopVfs.getOutputStream(uri, false)) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static String readText(String uri) throws Exception {
    try (InputStream in = HopVfs.getInputStream(uri)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
