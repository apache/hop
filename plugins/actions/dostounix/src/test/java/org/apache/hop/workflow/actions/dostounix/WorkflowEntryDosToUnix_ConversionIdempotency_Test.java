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

package org.apache.hop.workflow.actions.dostounix;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class WorkflowEntryDosToUnix_ConversionIdempotency_Test {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  private File tmpFile;
  private String tmpFilePath;
  private ActionDosToUnix entry;

  @BeforeEach
  void setUp() throws Exception {
    tmpFile = File.createTempFile("pdi-14161-", null);
    tmpFilePath = tmpFile.toURI().toString();
    entry = new ActionDosToUnix();
  }

  @AfterEach
  void tearDown() {
    if (tmpFile != null) {
      tmpFile.delete();
      tmpFile = null;
    }
    tmpFilePath = null;
    entry = null;
  }

  @Test
  void oneSeparator_nix2dos() throws Exception {
    doTest("\n", false, "\r\n");
  }

  @Test
  void oneSeparator_nix2nix() throws Exception {
    doTest("\n", true, "\n");
  }

  @Test
  void oneSeparator_dos2nix() throws Exception {
    doTest("\r\n", true, "\n");
  }

  @Test
  void oneSeparator_dos2dos() throws Exception {
    doTest("\r\n", false, "\r\n");
  }

  @Test
  void charNewLineChar_nix2dos() throws Exception {
    doTest("a\nb", false, "a\r\nb");
  }

  @Test
  void charNewLineChar_nix2nix() throws Exception {
    doTest("a\nb", true, "a\nb");
  }

  @Test
  void charNewLineChar_dos2nix() throws Exception {
    doTest("a\r\nb", true, "a\nb");
  }

  @Test
  void charNewLineChar_dos2dos() throws Exception {
    doTest("a\r\nb", false, "a\r\nb");
  }

  @Test
  void twoCrOneLf_2nix() throws Exception {
    doTest("\r\r\n", true, "\r\n");
  }

  @Test
  void twoCrOneLf_2dos() throws Exception {
    doTest("\r\r\n", false, "\r\r\n");
  }

  @Test
  void crCharCrLf_2nix() throws Exception {
    doTest("\ra\r\n", true, "\ra\n");
  }

  @Test
  void crCharCrLf_2dos() throws Exception {
    doTest("\ra\r\n", false, "\ra\r\n");
  }

  @Test
  void oneSeparator_nix2dos_hugeInput() throws Exception {
    doTestForSignificantInput("\n", false, "\r\n");
  }

  @Test
  void oneSeparator_nix2nix_hugeInput() throws Exception {
    doTestForSignificantInput("\n", true, "\n");
  }

  @Test
  void oneSeparator_dos2nix_hugeInput() throws Exception {
    doTestForSignificantInput("\r\n", true, "\n");
  }

  @Test
  void oneSeparator_dos2dos_hugeInput() throws Exception {
    doTestForSignificantInput("\r\n", false, "\r\n");
  }

  private void doTestForSignificantInput(
      String contentPattern, boolean toUnix, String expectedPattern) throws Exception {
    int copyTimes = (8 * 1024 / contentPattern.length()) + 1;
    String content = copyUntilReachesEightKbs(contentPattern, copyTimes);
    String expected = copyUntilReachesEightKbs(expectedPattern, copyTimes);

    doTest(content, toUnix, expected);
  }

  private String copyUntilReachesEightKbs(String pattern, int times) {
    StringBuilder sb = new StringBuilder(pattern.length() * times);
    for (int i = 0; i < times; i++) {
      sb.append(pattern);
    }
    return sb.toString();
  }

  private void doTest(String content, boolean toUnix, String expected) throws Exception {
    try (OutputStream os = new FileOutputStream(tmpFile)) {
      IOUtils.write(content.getBytes(), os);
    }

    entry.convert(HopVfs.getFileObject(tmpFilePath), toUnix);

    String converted = HopVfs.getTextFileContent(tmpFilePath, "UTF-8");
    assertEquals(expected, converted);
  }
}
