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

package org.apache.hop.ui.hopgui.delegates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class HopGuiFileBeforeCommitExtensionTest {

  private static final String GIT_DIR = File.separator + "projects" + File.separator + "sales";

  @Test
  void newExtensionIsNotCancelled() {
    HopGuiFileBeforeCommitExtension extension =
        new HopGuiFileBeforeCommitExtension(GIT_DIR, List.of("pipelines/load.hpl"));

    assertFalse(extension.isCancelled());
    assertNull(extension.getCancelReason());
  }

  @Test
  void cancelRecordsTheReason() {
    HopGuiFileBeforeCommitExtension extension =
        new HopGuiFileBeforeCommitExtension(GIT_DIR, List.of("pipelines/load.hpl"));

    extension.cancel("2 hardcoded passwords");

    assertTrue(extension.isCancelled());
    assertEquals("2 hardcoded passwords", extension.getCancelReason());
  }

  @Test
  void theFirstRefusalDecides() {
    // A second listener must not be able to overturn the first, nor replace the reason the user is
    // about to be shown.
    HopGuiFileBeforeCommitExtension extension =
        new HopGuiFileBeforeCommitExtension(GIT_DIR, List.of("pipelines/load.hpl"));

    extension.cancel("first reason");
    extension.cancel("second reason");

    assertTrue(extension.isCancelled());
    assertEquals("first reason", extension.getCancelReason());
  }

  @Test
  void nullFilenamesBecomeAnEmptyList() {
    HopGuiFileBeforeCommitExtension extension = new HopGuiFileBeforeCommitExtension(GIT_DIR, null);

    assertTrue(extension.getFilenames().isEmpty());
    assertTrue(extension.getAbsoluteFilenames().isEmpty());
  }

  @Test
  void filenamesCannotBeChangedByAListener() {
    List<String> files = new ArrayList<>(List.of("pipelines/load.hpl"));
    HopGuiFileBeforeCommitExtension extension = new HopGuiFileBeforeCommitExtension(GIT_DIR, files);

    assertThrows(
        UnsupportedOperationException.class, () -> extension.getFilenames().add("sneaky.hpl"));
  }

  @Test
  void absoluteFilenamesResolveAgainstTheGitDirectory() {
    HopGuiFileBeforeCommitExtension extension =
        new HopGuiFileBeforeCommitExtension(
            GIT_DIR, List.of("pipelines/load.hpl", "workflows/main.hwf"));

    assertEquals(
        List.of(
            new File(GIT_DIR, "pipelines/load.hpl").getPath(),
            new File(GIT_DIR, "workflows/main.hwf").getPath()),
        extension.getAbsoluteFilenames());
  }
}
