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

package org.apache.hop.ui.hopgui.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.eclipse.swt.SWT;
import org.junit.jupiter.api.Test;

class ReferencedFileOpenerTest {

  @Test
  void unchangedDialogOpensWithoutApplying() {
    assertEquals(ReferencedFileOpener.Decision.OPEN, ReferencedFileOpener.decide(false, SWT.YES));
    assertEquals(
        ReferencedFileOpener.Decision.OPEN, ReferencedFileOpener.decide(false, SWT.CANCEL));
  }

  @Test
  void modifiedDialogFollowsTheSaveAnswer() {
    assertEquals(
        ReferencedFileOpener.Decision.APPLY_AND_OPEN, ReferencedFileOpener.decide(true, SWT.YES));
    assertEquals(ReferencedFileOpener.Decision.OPEN, ReferencedFileOpener.decide(true, SWT.NO));
    assertEquals(
        ReferencedFileOpener.Decision.CANCEL, ReferencedFileOpener.decide(true, SWT.CANCEL));
    assertEquals(ReferencedFileOpener.Decision.CANCEL, ReferencedFileOpener.decide(true, SWT.NONE));
  }

  @Test
  void filenameFieldOrChangedFlagMarksTheDialogModified() {
    assertFalse(ReferencedFileOpener.isDialogModified(false, "pipe.hpl", "pipe.hpl"));
    assertFalse(ReferencedFileOpener.isDialogModified(false, null, null));
    assertFalse(ReferencedFileOpener.isDialogModified(false, "", null));
    assertTrue(ReferencedFileOpener.isDialogModified(false, "other.hpl", "pipe.hpl"));
    assertTrue(ReferencedFileOpener.isDialogModified(true, "pipe.hpl", "pipe.hpl"));
  }

  @Test
  void blankOrUnresolvedEmptyFilenameIsNotOpened() {
    IVariables variables = new Variables();
    variables.setVariable("FOLDER", "/data");
    variables.setVariable("EMPTY", "");

    assertFalse(ReferencedFileOpener.hasOpenableFilename(null, "pipe.hpl"));
    assertFalse(ReferencedFileOpener.hasOpenableFilename(variables, null));
    assertFalse(ReferencedFileOpener.hasOpenableFilename(variables, "  "));
    assertFalse(ReferencedFileOpener.hasOpenableFilename(variables, "${EMPTY}"));
    assertTrue(ReferencedFileOpener.hasOpenableFilename(variables, "${FOLDER}/pipe.hpl"));
  }
}
