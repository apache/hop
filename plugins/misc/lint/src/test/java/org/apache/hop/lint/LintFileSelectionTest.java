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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.ui.hopgui.HopGui;
import org.junit.jupiter.api.Test;

public class LintFileSelectionTest {

  /**
   * Outside the Hop GUI there is no active file. Resolution must return null rather than let the
   * HopGui class-initialisation failure escape into the extension point that called it.
   */
  @Test
  public void resolveLintFilePathWithoutHopGuiReturnsNull() {
    assertNull(LintFileSelection.resolveLintFilePath());
  }

  /**
   * The resolution must answer without bringing a GUI into existence.
   *
   * <p>Asserting on the return value alone is not enough: on a developer machine with a display,
   * building a HopGui succeeds and the test passes while the code is still wrong. On a headless
   * build agent the same call throws {@link org.eclipse.swt.SWTError} from gtk_init_check, which is
   * an Error rather than an Exception and escaped the catch. Checking that no instance was created
   * is the assertion that fails in both places when the guard is missing.
   */
  @Test
  public void resolvingTheSelectedFileNeverConstructsAGui() {
    assertNull(HopGui.peekInstance(), "precondition: no GUI in this test JVM");

    LintFileSelection.resolveLintFilePath();
    LintEditorGraphHelper.getActiveEditorFilename();
    LintEditorGraphHelper.findOpenGraphForFilename("/project/pipelines/load.hpl");
    LintEditorGraphHelper.findOpenHandlerForPath("/project/pipelines/load.hpl");

    assertNull(HopGui.peekInstance(), "resolution must not build a HopGui to answer");
  }
}
