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
package org.apache.hop.ui.core.dialog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.eclipse.swt.SWT;
import org.junit.jupiter.api.Test;

/** The shell style rule for transform, action and metadata dialogs, per environment. */
class DialogStyleTest {

  @Test
  void hopWebHasNoMinimizeOrMaximize() {
    int style = DialogStyle.of(true, true, true);
    assertEquals(SWT.DIALOG_TRIM | SWT.RESIZE, style);
  }

  @Test
  void macOsKeepsDialogsWithTheHopWindowByDefault() {
    int style = DialogStyle.of(false, true, false);
    assertEquals(SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX, style);
    assertFalse(has(style, SWT.MIN), "a minimized child window cannot be brought back on macOS");
    assertFalse(
        has(style, SWT.PRIMARY_MODAL), "a child window that follows the Hop window is modeless");
  }

  @Test
  void macOsCanOpenDialogsAsWindowsOfTheirOwn() {
    int style = DialogStyle.of(false, true, true);
    assertEquals(SWT.PRIMARY_MODAL | SWT.CLOSE | SWT.TITLE | SWT.RESIZE, style);
    assertFalse(has(style, SWT.MIN), "a minimized child window cannot be brought back on macOS");
  }

  @Test
  void otherDesktopsKeepTheFullWindowControls() {
    assertEquals(
        SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN, DialogStyle.of(false, false, true));
    assertEquals(
        SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN,
        DialogStyle.of(false, false, false),
        "the option only means something on macOS");
  }

  private static boolean has(int style, int bit) {
    return (style & bit) != 0;
  }
}
