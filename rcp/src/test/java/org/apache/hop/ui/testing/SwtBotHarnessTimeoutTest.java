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

package org.apache.hop.ui.testing;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The harness's own escape hatch: a UI test that never finishes has to fail on its own, not sit
 * there until CI kills the job hours later with nothing to show for it. Both shapes the harness
 * offers are covered - a scene whose worker walks away, and a dialog running its own event loop
 * that nobody closes - because they hang in different places and only one of them can be unblocked
 * from the pump loop.
 */
@Tag("uitest")
class SwtBotHarnessTimeoutTest extends SwtBotTestBase {

  private static final long TEST_TIMEOUT_MILLIS = 1500L;

  private String previousTimeout;

  @BeforeEach
  void shortenTheDeadline() {
    previousTimeout =
        System.setProperty("swtbot.test.timeoutMillis", Long.toString(TEST_TIMEOUT_MILLIS));
  }

  @AfterEach
  void restoreTheDeadline() {
    if (previousTimeout == null) {
      System.clearProperty("swtbot.test.timeoutMillis");
    } else {
      System.setProperty("swtbot.test.timeoutMillis", previousTimeout);
    }
  }

  @Test
  void sceneWhoseWorkerNeverReturnsFailsInsteadOfHanging() {
    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> withScene(shell -> shell.setText("never finishes"), bot -> sleepForever()));

    assertTrue(
        failure.getMessage().contains("did not finish within"),
        "the harness should report the timeout: " + failure.getMessage());
    assertTrue(
        failure.getMessage().contains("worker"),
        "the failure should say what the threads were doing: " + failure.getMessage());
  }

  @Test
  void dialogNobodyClosesFailsInsteadOfHanging() {
    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () -> withDialog(this::openDialogThatOnlyClosingCanEnd, bot -> sleepForever()));

    assertTrue(
        failure.getMessage().contains("did not finish within"),
        "the harness should report the timeout: " + failure.getMessage());
  }

  /** A dialog of the shape the harness drives: its own event loop, running until it is disposed. */
  private void openDialogThatOnlyClosingCanEnd(Shell parent) {
    Shell dialog = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL);
    dialog.setText("never closed");
    dialog.setSize(240, 120);
    dialog.open();
    Display dialogDisplay = dialog.getDisplay();
    while (!dialog.isDisposed()) {
      if (!dialogDisplay.readAndDispatch()) {
        dialogDisplay.sleep();
      }
    }
  }

  /** Interactions that never hand the UI thread anything back - what a wedged test looks like. */
  private static void sleepForever() {
    try {
      Thread.sleep(TEST_TIMEOUT_MILLIS * 100);
    } catch (InterruptedException e) {
      // the harness interrupts the worker when it gives up
      Thread.currentThread().interrupt();
    }
  }
}
