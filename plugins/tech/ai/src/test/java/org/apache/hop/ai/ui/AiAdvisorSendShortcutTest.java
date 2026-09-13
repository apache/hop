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

package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.swt.SWT;
import org.junit.jupiter.api.Test;

class AiAdvisorSendShortcutTest {

  @Test
  void ctrlAndCmdEnterSend() {
    assertTrue(AiAdvisorSessionPane.isSendShortcut(SWT.KeyDown, 0, SWT.MOD1, SWT.CR, SWT.CR));
    assertTrue(
        AiAdvisorSessionPane.isSendShortcut(SWT.KeyDown, 0, SWT.COMMAND, SWT.KEYPAD_CR, (char) 0));
    assertTrue(
        AiAdvisorSessionPane.isSendShortcut(
            SWT.Traverse, SWT.TRAVERSE_RETURN, SWT.CONTROL, SWT.CR, SWT.CR));
  }

  @Test
  void enterAloneDoesNotSend() {
    assertFalse(AiAdvisorSessionPane.isSendShortcut(SWT.KeyDown, 0, 0, SWT.CR, SWT.CR));
    assertFalse(
        AiAdvisorSessionPane.isSendShortcut(SWT.Traverse, SWT.TRAVERSE_RETURN, 0, SWT.CR, SWT.CR));
  }

  @Test
  void ctrlEnterNewlineIsEaten() {
    assertTrue(AiAdvisorSessionPane.shouldEatSendNewline(SWT.MOD1, "\n"));
    assertFalse(AiAdvisorSessionPane.shouldEatSendNewline(0, "\n"));
    assertFalse(AiAdvisorSessionPane.shouldEatSendNewline(SWT.MOD1, "hello"));
  }

  @Test
  void secondEventOfSameCtrlEnterDoesNotSend() {
    AiAdvisorSessionPane.SendShortcutGuard guard = new AiAdvisorSessionPane.SendShortcutGuard();
    assertTrue(guard.claim());
    assertTrue(guard.isArmed());
    assertFalse(guard.claim(), "GTK KeyDown after Traverse must not send again");
    guard.release();
    assertTrue(guard.claim());
  }
}
