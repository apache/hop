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

package org.apache.hop.testing.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Preference helpers for the input/golden data set replacement warnings (issue #2664). Dialog
 * display itself requires SWT and is not covered here.
 */
class TestingGuiPluginReplacementWarningTest {

  @Test
  void warningShownByDefaultWhenParameterMissing() {
    assertTrue(TestingGuiPlugin.shouldShowReplacementWarning(null));
    assertTrue(TestingGuiPlugin.shouldShowReplacementWarning(""));
    assertTrue(TestingGuiPlugin.shouldShowReplacementWarning("Y"));
    assertTrue(TestingGuiPlugin.shouldShowReplacementWarning("y"));
  }

  @Test
  void warningSuppressedWhenUserChoseNotToShowAgain() {
    assertFalse(TestingGuiPlugin.shouldShowReplacementWarning("N"));
    assertFalse(TestingGuiPlugin.shouldShowReplacementWarning("n"));
  }

  @Test
  void toggleStatePersistsAsCustomParameter() {
    assertEquals("N", TestingGuiPlugin.replacementWarningStoredValue(true));
    assertEquals("Y", TestingGuiPlugin.replacementWarningStoredValue(false));
  }
}
