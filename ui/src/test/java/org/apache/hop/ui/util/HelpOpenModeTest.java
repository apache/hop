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

package org.apache.hop.ui.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class HelpOpenModeTest {

  @Test
  void fromConfigValueAcceptsEnumNamesAndLegacyBoolean() {
    assertEquals(HelpOpenMode.BROWSER, HelpOpenMode.fromConfigValue(null));
    assertEquals(HelpOpenMode.BROWSER, HelpOpenMode.fromConfigValue(""));
    assertEquals(HelpOpenMode.BROWSER, HelpOpenMode.fromConfigValue("BROWSER"));
    assertEquals(HelpOpenMode.TAB, HelpOpenMode.fromConfigValue("tab"));
    assertEquals(HelpOpenMode.DIALOG, HelpOpenMode.fromConfigValue("Dialog"));
    assertEquals(HelpOpenMode.TAB, HelpOpenMode.fromConfigValue("true"));
    assertEquals(HelpOpenMode.BROWSER, HelpOpenMode.fromConfigValue("false"));
    assertEquals(HelpOpenMode.BROWSER, HelpOpenMode.fromConfigValue("not-a-mode"));
  }

  @Test
  void fromLabelAcceptsEnumNameAndTranslatedLabel() {
    assertEquals(HelpOpenMode.BROWSER, HelpOpenMode.fromLabel(null));
    assertEquals(HelpOpenMode.DIALOG, HelpOpenMode.fromLabel("DIALOG"));
    assertEquals(HelpOpenMode.TAB, HelpOpenMode.fromLabel(HelpOpenMode.TAB.getLabel()));
    assertEquals(HelpOpenMode.DIALOG, HelpOpenMode.fromLabel(HelpOpenMode.DIALOG.getLabel()));
  }

  @Test
  void toConfigValueIsEnumName() {
    assertEquals("BROWSER", HelpOpenMode.BROWSER.toConfigValue());
    assertEquals("TAB", HelpOpenMode.TAB.toConfigValue());
    assertEquals("DIALOG", HelpOpenMode.DIALOG.toConfigValue());
  }
}
