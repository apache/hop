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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit test for {@link HopWeb} */
class HopWebSplashTest {

  @Test
  void lightSplashUsesLightThemeClass() {
    String html = HopWeb.splashBodyHtml("light");

    assertTrue(html.contains("id=\"hop-web-splash\""));
    assertTrue(html.contains("class=\"hop-web-splash--light\""));
    assertFalse(html.contains("{{theme}}"));
  }

  @Test
  void darkSplashUsesDarkThemeClass() {
    String html = HopWeb.splashBodyHtml("dark");

    assertTrue(html.contains("class=\"hop-web-splash--dark\""));
    assertFalse(html.contains("class=\"hop-web-splash--light\""));
  }

  @Test
  void overlaySitsAboveRapShellsThenDropsZIndexOnHide() {
    String html = HopWeb.splashBodyHtml("light");

    assertTrue(html.contains("z-index: 100000010"));
    assertTrue(html.contains("el.style.zIndex = \"0\""));
    assertTrue(html.contains("MutationObserver"));
    assertTrue(html.contains("toolbar-10010-new"));
    assertTrue(html.contains("requestAnimationFrame"));
  }
}
