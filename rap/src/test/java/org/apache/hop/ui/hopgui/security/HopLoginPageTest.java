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

package org.apache.hop.ui.hopgui.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HopLoginPageTest {

  @Test
  void renderIncludesBrandingAndWelcome() {
    String html = HopLoginPage.render("", "Welcome to our team portal.", null, "/ui", "alice");
    assertTrue(html.contains("Apache Hop Web"));
    assertTrue(html.contains("Welcome to our team portal."));
    assertTrue(html.contains("name=\"username\""));
    assertTrue(html.contains("value=\"alice\""));
    assertTrue(html.contains("/login/logo.svg"));
    assertTrue(html.contains("/login/login.css"));
  }

  @Test
  void escapeAndOpenRedirectBlocked() {
    assertTrue(HopLoginPage.escapeHtml("<x>").contains("&lt;"));
    assertTrue(HopLoginPage.sanitizeRedirect("https://evil.example/", "").endsWith("/ui"));
    assertTrue(HopLoginPage.sanitizeRedirect("/ui-dark", "").equals("/ui-dark"));
  }

  @Test
  void loginAssetPaths() {
    assertTrue(HopLoginPage.isLoginAssetPath("/login"));
    assertTrue(HopLoginPage.isLoginAssetPath("/login/login.css"));
    assertFalse(HopLoginPage.isLoginAssetPath("/ui"));
  }
}
