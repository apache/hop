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
package org.apache.hop.ui.hopgui.notifications;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for the link policy applied to notifications coming from remote feeds. */
public class NotificationLinksTest {

  @Test
  public void testHttpAndHttpsAreAccepted() {
    assertTrue(NotificationLinks.isSafe("https://hop.apache.org"));
    assertTrue(NotificationLinks.isSafe("http://hop.apache.org/download"));
    assertTrue(NotificationLinks.isSafe("https://github.com/apache/hop/releases/tag/2.19.0"));
    assertTrue(NotificationLinks.isSafe("  https://hop.apache.org  "));
  }

  @Test
  public void testSchemeIsCaseInsensitive() {
    assertTrue(NotificationLinks.isSafe("HTTPS://hop.apache.org"));
    assertTrue(NotificationLinks.isSafe("HtTp://hop.apache.org"));
  }

  @Test
  public void testOtherSchemesAreRejected() {
    // These all reach Program.launch() on the desktop, which asks the operating system to open
    // them. A feed must not be able to get that far.
    assertFalse(NotificationLinks.isSafe("file:///etc/passwd"));
    assertFalse(NotificationLinks.isSafe("javascript:alert(1)"));
    assertFalse(NotificationLinks.isSafe("smb://server/share/payload.exe"));
    assertFalse(NotificationLinks.isSafe("ftp://example.com/payload"));
    assertFalse(NotificationLinks.isSafe("data:text/html;base64,PHNjcmlwdD4="));
    assertFalse(NotificationLinks.isSafe("vbscript:msgbox"));
  }

  @Test
  public void testNonUrlsAreRejected() {
    assertFalse(NotificationLinks.isSafe(null));
    assertFalse(NotificationLinks.isSafe(""));
    assertFalse(NotificationLinks.isSafe("   "));
    assertFalse(NotificationLinks.isSafe("/Applications/Calculator.app"));
    assertFalse(NotificationLinks.isSafe("C:\\Windows\\System32\\calc.exe"));
    assertFalse(NotificationLinks.isSafe("\\\\server\\share\\payload.exe"));
    assertFalse(NotificationLinks.isSafe("hop.apache.org"));
    assertFalse(NotificationLinks.isSafe("not a url at all"));
  }

  @Test
  public void testUrlWithoutHostIsRejected() {
    assertFalse(NotificationLinks.isSafe("http://"));
    assertFalse(NotificationLinks.isSafe("https:///path/only"));
  }
}
