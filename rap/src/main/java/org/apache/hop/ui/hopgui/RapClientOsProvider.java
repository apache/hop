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

import org.apache.hop.core.ClientOsProvider;
import org.apache.hop.core.Const;
import org.eclipse.rap.rwt.RWT;

/**
 * Provides the client's OS (macOS vs other) from the Hop Web session. The value is derived from the
 * browser's User-Agent when the session is created so that {@link Const#isOSX()} reflects the
 * user's machine for shortcuts and labels.
 */
public class RapClientOsProvider implements ClientOsProvider {

  public static final String SESSION_ATTR_CLIENT_MAC = "hop.client.macos";

  @Override
  public boolean isClientMac() {
    try {
      Object attr = RWT.getUISession().getAttribute(SESSION_ATTR_CLIENT_MAC);
      return Boolean.TRUE.equals(attr);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Detect macOS from the HTTP User-Agent and store in the current session. Call once at session
   * start (e.g. in HopWebEntryPoint).
   *
   * @return true if the User-Agent indicates macOS
   */
  public static boolean detectAndStoreClientMac() {
    boolean isMac = isMacFromUserAgent(getUserAgent());
    RWT.getUISession().setAttribute(SESSION_ATTR_CLIENT_MAC, isMac);
    return isMac;
  }

  private static String getUserAgent() {
    try {
      return RWT.getRequest().getHeader("User-Agent");
    } catch (Exception e) {
      return "";
    }
  }

  /**
   * Heuristic: macOS browsers typically send "Macintosh" or "Mac OS" in the User-Agent. Exclude
   * iPhone/iPad so we only treat desktop Mac as macOS for shortcut purposes.
   */
  private static boolean isMacFromUserAgent(String userAgent) {
    if (userAgent == null) {
      return false;
    }
    String ua = userAgent.toLowerCase();
    if (ua.contains("iphone") || ua.contains("ipad")) {
      return false;
    }
    return ua.contains("macintosh") || ua.contains("mac os");
  }
}
